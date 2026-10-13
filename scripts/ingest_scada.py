"""Ingest AEMO Dispatch SCADA files into the bronze layer.

Lists the AEMO Dispatch_SCADA directory, downloads any file not already
recorded in the watermark, parses the nested AEMO CSV format, and writes the
rows to date-partitioned Parquet.

Each source file produces exactly one Parquet file, which is never rewritten.
Git history is append-only, so a file rewritten on every run leaves a permanent
blob behind each time; writing once per source interval keeps the repository
growing at roughly the rate of the data itself.

Usage:
    python scripts/ingest_scada.py                 # ingest everything pending
    python scripts/ingest_scada.py --limit 12      # ingest at most 12 files
    python scripts/ingest_scada.py --list-only     # show what is pending
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import sys
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Final

import duckdb
import httpx

LOG: Final = logging.getLogger("ingest_scada")

SOURCE_URL: Final = "https://nemweb.com.au/Reports/Current/Dispatch_SCADA/"
REPO_ROOT: Final = Path(__file__).resolve().parent.parent
BRONZE_DIR: Final = REPO_ROOT / "data" / "bronze"
WATERMARK_NAME: Final = "_ingested.txt"

# AEMO publishes SETTLEMENTDATE in NEM time: AEST, UTC+10, no daylight saving.
NEM_TZ: Final = timezone(timedelta(hours=10), name="AEST")

# The table of interest inside the DISPATCHSCADA report.
UNIT_SCADA_TABLE: Final = "DISPATCH_UNIT_SCADA"

FILENAME_RE: Final = re.compile(r"PUBLIC_DISPATCHSCADA_(\d{14})_\d+\.zip", re.IGNORECASE)
SETTLEMENT_FORMATS: Final = ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S")

HTTP_TIMEOUT: Final = httpx.Timeout(30.0, connect=15.0)
USER_AGENT: Final = "aemo-wind/0.1 (+github.com/HowardXUuts/WindPowerProject_AEMO20261013)"


class ParseError(Exception):
    """The AEMO CSV payload could not be interpreted."""


# ---------------------------------------------------------------------------
# AEMO CSV parsing
# ---------------------------------------------------------------------------


def parse_aemo_csv(text: str) -> Iterator[tuple[str, dict[str, str]]]:
    """Yield ``(table, row)`` pairs for every D row in an AEMO nested CSV.

    AEMO reports pack several tables into one file, distinguished by a marker in
    the first column: C rows are header and footer metadata, I rows declare the
    column names for a table, and D rows carry the data. An I row applies to
    every following D row sharing its category, subcategory and version, so
    headers have to be tracked as the file is read.
    """
    headers: dict[tuple[str, str, str], list[str]] = {}

    for row in csv.reader(io.StringIO(text)):
        if not row or not row[0].strip():
            continue

        marker = row[0].strip().upper()
        if marker == "C":  # report metadata and "END OF REPORT"
            continue
        if marker not in {"I", "D"} or len(row) < 4:
            LOG.debug("skipping unrecognised row: %.60s", ",".join(row))
            continue

        key = (row[1].strip(), row[2].strip(), row[3].strip())

        if marker == "I":
            headers[key] = [field.strip() for field in row[4:]]
            continue

        header = headers.get(key)
        if header is None:
            # A D row before its I row means the file is truncated or corrupt.
            raise ParseError(f"D row for {key} has no preceding I row")

        values = row[4:]
        if len(values) < len(header):
            # AEMO sometimes omits trailing empty fields.
            values = values + [""] * (len(header) - len(values))

        yield f"{key[0]}_{key[1]}", dict(zip(header, values[: len(header)], strict=True))


def parse_settlement_date(value: str) -> datetime:
    """Parse an AEMO SETTLEMENTDATE into a timezone-aware NEM-time datetime."""
    cleaned = value.strip().strip('"')
    for fmt in SETTLEMENT_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).replace(tzinfo=NEM_TZ)
        except ValueError:
            continue
    raise ParseError(f"unrecognised SETTLEMENTDATE: {value!r}")


def parse_scada_value(value: str) -> float | None:
    """Parse a SCADAVALUE, treating a blank as missing rather than zero."""
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError as exc:
        raise ParseError(f"unrecognised SCADAVALUE: {value!r}") from exc


def extract_unit_scada(text: str, source_file: str) -> list[tuple]:
    """Return bronze rows for the UNIT_SCADA table in an AEMO CSV payload."""
    ingested_at = datetime.now(UTC)
    rows: list[tuple] = []

    for table, record in parse_aemo_csv(text):
        if table.upper() != UNIT_SCADA_TABLE:
            continue
        try:
            duid = record["DUID"].strip()
            settlement = parse_settlement_date(record["SETTLEMENTDATE"])
        except KeyError as exc:
            raise ParseError(f"UNIT_SCADA row missing column {exc}") from exc
        rows.append(
            (
                settlement,
                duid,
                parse_scada_value(record.get("SCADAVALUE", "")),
                source_file,
                ingested_at,
            )
        )

    if not rows:
        raise ParseError(f"no {UNIT_SCADA_TABLE} rows found in {source_file}")
    return rows


def read_zip_payload(payload: bytes, source_file: str) -> str:
    """Extract the single CSV member of an AEMO zip archive."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = [name for name in archive.namelist() if name.upper().endswith(".CSV")]
        if len(members) != 1:
            raise ParseError(f"expected one CSV in {source_file}, found {members}")
        return archive.read(members[0]).decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------


def watermark_path(bronze_dir: Path = BRONZE_DIR) -> Path:
    """Return the path of the ingestion watermark file."""
    return bronze_dir / WATERMARK_NAME


def load_watermark(path: Path) -> set[str]:
    """Return the set of source file names already ingested."""
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.strip()}


def append_watermark(names: list[str], path: Path) -> None:
    """Record source file names as ingested.

    Appended only after the Parquet write succeeds, so a crashed run retries the
    file on its next pass rather than skipping it forever.
    """
    if not names:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        for name in sorted(names):
            handle.write(f"{name}\n")


# ---------------------------------------------------------------------------
# Remote listing and download
# ---------------------------------------------------------------------------


def interval_from_filename(name: str) -> datetime:
    """Parse the interval timestamp embedded in an AEMO file name (NEM time)."""
    match = FILENAME_RE.search(name)
    if match is None:
        raise ParseError(f"not a DISPATCHSCADA file name: {name!r}")
    return datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=NEM_TZ)


def list_remote_files(client: httpx.Client, url: str = SOURCE_URL) -> list[str]:
    """Return the DISPATCHSCADA zip file names in the AEMO directory listing."""
    response = client.get(url)
    response.raise_for_status()
    found = {match.group(0) for match in FILENAME_RE.finditer(response.text)}
    return sorted(found)


def download(client: httpx.Client, name: str, url: str = SOURCE_URL) -> bytes:
    """Download one source file."""
    response = client.get(url + name)
    response.raise_for_status()
    return response.content


# ---------------------------------------------------------------------------
# Bronze write
# ---------------------------------------------------------------------------

_BRONZE_DDL: Final = """
CREATE OR REPLACE TABLE bronze_rows (
    settlementdate TIMESTAMPTZ,
    duid           VARCHAR,
    scadavalue     DOUBLE,
    source_file    VARCHAR,
    ingested_at    TIMESTAMPTZ
)
"""


def bronze_path(source_file: str, bronze_dir: Path = BRONZE_DIR) -> Path:
    """Return the Parquet path for a source file, partitioned by NEM date."""
    interval = interval_from_filename(source_file)
    partition = f"date={interval.date().isoformat()}"
    return bronze_dir / partition / f"{Path(source_file).stem}.parquet"


def write_bronze(rows: list[tuple], destination: Path) -> int:
    """Write bronze rows to Parquet and return the row count."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    literal = str(destination).replace("'", "''")

    con = duckdb.connect()
    try:
        con.execute(_BRONZE_DDL)
        con.executemany("INSERT INTO bronze_rows VALUES (?, ?, ?, ?, ?)", rows)
        con.execute(
            "COPY (SELECT * FROM bronze_rows ORDER BY settlementdate, duid) "
            f"TO '{literal}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    return len(rows)


def ingest_payload(
    payload: bytes,
    source_file: str,
    bronze_dir: Path = BRONZE_DIR,
) -> tuple[Path, int]:
    """Parse one downloaded archive and write its bronze Parquet file."""
    text = read_zip_payload(payload, source_file)
    rows = extract_unit_scada(text, source_file)
    destination = bronze_path(source_file, bronze_dir)
    return destination, write_bronze(rows, destination)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def pending_files(
    available: list[str],
    ingested: set[str],
    limit: int | None = None,
) -> list[str]:
    """Return the files not yet ingested, oldest interval first."""
    outstanding = sorted(set(available) - ingested, key=interval_from_filename)
    return outstanding[:limit] if limit else outstanding


def run(
    limit: int | None = None,
    list_only: bool = False,
    bronze_dir: Path = BRONZE_DIR,
) -> int:
    """Ingest all pending source files and return the number ingested."""
    watermark = watermark_path(bronze_dir)
    ingested = load_watermark(watermark)
    LOG.info("watermark holds %d previously ingested files", len(ingested))

    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(timeout=HTTP_TIMEOUT, headers=headers, follow_redirects=True) as client:
        available = list_remote_files(client)
        LOG.info("AEMO directory lists %d files", len(available))

        outstanding = pending_files(available, ingested, limit)
        if list_only:
            for name in outstanding:
                print(name)
            return 0
        if not outstanding:
            LOG.info("nothing to ingest")
            return 0

        LOG.info("ingesting %d files", len(outstanding))
        done: list[str] = []
        total_rows = 0
        for name in outstanding:
            try:
                payload = download(client, name)
                destination, count = ingest_payload(payload, name, bronze_dir)
            except (httpx.HTTPError, ParseError, zipfile.BadZipFile) as exc:
                # One bad file must not abort the run. The watermark is not
                # advanced for it, so the next run retries it.
                LOG.warning("skipping %s: %s", name, exc)
                continue
            LOG.info("wrote %s (%d rows)", destination.name, count)
            done.append(name)
            total_rows += count

    append_watermark(done, watermark)
    LOG.info("ingested %d files, %d rows", len(done), total_rows)
    return len(done)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest AEMO Dispatch SCADA into bronze.")
    parser.add_argument("--limit", type=int, default=None, help="ingest at most N pending files")
    parser.add_argument("--list-only", action="store_true", help="print pending files and exit")
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR, help="output directory")
    parser.add_argument("--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        run(limit=args.limit, list_only=args.list_only, bronze_dir=args.bronze_dir)
    except httpx.HTTPError as exc:
        LOG.error("could not reach AEMO: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
