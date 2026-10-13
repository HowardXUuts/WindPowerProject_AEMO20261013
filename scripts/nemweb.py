"""Shared ingestion core for AEMO NEMWeb reports.

Every NEMWeb report is published the same way: a directory of zipped CSVs, one
file per five-minute interval, each CSV packing several tables behind C/I/D row
markers. This module holds everything that does not vary between reports —
listing, downloading, parsing, the watermark, and the Parquet write — so adding
a report is a matter of declaring a ``ReportSpec``.

Each source file produces exactly one Parquet file, which is never rewritten.
Git history is append-only, so a file rewritten on every run leaves a permanent
blob behind each time; writing once per source interval keeps the repository
growing at roughly the rate of the data itself.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Final

import duckdb
import httpx

LOG: Final = logging.getLogger("nemweb")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
BRONZE_DIR: Final = REPO_ROOT / "data" / "bronze"
WATERMARK_NAME: Final = "_ingested.txt"

# AEMO publishes SETTLEMENTDATE in NEM time: AEST, UTC+10, no daylight saving.
NEM_TZ: Final = timezone(timedelta(hours=10), name="AEST")

SETTLEMENT_FORMATS: Final = ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S")

HTTP_TIMEOUT: Final = httpx.Timeout(30.0, connect=15.0)
USER_AGENT: Final = "aemo-wind/0.1 (+github.com/HowardXUuts/WindPowerProject_AEMO20261013)"


class ParseError(Exception):
    """The AEMO CSV payload could not be interpreted."""


class ListingError(Exception):
    """The AEMO directory listing could not be understood."""


# ---------------------------------------------------------------------------
# Report declaration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Column:
    """One bronze column, and how to get it from an AEMO source column."""

    name: str
    sql_type: str
    source: str
    parse: Callable[[str], object]


@dataclass(frozen=True)
class ReportSpec:
    """Everything that distinguishes one NEMWeb report from another."""

    key: str  # bronze subdirectory, e.g. "scada"
    url: str
    file_prefix: str  # e.g. "PUBLIC_DISPATCHSCADA"
    table: str  # table inside the report, e.g. "DISPATCH_UNIT_SCADA"
    columns: tuple[Column, ...]

    @property
    def filename_re(self) -> re.Pattern[str]:
        # Dispatch reports stamp the interval as YYYYMMDDHHMM; some NEMWeb
        # reports add seconds. Accept either, longest first.
        return re.compile(
            rf"{self.file_prefix}_(\d{{14}}|\d{{12}})_\d+\.zip", re.IGNORECASE
        )


# ---------------------------------------------------------------------------
# Field parsers
# ---------------------------------------------------------------------------


def as_text(value: str) -> str | None:
    """Return a trimmed, unquoted string, or None when blank."""
    cleaned = value.strip().strip('"').strip()
    return cleaned or None


def as_float(value: str) -> float | None:
    """Parse a numeric field, treating a blank as missing rather than zero."""
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError as exc:
        raise ParseError(f"not a number: {value!r}") from exc


def as_int(value: str) -> int | None:
    """Parse an integer flag field, treating a blank as missing."""
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return int(float(cleaned))  # AEMO writes some flags as "0.0"
    except ValueError as exc:
        raise ParseError(f"not an integer: {value!r}") from exc


def as_settlement_date(value: str) -> datetime:
    """Parse an AEMO SETTLEMENTDATE into a timezone-aware NEM-time datetime."""
    cleaned = value.strip().strip('"')
    for fmt in SETTLEMENT_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).replace(tzinfo=NEM_TZ)
        except ValueError:
            continue
    raise ParseError(f"unrecognised SETTLEMENTDATE: {value!r}")


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


def extract_rows(spec: ReportSpec, text: str, source_file: str) -> list[tuple]:
    """Return bronze rows for a report's table of interest."""
    ingested_at = datetime.now(UTC)
    rows: list[tuple] = []

    for table, record in parse_aemo_csv(text):
        if table.upper() != spec.table:
            continue
        try:
            values = [column.parse(record[column.source]) for column in spec.columns]
        except KeyError as exc:
            raise ParseError(f"{spec.table} row missing column {exc}") from exc
        rows.append((*values, source_file, ingested_at))

    if not rows:
        raise ParseError(f"no {spec.table} rows found in {source_file}")
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


def watermark_path(spec: ReportSpec, bronze_dir: Path = BRONZE_DIR) -> Path:
    """Return the watermark path for a report.

    Each report keeps its own watermark, so one source failing or falling behind
    never blocks the other.
    """
    return bronze_dir / spec.key / WATERMARK_NAME


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


def interval_from_filename(spec: ReportSpec, name: str) -> datetime:
    """Parse the interval timestamp embedded in an AEMO file name (NEM time)."""
    match = spec.filename_re.search(name)
    if match is None:
        raise ParseError(f"not a {spec.file_prefix} file name: {name!r}")
    stamp = match.group(1)
    fmt = "%Y%m%d%H%M%S" if len(stamp) == 14 else "%Y%m%d%H%M"
    return datetime.strptime(stamp, fmt).replace(tzinfo=NEM_TZ)


def list_remote_files(spec: ReportSpec, client: httpx.Client) -> list[str]:
    """Return the report's zip file names in the AEMO directory listing.

    A 200 response with no matching file names means the listing format moved
    out from under the pattern, not that AEMO published nothing: ``Current/``
    always holds about two days of files. Treating that as "nothing to ingest"
    produces a green run that silently ingests zero rows, so it raises instead.
    """
    response = client.get(spec.url)
    response.raise_for_status()
    names = sorted({match.group(0) for match in spec.filename_re.finditer(response.text)})
    if not names:
        excerpt = " ".join(response.text.split())[:200]
        raise ListingError(
            f"no {spec.file_prefix} file names in {spec.url} "
            f"({len(response.text)} bytes). First 200 chars: {excerpt!r}"
        )
    return names


def download(spec: ReportSpec, client: httpx.Client, name: str) -> bytes:
    """Download one source file."""
    response = client.get(spec.url + name)
    response.raise_for_status()
    return response.content


# ---------------------------------------------------------------------------
# Bronze write
# ---------------------------------------------------------------------------


def bronze_ddl(spec: ReportSpec) -> str:
    """Return the CREATE TABLE statement for a report's bronze schema."""
    columns = [f"{column.name} {column.sql_type}" for column in spec.columns]
    columns += ["source_file VARCHAR", "ingested_at TIMESTAMPTZ"]
    return "CREATE OR REPLACE TABLE bronze_rows (" + ", ".join(columns) + ")"


def hour_of(moment: datetime) -> datetime:
    """Truncate a NEM-time instant to the start of its hour."""
    return moment.replace(minute=0, second=0, microsecond=0)


def bronze_path(spec: ReportSpec, hour: datetime, bronze_dir: Path = BRONZE_DIR) -> Path:
    """Return the Parquet path for one closed NEM hour."""
    partition = f"date={hour.date().isoformat()}"
    return bronze_dir / spec.key / partition / f"hour={hour:%H}.parquet"


def write_bronze(spec: ReportSpec, rows: list[tuple], destination: Path) -> int:
    """Write bronze rows to Parquet and return the row count."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    literal = str(destination).replace("'", "''")
    placeholders = ", ".join("?" * (len(spec.columns) + 2))
    order_by = ", ".join(column.name for column in spec.columns[:2])

    con = duckdb.connect()
    try:
        con.execute(bronze_ddl(spec))
        con.executemany(f"INSERT INTO bronze_rows VALUES ({placeholders})", rows)
        con.execute(
            f"COPY (SELECT * FROM bronze_rows ORDER BY {order_by}) "
            f"TO '{literal}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    return len(rows)


def ingest_hour(
    spec: ReportSpec,
    hour: datetime,
    payloads: list[tuple[str, bytes]],
    bronze_dir: Path = BRONZE_DIR,
) -> tuple[Path, int]:
    """Parse one closed hour's archives and write a single Parquet file."""
    rows: list[tuple] = []
    for source_file, payload in payloads:
        text = read_zip_payload(payload, source_file)
        rows.extend(extract_rows(spec, text, source_file))

    destination = bronze_path(spec, hour, bronze_dir)
    return destination, write_bronze(spec, rows, destination)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def pending_files(spec: ReportSpec, available: list[str], ingested: set[str]) -> list[str]:
    """Return the files not yet ingested, oldest interval first."""
    return sorted(
        set(available) - ingested,
        key=lambda name: interval_from_filename(spec, name),
    )


def group_by_hour(spec: ReportSpec, names: list[str]) -> dict[datetime, list[str]]:
    """Group source file names by the NEM hour their interval falls in."""
    groups: dict[datetime, list[str]] = {}
    for name in names:
        groups.setdefault(hour_of(interval_from_filename(spec, name)), []).append(name)
    return {hour: sorted(files) for hour, files in sorted(groups.items())}


def select_closed_hours(
    spec: ReportSpec,
    pending: list[str],
    available: list[str],
    limit: int | None = None,
) -> list[tuple[datetime, list[str]]]:
    """Return whole, closed hours to ingest, oldest first.

    An hour is closed once AEMO has published an interval in a later hour, so
    no further file can land in it. Only closed hours are written, and always
    in full: a half-written hour would have to be rewritten when the rest
    arrived, and rewriting is what makes a committed-data repository grow
    without bound. The clock is never consulted — the listing itself says how
    far AEMO has got.

    ``limit`` caps the number of source files, rounded down to whole hours, so
    a cap can never split an hour across two runs. One hour is always returned
    if any is ready, even when it holds more files than the limit.
    """
    if not pending:
        return []

    latest = max(interval_from_filename(spec, name) for name in available)
    selected: list[tuple[datetime, list[str]]] = []
    taken = 0

    for hour, files in group_by_hour(spec, pending).items():
        if hour >= hour_of(latest):
            break  # the hour AEMO is still publishing into
        if limit is not None and selected and taken + len(files) > limit:
            break
        selected.append((hour, files))
        taken += len(files)

    return selected


def run(
    spec: ReportSpec,
    limit: int | None = None,
    list_only: bool = False,
    bronze_dir: Path = BRONZE_DIR,
) -> int:
    """Ingest all closed, un-ingested hours. Returns the number of hours written."""
    watermark = watermark_path(spec, bronze_dir)
    ingested = load_watermark(watermark)
    LOG.info("[%s] watermark holds %d previously ingested files", spec.key, len(ingested))

    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(timeout=HTTP_TIMEOUT, headers=headers, follow_redirects=True) as client:
        available = list_remote_files(spec, client)
        pending = pending_files(spec, available, ingested)
        hours = select_closed_hours(spec, pending, available, limit)
        LOG.info(
            "[%s] %d listed, %d pending, %d closed hours selected",
            spec.key,
            len(available),
            len(pending),
            len(hours),
        )

        if list_only:
            for hour, files in hours:
                print(f"{hour:%Y-%m-%d %H}:00  {len(files)} files")
            return 0
        if not hours:
            LOG.info("[%s] nothing to ingest", spec.key)
            return 0

        done: list[str] = []
        total_rows = 0
        written = 0
        for hour, files in hours:
            destination = bronze_path(spec, hour, bronze_dir)
            if destination.exists():
                # Write-once: an existing file means this hour is already done.
                LOG.warning("[%s] %s exists, skipping", spec.key, destination.name)
                done.extend(files)
                continue

            payloads: list[tuple[str, bytes]] = []
            try:
                for name in files:
                    payloads.append((name, download(spec, client, name)))
                _, count = ingest_hour(spec, hour, payloads, bronze_dir)
            except (httpx.HTTPError, ParseError, zipfile.BadZipFile) as exc:
                # Leave the whole hour un-watermarked so the next run retries it
                # in full, rather than committing a partial hour.
                LOG.warning("[%s] skipping hour %s: %s", spec.key, f"{hour:%Y-%m-%d %H}", exc)
                continue

            LOG.info(
                "[%s] wrote %s from %d files (%d rows)",
                spec.key,
                destination.relative_to(bronze_dir),
                len(files),
                count,
            )
            done.extend(files)
            total_rows += count
            written += 1

    append_watermark(done, watermark)
    LOG.info("[%s] wrote %d hours, %d files, %d rows", spec.key, written, len(done), total_rows)
    return written
