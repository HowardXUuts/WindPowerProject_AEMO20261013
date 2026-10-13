"""Refresh the wind DUID registry from AEMO's registration list.

AEMO publishes the NEM Registration and Exemption List as a spreadsheet whose
"Generators and Scheduled Loads" sheet maps each DUID to its station, region,
fuel source and registered capacity. That mapping is what turns bronze's bare
DUIDs into wind farms with a region and a capacity factor denominator.

It changes only when a generator is registered or retired, so this is a manual
refresh rather than part of the hourly pipeline: run it, review the diff, and
commit. The published layout shifts occasionally, so the parser locates the
header row and columns by name and reports what it found when it cannot.

Usage:
    python scripts/refresh_reference.py                  # download and write
    python scripts/refresh_reference.py --from-file x.xlsx
    python scripts/refresh_reference.py --inspect        # dump sheets/columns

Needs the reference extras: pip install '.[reference]'
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

import httpx

LOG: Final = logging.getLogger("refresh_reference")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
OUT_CSV: Final = REPO_ROOT / "data" / "reference" / "wind_duids.csv"

SOURCE_URL: Final = (
    "https://aemo.com.au/-/media/files/electricity/nem/participant_information/"
    "nem-registration-and-exemption-list.xls"
)

SHEET_HINTS: Final = ("generators and scheduled loads", "generators", "scheduled loads")
NEM_REGIONS: Final = ("NSW1", "QLD1", "SA1", "TAS1", "VIC1")

# Candidate spellings for each output column, matched case-insensitively against
# the header row. AEMO renames these between editions.
COLUMN_HINTS: Final = {
    "duid": ("duid",),
    "station_name": ("station name", "station", "site name"),
    "region": ("region", "region id", "regionid"),
    "fuel_type": (
        "fuel source - descriptor",
        "fuel source descriptor",
        "fuel source - primary",
        "fuel source",
        "technology type - descriptor",
        "technology type - primary",
        "technology type",
    ),
    "capacity_mw": (
        "reg cap (mw)",
        "reg cap generation (mw)",
        "registered capacity (mw)",
        "nameplate capacity (mw)",
        "max cap (mw)",
    ),
}

HEADER: Final = ("duid", "station_name", "region", "fuel_type", "capacity_mw")
WIND_RE: Final = re.compile(r"wind", re.IGNORECASE)
HTTP_TIMEOUT: Final = httpx.Timeout(120.0, connect=30.0)
USER_AGENT: Final = "aemo-wind/0.1 (+github.com/HowardXUuts/WindPowerProject_AEMO20261013)"


class RegistryError(Exception):
    """The registration list could not be interpreted."""


def download(url: str = SOURCE_URL) -> bytes:
    """Fetch the registration spreadsheet."""
    LOG.info("downloading %s", url)
    with httpx.Client(
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    ) as client:
        response = client.get(url)
        response.raise_for_status()
    LOG.info("got %d bytes (%s)", len(response.content), response.headers.get("content-type", "?"))
    return response.content


def read_sheets(payload: bytes) -> dict[str, object]:
    """Read every sheet of an .xls or .xlsx workbook into DataFrames."""
    import io

    import pandas as pd

    errors: list[str] = []
    for engine in ("openpyxl", "xlrd", "calamine"):
        try:
            return pd.read_excel(io.BytesIO(payload), sheet_name=None, header=None, engine=engine)
        except Exception as exc:  # noqa: BLE001 - try the next engine
            errors.append(f"{engine}: {type(exc).__name__}: {exc}")
    raise RegistryError("could not open the workbook. " + " | ".join(errors))


def pick_sheet(sheets: dict[str, object]) -> str:
    """Return the sheet most likely to hold the generator table."""
    for hint in SHEET_HINTS:
        for name in sheets:
            if hint in name.strip().lower():
                return name
    # Fall back to whichever sheet mentions DUID at all.
    for name, frame in sheets.items():
        if frame.astype(str).apply(lambda col: col.str.fullmatch("DUID", case=False)).any().any():
            return name
    raise RegistryError(f"no sheet looks like the generator table. Sheets: {list(sheets)}")


def find_header_row(frame: object) -> int:
    """Return the index of the row holding the column names."""
    for index in range(min(len(frame), 30)):
        values = [str(value).strip().lower() for value in frame.iloc[index].tolist()]
        if "duid" in values:
            return index
    raise RegistryError("no header row containing 'DUID' in the first 30 rows")


def map_columns(columns: list[str]) -> dict[str, str]:
    """Map each output column to the source column that supplies it."""
    normalised = {str(name).strip().lower(): name for name in columns}
    mapping: dict[str, str] = {}
    for field, hints in COLUMN_HINTS.items():
        for hint in hints:
            if hint in normalised:
                mapping[field] = normalised[hint]
                break
    missing = [field for field in HEADER if field not in mapping]
    if missing:
        raise RegistryError(f"columns not found: {missing}. Header row holds: {list(normalised)}")
    return mapping


def normalise_region(value: str) -> str | None:
    """Return a canonical NEM region id, or None if the row has no usable one."""
    text = str(value).strip().upper().replace(" ", "")
    if text in NEM_REGIONS:
        return text
    # Some editions write "NSW" rather than "NSW1".
    for region in NEM_REGIONS:
        if text == region[:-1]:
            return region
    return None


def normalise_capacity(value: object) -> float | None:
    """Parse a registered capacity, tolerating blanks and stray characters."""
    text = re.sub(r"[^0-9.\-]", "", str(value))
    if not text or text in {"-", "."}:
        return None
    try:
        capacity = float(text)
    except ValueError:
        return None
    return capacity if capacity > 0 else None


def extract_wind(payload: bytes) -> list[dict[str, object]]:
    """Return one row per wind DUID in the registration list."""
    sheets = read_sheets(payload)
    sheet_name = pick_sheet(sheets)
    frame = sheets[sheet_name]
    LOG.info("using sheet %r (%d rows)", sheet_name, len(frame))

    header_row = find_header_row(frame)
    columns = [str(value).strip() for value in frame.iloc[header_row].tolist()]
    mapping = map_columns(columns)
    LOG.info("header row %d; using columns %s", header_row, mapping)

    body = frame.iloc[header_row + 1 :].copy()
    body.columns = columns

    records: dict[str, dict[str, object]] = {}
    skipped_region = 0
    for _, row in body.iterrows():
        duid = str(row[mapping["duid"]]).strip().upper()
        if not duid or duid in {"NAN", "-", "DUID"}:
            continue

        fuel = str(row[mapping["fuel_type"]]).strip()
        if not WIND_RE.search(fuel):
            continue

        region = normalise_region(row[mapping["region"]])
        if region is None:
            # A generator with no NEM region is not dispatchable here.
            skipped_region += 1
            continue

        records[duid] = {
            "duid": duid,
            "station_name": " ".join(str(row[mapping["station_name"]]).split()),
            "region": region,
            "fuel_type": " ".join(fuel.split()),
            "capacity_mw": normalise_capacity(row[mapping["capacity_mw"]]),
        }

    if skipped_region:
        LOG.warning("skipped %d wind rows with no recognisable NEM region", skipped_region)
    if not records:
        raise RegistryError("no wind rows found; the fuel column may have moved")

    LOG.info("found %d wind DUIDs", len(records))
    return [records[duid] for duid in sorted(records)]


def write_csv(rows: list[dict[str, object]], destination: Path) -> None:
    """Write the registry, sorted by DUID, with a sidecar provenance file."""
    import csv

    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_meta(rows: list[dict[str, object]], payload: bytes, url: str, destination: Path) -> None:
    """Record where the registry came from, so a stale file is obvious."""
    meta = {
        "source_url": url,
        "retrieved_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "source_sha256": hashlib.sha256(payload).hexdigest(),
        "source_bytes": len(payload),
        "wind_duids": len(rows),
        "regions": sorted({str(row["region"]) for row in rows}),
        "total_capacity_mw": round(
            sum(float(row["capacity_mw"]) for row in rows if row["capacity_mw"]), 1
        ),
    }
    destination.write_text(json.dumps(meta, indent=2) + "\n")


def inspect(payload: bytes) -> None:
    """Print the workbook's structure, for when the layout has moved."""
    sheets = read_sheets(payload)
    for name, frame in sheets.items():
        print(f"\n=== sheet {name!r}: {frame.shape[0]} rows x {frame.shape[1]} cols ===")
        for index in range(min(len(frame), 5)):
            values = [str(v)[:28] for v in frame.iloc[index].tolist()[:12]]
            print(f"  row {index}: {values}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Refresh the wind DUID registry from AEMO.")
    parser.add_argument("--url", default=SOURCE_URL, help="registration list URL")
    parser.add_argument("--from-file", type=Path, default=None, help="use a local copy instead")
    parser.add_argument("--out", type=Path, default=OUT_CSV, help="output CSV path")
    parser.add_argument("--inspect", action="store_true", help="dump sheets and columns, then exit")
    parser.add_argument("--verbose", action="store_true", help="enable debug logging")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        payload = args.from_file.read_bytes() if args.from_file else download(args.url)
    except httpx.HTTPError as exc:
        LOG.error("could not download the registration list: %s", exc)
        return 1

    try:
        if args.inspect:
            inspect(payload)
            return 0
        rows = extract_wind(payload)
    except RegistryError as exc:
        LOG.error("%s", exc)
        LOG.error("run with --inspect to dump the workbook's sheets and header rows")
        return 1

    write_csv(rows, args.out)
    write_meta(rows, payload, args.url, args.out.with_suffix(".meta.json"))

    capacity = sum(float(row["capacity_mw"]) for row in rows if row["capacity_mw"])
    missing = sum(1 for row in rows if not row["capacity_mw"])
    LOG.info("wrote %s: %d wind DUIDs, %.1f MW registered", args.out, len(rows), capacity)
    if missing:
        LOG.warning("%d DUIDs have no registered capacity (capacity factor will be null)", missing)
    return 0


if __name__ == "__main__":
    sys.exit(main())
