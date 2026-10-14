"""Print the real table names, headers and a sample row from a NEMWeb report.

Column names cannot be guessed. A wrong guess either fails the run or, worse,
quietly writes a column of nulls that nobody notices until a model trained on
it underperforms for reasons no one can explain. This downloads one file and
prints exactly what AEMO sent, so a ReportSpec can be written against the real
headers rather than against a recollection of them.

It is deliberately not part of any scheduled workflow: it downloads a file and
writes nothing. Run it by hand, or through the "Inspect AEMO report" workflow,
and paste the output into the spec.

Usage:
    python scripts/inspect_report.py p5min
    python scripts/inspect_report.py dispatchis --table DISPATCH_REGIONSUM
    python scripts/inspect_report.py predispatchis --rows 3
    python scripts/inspect_report.py all
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from typing import Final

import httpx
from nemweb import (
    HTTP_TIMEOUT,
    LOG,
    USER_AGENT,
    ListingError,
    ParseError,
    ReportSpec,
    as_text,
    download,
    list_remote_files,
    parse_aemo_csv,
    read_zip_payload,
)

BASE: Final = "https://nemweb.com.au/Reports/Current/"


@dataclass(frozen=True)
class Report:
    """A report to inspect, named only by where it lives."""

    name: str
    url: str
    file_prefix: str
    note: str


# Only the directory and the file prefix are asserted here — everything else is
# what we are trying to find out. A placeholder TableSpec satisfies ReportSpec
# without claiming to know any column.
REPORTS: Final = (
    Report("scada", BASE + "Dispatch_SCADA/", "PUBLIC_DISPATCHSCADA",
           "unit MW, already ingested"),
    Report("dispatchis", BASE + "DispatchIS_Reports/", "PUBLIC_DISPATCHIS",
           "price, regionsum, interconnectors"),
    Report("p5min", BASE + "P5_Reports/", "PUBLIC_P5MIN",
           "five-minute pre-dispatch, 12 intervals ahead, one file every 5 min"),
    Report("predispatchis", BASE + "PredispatchIS_Reports/", "PUBLIC_PREDISPATCHIS",
           "half-hourly pre-dispatch, one file every 30 min"),
)


def spec_for(report: Report) -> ReportSpec:
    """A ReportSpec good enough to list and download, declaring no tables.

    Listing and downloading need only the directory and the file name pattern.
    Leaving ``tables`` empty is the point: this tool must not assert a single
    column name, because finding them out is what it is for.
    """
    return ReportSpec(url=report.url, file_prefix=report.file_prefix, tables=())


def inspect(report: Report, client: httpx.Client, rows: int, only: str | None) -> int:
    """Print every table in the report's newest file. Returns tables found."""
    spec = spec_for(report)
    names = list_remote_files(spec, client)
    newest = names[-1]
    payload = download(spec, client, newest)
    text = read_zip_payload(payload, newest)

    print(f"\n{'=' * 78}\n{report.name}  —  {report.note}")
    print(f"{report.url}\nnewest of {len(names)} files: {newest}  ({len(payload):,} bytes zipped, "
          f"{len(text):,} bytes of CSV)\n{'=' * 78}")

    seen: dict[str, list[dict[str, str]]] = {}
    for table, record in parse_aemo_csv(text):
        seen.setdefault(table, []).append(record)

    for table, records in seen.items():
        if only and only.upper() not in table.upper():
            continue
        print(f"\n--- {table}   ({len(records):,} rows)")
        print(f"    columns ({len(records[0])}): {', '.join(records[0])}")
        for record in records[:rows]:
            shown = ", ".join(f"{k}={as_text(v)!r}" for k, v in record.items())
            print(f"    sample: {shown}")

    if only and not any(only.upper() in table.upper() for table in seen):
        print(f"\n    NO TABLE MATCHING {only!r}. This file holds: {', '.join(seen)}")
    return len(seen)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "report",
        choices=[r.name for r in REPORTS] + ["all"],
        help="which NEMWeb report to inspect",
    )
    parser.add_argument("--table", help="only show tables whose name contains this")
    parser.add_argument("--rows", type=int, default=1, help="sample rows per table")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    chosen = REPORTS if args.report == "all" else [r for r in REPORTS if r.name == args.report]
    headers = {"User-Agent": USER_AGENT}
    failed = 0
    with httpx.Client(timeout=HTTP_TIMEOUT, headers=headers, follow_redirects=True) as client:
        for report in chosen:
            try:
                inspect(report, client, args.rows, args.table)
            except (httpx.HTTPError, ListingError, ParseError) as exc:
                LOG.error("[%s] %s", report.name, exc)
                failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
