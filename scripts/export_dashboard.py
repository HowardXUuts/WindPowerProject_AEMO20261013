"""Export small JSON slices from gold into docs/data/ for the dashboard.

GitHub Pages serves static files and runs no server code, so the page cannot
query Parquet. This writes exactly what each panel needs as JSON — a few
kilobytes, small enough to fetch on a phone connection — and nothing else.

The slices come from gold rather than silver so the page never carries the
cost of aggregating hundreds of thousands of rows in a browser.

Usage:
    python scripts/export_dashboard.py
    python scripts/export_dashboard.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Final

import duckdb

LOG: Final = logging.getLogger("export_dashboard")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
GOLD_DIR: Final = REPO_ROOT / "data" / "gold"
SQL_DIR: Final = REPO_ROOT / "sql" / "dashboard"
OUT_DIR: Final = REPO_ROOT / "docs" / "data"

PARAM_RE: Final = re.compile(r"\$([a-z][a-z0-9_]*)")

# slice name -> the gold tables it needs
SLICES: Final = {
    "region_today": ("region_hourly",),
    "cf_trend": ("farm_daily",),
    "farm_cf_week": ("farm_daily",),
    "revenue_region": ("farm_revenue_daily",),
    "capture": ("capture_price",),
    "quality": ("quality",),
    "freshness": ("latest_snapshot",),
}

# SQL parameter name -> gold table file stem
PARAM_TABLE: Final = {
    "region_hourly": "region_hourly",
    "farm_daily": "farm_daily",
    "farm_revenue": "farm_revenue_daily",
    "capture": "capture_price",
    "quality": "quality",
    "snapshot": "latest_snapshot",
}


class ExportError(Exception):
    """The dashboard slices could not be exported."""


def json_safe(value: object) -> object:
    """Render a DuckDB value as something json.dumps accepts.

    Decimals become floats rather than strings: DuckDB returns DECIMAL for some
    aggregates, and a quoted number in the JSON would reach the page as a string
    that silently breaks arithmetic and chart scales.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def export(
    gold_dir: Path = GOLD_DIR,
    sql_dir: Path = SQL_DIR,
    out_dir: Path = OUT_DIR,
    dry_run: bool = False,
) -> dict[str, int]:
    """Write every slice whose gold tables exist. Returns rows written per slice."""
    params = {
        name: str(gold_dir / f"{table}.parquet") for name, table in PARAM_TABLE.items()
    }

    written: dict[str, int] = {}
    con = duckdb.connect()
    try:
        for name, needs in SLICES.items():
            missing = [t for t in needs if not (gold_dir / f"{t}.parquet").exists()]
            if missing:
                LOG.warning("SKIP  %-16s needs %s", name, ", ".join(missing))
                continue

            path = sql_dir / f"{name}.sql"
            if not path.exists():
                raise ExportError(f"missing dashboard model: {path}")

            sql = path.read_text()
            used = {p: params[p] for p in set(PARAM_RE.findall(sql)) if p in params}
            cursor = con.execute(sql, used)
            columns = [d[0] for d in cursor.description]
            rows = [
                {col: json_safe(value) for col, value in zip(columns, row, strict=True)}
                for row in cursor.fetchall()
            ]

            if dry_run:
                LOG.info("DRY   %-16s %d rows", name, len(rows))
                continue

            out_dir.mkdir(parents=True, exist_ok=True)
            destination = out_dir / f"{name}.json"
            # Separators without spaces: these files are fetched over mobile
            # connections and nobody reads them by hand.
            destination.write_text(json.dumps(rows, separators=(",", ":")) + "\n")
            LOG.info(
                "WROTE %-16s %4d rows  %6d bytes", name, len(rows), destination.stat().st_size
            )
            written[name] = len(rows)
    finally:
        con.close()

    if not dry_run and written:
        meta = {
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "slices": sorted(written),
        }
        (out_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
        LOG.info("wrote %d slice(s) to %s", len(written), out_dir)

    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export dashboard JSON from gold.")
    parser.add_argument("--gold-dir", type=Path, default=GOLD_DIR)
    parser.add_argument("--sql-dir", type=Path, default=SQL_DIR)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        export(
            gold_dir=args.gold_dir,
            sql_dir=args.sql_dir,
            out_dir=args.out_dir,
            dry_run=args.dry_run,
        )
    except ExportError as exc:
        LOG.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
