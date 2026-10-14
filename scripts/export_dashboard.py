"""Export small JSON slices from gold into docs/data/ for the dashboard.

GitHub Pages serves static files and runs no server code, so the page cannot
query Parquet. This writes exactly what each panel needs as JSON — a few
kilobytes, small enough to fetch on a phone connection — and nothing else.

Most slices come from gold so the page never carries the cost of aggregating
hundreds of thousands of rows in a browser. The per-farm detail is the
exception: it reads silver, because its subject is the five-minute shape that
gold has already averaged away. Gold is committed and rewritten every run, so
holding 96 farms x 288 intervals there would add megabytes to git history
hourly; these files are gitignored and rebuilt at deploy time instead.

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
SILVER_DIR: Final = REPO_ROOT / "data" / "silver"
SQL_DIR: Final = REPO_ROOT / "sql" / "dashboard"
OUT_DIR: Final = REPO_ROOT / "docs" / "data"

PARAM_RE: Final = re.compile(r"\$([a-z][a-z0-9_]*)")

# A DUID becomes a file name, so it may only be what AEMO actually issues.
DUID_RE: Final = re.compile(r"^[A-Z0-9_#-]{3,20}$")

# slice name -> the gold tables it needs
SLICES: Final = {
    "region_today": ("region_hourly",),
    "cf_trend": ("farm_daily",),
    "farm_cf_week": ("farm_daily",),
    "revenue_region": ("farm_revenue_daily",),
    "capture": ("capture_price",),
    "quality": ("quality",),
    "freshness": ("latest_snapshot",),
    "farm_index": ("farm_daily", "farm_revenue_daily"),
}

# Models fanned out to one file per farm, keyed by the column they split on.
# Run once each and grouped in Python: a query per farm would rescan silver 96
# times over.
FARM_DETAIL: Final = {
    "daily": ("farm_daily", "farm_revenue_daily", "capture_price"),
    "profile": ("silver",),
    "shape": ("silver",),
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


def run_model(con: duckdb.DuckDBPyConnection, path: Path, params: dict[str, str]) -> list[dict]:
    """Execute one dashboard model and return its rows as JSON-ready dicts."""
    if not path.exists():
        raise ExportError(f"missing dashboard model: {path}")
    sql = path.read_text()
    # DuckDB rejects a named parameter the statement does not use, so pass only
    # the ones this model actually references.
    used = {name: params[name] for name in set(PARAM_RE.findall(sql)) if name in params}
    cursor = con.execute(sql, used)
    columns = [d[0] for d in cursor.description]
    return [
        {col: json_safe(value) for col, value in zip(columns, row, strict=True)}
        for row in cursor.fetchall()
    ]


def group_by_duid(rows: list[dict]) -> dict[str, list[dict]]:
    """Split model rows per farm, dropping the duid from each row.

    The duid is the file name, so repeating it on every row would be hundreds of
    wasted bytes per farm.
    """
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        duid = row.get("duid")
        if not isinstance(duid, str) or not DUID_RE.match(duid):
            LOG.warning("skipping row with unusable duid %r", duid)
            continue
        grouped.setdefault(duid, []).append({k: v for k, v in row.items() if k != "duid"})
    return grouped


def export_farms(
    con: duckdb.DuckDBPyConnection,
    index: list[dict],
    params: dict[str, str],
    sql_dir: Path,
    out_dir: Path,
    have: set[str],
    dry_run: bool,
) -> int:
    """Write one JSON per farm for the farm detail page. Returns files written."""
    parts: dict[str, dict[str, list[dict]]] = {}
    for name, needs in FARM_DETAIL.items():
        missing = [need for need in needs if need not in have]
        if missing:
            # Still keyed, just empty: the page reads every section by name, and
            # a section that vanishes when an input is missing is a harder
            # contract to render against than one that is reliably present.
            LOG.warning("SKIP  farm/%-10s needs %s", name, ", ".join(missing))
            parts[name] = {}
            continue
        parts[name] = group_by_duid(run_model(con, sql_dir / f"farm_{name}_all.sql", params))

    if not any(parts.values()):
        return 0

    farm_dir = out_dir / "farm"
    written = 0
    total = 0
    for row in index:
        duid = row.get("duid")
        if not isinstance(duid, str) or not DUID_RE.match(duid):
            continue
        payload = {"farm": row}
        for name, grouped in parts.items():
            payload[name] = grouped.get(duid, [])
        if dry_run:
            written += 1
            continue
        farm_dir.mkdir(parents=True, exist_ok=True)
        destination = farm_dir / f"{duid}.json"
        destination.write_text(json.dumps(payload, separators=(",", ":")) + "\n")
        total += destination.stat().st_size
        written += 1

    if dry_run:
        LOG.info("DRY   farm/*          would write %d file(s)", written)
    else:
        LOG.info(
            "WROTE farm/*          %4d files  %6d bytes total (%d avg)",
            written, total, total // max(written, 1),
        )
    return written


def export(
    gold_dir: Path = GOLD_DIR,
    sql_dir: Path = SQL_DIR,
    out_dir: Path = OUT_DIR,
    silver_dir: Path = SILVER_DIR,
    dry_run: bool = False,
) -> dict[str, int]:
    """Write every slice whose inputs exist. Returns rows written per slice."""
    params = {name: str(gold_dir / f"{table}.parquet") for name, table in PARAM_TABLE.items()}
    params["wind_glob"] = str(silver_dir / "wind_output" / "*.parquet")
    params["price_glob"] = str(silver_dir / "price" / "*.parquet")

    have = {table for table in PARAM_TABLE.values() if (gold_dir / f"{table}.parquet").exists()}
    if any((silver_dir / "wind_output").glob("*.parquet")) and any(
        (silver_dir / "price").glob("*.parquet")
    ):
        have.add("silver")

    written: dict[str, int] = {}
    index: list[dict] = []
    con = duckdb.connect()
    try:
        for name, needs in SLICES.items():
            missing = [t for t in needs if t not in have]
            if missing:
                LOG.warning("SKIP  %-16s needs %s", name, ", ".join(missing))
                continue

            rows = run_model(con, sql_dir / f"{name}.sql", params)
            if name == "farm_index":
                index = rows

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

        farms = export_farms(con, index, params, sql_dir, out_dir, have, dry_run)
        if farms:
            written["farm_detail"] = farms
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
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR)
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
            silver_dir=args.silver_dir,
            dry_run=args.dry_run,
        )
    except ExportError as exc:
        LOG.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
