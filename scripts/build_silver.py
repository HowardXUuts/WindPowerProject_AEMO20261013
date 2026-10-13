"""Build the silver layer from bronze, one immutable file per trading date.

Silver files are write-once, like bronze: a rewritten file is a permanent extra
blob in git history. That makes *when* a day may be written the central
question, because a day written early can never be completed later.

A trading date is written once either of these holds:

  complete  bronze holds all 288 five-minute intervals for the date
  sealed    AEMO can no longer supply the missing ones

Sealing matters because AEMO's Current/ directory retains only about two days.
A day with a permanent gap — a run that failed while the files were still
available, or a backfill started mid-day — would otherwise never reach 288 and
would block silver for that date forever. Once bronze has observed intervals
more than the retention window past the end of a day, the missing files are
gone for good, so the day is written as it stands and every row carries
intervals_present and is_complete to say so.

The escape hatch, deliberately manual: if a day is later recovered from AEMO's
archive, delete that silver file and re-run. Nothing automatic rewrites a
silver file.

Usage:
    python scripts/build_silver.py                 # write every ready date
    python scripts/build_silver.py --dry-run       # report, write nothing
    python scripts/build_silver.py --date 2026-10-12
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Final

import duckdb

LOG: Final = logging.getLogger("build_silver")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
BRONZE_DIR: Final = REPO_ROOT / "data" / "bronze"
SILVER_DIR: Final = REPO_ROOT / "data" / "silver"
SQL_DIR: Final = REPO_ROOT / "sql" / "silver"
REGISTRY: Final = REPO_ROOT / "data" / "reference" / "wind_duids.csv"

# 24 hours of five-minute dispatch intervals.
INTERVALS_PER_DAY: Final = 288

# AEMO's Current/ directory holds about two days. A day is sealed once bronze
# has seen intervals this far past its end, because the missing source files
# have left the server by then.
SEAL_AFTER_DAYS: Final = 2

NEM_TZ_NAME: Final = "Australia/Brisbane"  # UTC+10 year-round, like NEM time


class SilverError(Exception):
    """Silver could not be built from the inputs given."""


def read_sql(name: str, sql_dir: Path = SQL_DIR) -> str:
    """Load a silver SQL model by file name."""
    path = sql_dir / name
    if not path.exists():
        raise SilverError(f"missing SQL model: {path}")
    return path.read_text()


def source_glob(bronze_dir: Path, source: str) -> str:
    """Return the Parquet glob for one bronze source."""
    return str(bronze_dir / source / "**" / "*.parquet")


def has_bronze(bronze_dir: Path, source: str) -> bool:
    """Return whether any bronze file exists for a source."""
    return any((bronze_dir / source).glob("**/*.parquet"))


def check_registry(registry: Path) -> None:
    """Fail early and clearly if the registry predates the in_scada column."""
    if not registry.exists():
        raise SilverError(f"no wind DUID registry at {registry}")
    header = registry.read_text().splitlines()[0].split(",")
    if "in_scada" not in header:
        raise SilverError(
            f"{registry} has no in_scada column (found: {header}). "
            "Re-run the 'Refresh wind DUID registry' workflow."
        )


def day_coverage(
    con: duckdb.DuckDBPyConnection,
    glob: str,
) -> tuple[dict[date, int], date | None]:
    """Return intervals present per trading date, and the latest interval seen.

    Dated by interval start, matching the SQL models: the interval ending
    00:00 belongs to the previous day.
    """
    rows = con.execute(
        f"""
        WITH starts AS (
            SELECT DISTINCT
                (settlementdate - INTERVAL 5 MINUTE)
                    AT TIME ZONE '{NEM_TZ_NAME}' AS interval_start_nem
            FROM read_parquet(?)
        )
        SELECT CAST(interval_start_nem AS DATE) AS trading_date, count(*) AS intervals
        FROM starts GROUP BY 1 ORDER BY 1
        """,
        [glob],
    ).fetchall()

    coverage = {row[0]: int(row[1]) for row in rows}
    latest = max(coverage) if coverage else None
    return coverage, latest


def is_sealed(trading_date: date, latest_date: date | None, seal_after_days: int) -> bool:
    """Return whether AEMO can no longer supply missing intervals for a date.

    Judged from the data rather than the clock: the newest interval bronze has
    seen is what says how far AEMO has moved on. A wall-clock check would seal
    days during a long ingestion outage, writing gaps that were still fillable.
    """
    if latest_date is None:
        return False
    return latest_date > trading_date + timedelta(days=seal_after_days)


def ready_dates(
    coverage: dict[date, int],
    latest_date: date | None,
    seal_after_days: int = SEAL_AFTER_DAYS,
) -> list[tuple[date, int, bool]]:
    """Return (date, intervals_present, is_complete) for every writable date."""
    ready: list[tuple[date, int, bool]] = []
    for trading_date in sorted(coverage):
        intervals = coverage[trading_date]
        complete = intervals >= INTERVALS_PER_DAY
        if complete or is_sealed(trading_date, latest_date, seal_after_days):
            ready.append((trading_date, intervals, complete))
    return ready


def silver_path(silver_dir: Path, model: str, trading_date: date) -> Path:
    """Return the Parquet path for one model and trading date."""
    return silver_dir / model / f"date={trading_date.isoformat()}.parquet"


def write_day(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    params: dict[str, object],
    destination: Path,
) -> int:
    """Run a silver model for one date and write it, returning the row count."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    literal = str(destination).replace("'", "''")
    con.execute(
        f"COPY ({sql}) TO '{literal}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        params,
    )
    (count,) = con.execute("SELECT count(*) FROM read_parquet(?)", [str(destination)]).fetchone()
    return int(count)


def build(
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    registry: Path = REGISTRY,
    sql_dir: Path = SQL_DIR,
    only: date | None = None,
    seal_after_days: int = SEAL_AFTER_DAYS,
    dry_run: bool = False,
) -> int:
    """Build every writable trading date. Returns the number of files written."""
    check_registry(registry)

    models = {
        "wind_output": ("scada", read_sql("wind_output.sql", sql_dir)),
        "price": ("price", read_sql("price.sql", sql_dir)),
    }

    written = 0
    con = duckdb.connect()
    try:
        for model, (source, sql) in models.items():
            if not has_bronze(bronze_dir, source):
                LOG.warning("[%s] no bronze under %s, skipping", model, bronze_dir / source)
                continue

            glob = source_glob(bronze_dir, source)
            coverage, latest = day_coverage(con, glob)
            ready = ready_dates(coverage, latest, seal_after_days)

            for trading_date, intervals, complete in ready:
                if only is not None and trading_date != only:
                    continue

                destination = silver_path(silver_dir, model, trading_date)
                if destination.exists():
                    LOG.debug("[%s] %s already written", model, trading_date)
                    continue

                state = "complete" if complete else "sealed incomplete"
                if dry_run:
                    LOG.info(
                        "[%s] would write %s (%d/%d intervals, %s)",
                        model, trading_date, intervals, INTERVALS_PER_DAY, state,
                    )
                    continue

                params: dict[str, object] = {
                    "trading_date": trading_date,
                    "intervals_present": intervals,
                    "is_complete": complete,
                }
                params["price_glob" if model == "price" else "scada_glob"] = glob
                if model == "wind_output":
                    params["registry"] = str(registry)

                rows = write_day(con, sql, params, destination)
                if not complete:
                    LOG.warning(
                        "[%s] %s written with %d/%d intervals: sealed, AEMO no longer "
                        "holds the rest",
                        model, trading_date, intervals, INTERVALS_PER_DAY,
                    )
                LOG.info(
                    "[%s] wrote %s (%d rows, %d/%d intervals, %s)",
                    model, destination.name, rows, intervals, INTERVALS_PER_DAY, state,
                )
                written += 1

            waiting = [d for d in sorted(coverage) if d not in {r[0] for r in ready}]
            if waiting:
                LOG.info(
                    "[%s] waiting on %s",
                    model,
                    ", ".join(f"{d} ({coverage[d]}/{INTERVALS_PER_DAY})" for d in waiting),
                )
    finally:
        con.close()

    LOG.info("wrote %d silver file(s)", written)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the silver layer from bronze.")
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR)
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--sql-dir", type=Path, default=SQL_DIR)
    parser.add_argument("--date", type=date.fromisoformat, default=None, help="one date only")
    parser.add_argument(
        "--seal-after-days",
        type=int,
        default=SEAL_AFTER_DAYS,
        help="days past a date's end after which missing intervals are unrecoverable",
    )
    parser.add_argument("--dry-run", action="store_true", help="report, write nothing")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        build(
            bronze_dir=args.bronze_dir,
            silver_dir=args.silver_dir,
            registry=args.registry,
            sql_dir=args.sql_dir,
            only=args.date,
            seal_after_days=args.seal_after_days,
            dry_run=args.dry_run,
        )
    except SilverError as exc:
        LOG.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
