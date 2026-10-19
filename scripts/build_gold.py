"""Build the gold layer: aggregates and dashboard-ready tables.

Gold is a full rewrite every run, unlike bronze and silver. Each table is a
derived view of everything below it, so there is no incremental state to get
wrong, and at a few hundred kilobytes the rewrite costs less than the
machinery incremental updates would need. The trade-off is that gold does add
a blob to git history on every run that changes it, which is affordable only
because the tables are small — see CLAUDE.md's layer contract.

A model whose inputs do not exist is skipped rather than written empty, and
any table already on disk is left alone. An empty table published over a good
one would look like a real answer.

Usage:
    python scripts/build_gold.py
    python scripts/build_gold.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import duckdb

LOG: Final = logging.getLogger("build_gold")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
SILVER_DIR: Final = REPO_ROOT / "data" / "silver"
BRONZE_DIR: Final = REPO_ROOT / "data" / "bronze"
GOLD_DIR: Final = REPO_ROOT / "data" / "gold"
SQL_DIR: Final = REPO_ROOT / "sql" / "gold"
REGISTRY: Final = REPO_ROOT / "data" / "reference" / "wind_duids.csv"

PARAM_RE: Final = re.compile(r"\$([a-z][a-z0-9_]*)")


@dataclass(frozen=True)
class Model:
    """One gold table: a SQL model and the inputs it needs."""

    name: str
    needs: tuple[str, ...]  # wind, price, bronze_scada, bronze_regionsum, registry


MODELS: Final = (
    Model("farm_daily", ("wind",)),
    Model("farm_revenue_daily", ("wind", "price")),
    Model("capture_price", ("wind", "price")),
    Model("region_hourly", ("wind",)),
    Model("latest_snapshot", ("wind", "bronze_scada", "registry")),
    Model("revenue_concentration", ("wind", "price")),
    # Regional demand is the only input outside silver: penetration needs the
    # denominator, and silver holds wind units and price, not regional totals.
    Model("cannibalisation", ("wind", "price", "bronze_regionsum")),
    Model("negative_price_runs", ("wind", "price")),
    Model("negative_price_worst", ("wind", "price")),
)


class GoldError(Exception):
    """Gold could not be built from the inputs given."""


def available(silver_dir: Path, bronze_dir: Path, registry: Path) -> set[str]:
    """Return which inputs exist."""
    present: set[str] = set()
    if any((silver_dir / "wind_output").glob("*.parquet")):
        present.add("wind")
    if any((silver_dir / "price").glob("*.parquet")):
        present.add("price")
    if any((bronze_dir / "scada").glob("**/*.parquet")):
        present.add("bronze_scada")
    if any((bronze_dir / "regionsum").glob("**/*.parquet")):
        present.add("bronze_regionsum")
    if registry.exists():
        present.add("registry")
    return present


def build(
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR,
    gold_dir: Path = GOLD_DIR,
    registry: Path = REGISTRY,
    sql_dir: Path = SQL_DIR,
    dry_run: bool = False,
) -> dict[str, int]:
    """Rewrite every gold table whose inputs exist. Returns rows written per table."""
    present = available(silver_dir, bronze_dir, registry)
    params: dict[str, object] = {
        "wind_glob": str(silver_dir / "wind_output" / "*.parquet"),
        "price_glob": str(silver_dir / "price" / "*.parquet"),
        "bronze_glob": str(bronze_dir / "scada" / "**" / "*.parquet"),
        "regionsum_glob": str(bronze_dir / "regionsum" / "**" / "*.parquet"),
        "registry": str(registry),
    }

    written: dict[str, int] = {}
    con = duckdb.connect()
    # Single-threaded on purpose. Floating-point addition is not associative, so
    # a parallel sum adds its partials in whatever order the threads finish and
    # the last bit of an aggregate moves between runs on identical input. Gold
    # is rewritten and committed every hour, so that alone was entering a new
    # Parquet blob into git history hourly for no change in the data — measured
    # at 34 KB a run across capture_price and region_hourly, about 300 MB a
    # year. Rounding the outputs narrows it but cannot close it: a true value
    # sitting on a rounding boundary still tips either way. One thread makes the
    # summation order deterministic, which does close it. Gold is a few hundred
    # thousand rows, so the whole build costs seconds either way.
    con.execute("SET threads TO 1")
    try:
        for model in MODELS:
            missing = [need for need in model.needs if need not in present]
            if missing:
                LOG.warning("SKIP  %-20s needs %s", model.name, ", ".join(missing))
                continue

            path = sql_dir / f"{model.name}.sql"
            if not path.exists():
                raise GoldError(f"missing gold model: {path}")

            sql = path.read_text()
            used = {name: params[name] for name in set(PARAM_RE.findall(sql)) if name in params}

            if dry_run:
                (count,) = con.execute(
                    f"SELECT count(*) FROM ({sql})", used
                ).fetchone()
                LOG.info("DRY   %-20s would write %d rows", model.name, count)
                continue

            destination = gold_dir / f"{model.name}.parquet"
            destination.parent.mkdir(parents=True, exist_ok=True)
            literal = str(destination).replace("'", "''")
            con.execute(
                f"COPY ({sql}) TO '{literal}' (FORMAT PARQUET, COMPRESSION ZSTD)", used
            )
            (count,) = con.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(destination)]
            ).fetchone()
            LOG.info(
                "WROTE %-20s %6d rows  %8d bytes",
                model.name, count, destination.stat().st_size,
            )
            written[model.name] = int(count)
    finally:
        con.close()

    if not dry_run:
        LOG.info("rewrote %d gold table(s)", len(written))
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the gold layer.")
    parser.add_argument("--silver-dir", type=Path, default=SILVER_DIR)
    parser.add_argument("--bronze-dir", type=Path, default=BRONZE_DIR)
    parser.add_argument("--gold-dir", type=Path, default=GOLD_DIR)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--sql-dir", type=Path, default=SQL_DIR)
    parser.add_argument("--dry-run", action="store_true", help="report row counts, write nothing")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        build(
            silver_dir=args.silver_dir,
            bronze_dir=args.bronze_dir,
            gold_dir=args.gold_dir,
            registry=args.registry,
            sql_dir=args.sql_dir,
            dry_run=args.dry_run,
        )
    except GoldError as exc:
        LOG.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
