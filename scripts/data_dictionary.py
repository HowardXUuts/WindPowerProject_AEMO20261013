"""Generate DATA_DICTIONARY.md from the models and the data itself.

    python scripts/data_dictionary.py            # write DATA_DICTIONARY.md
    python scripts/data_dictionary.py --check    # fail if it is out of date

Written rather than hand-maintained because a hand-maintained dictionary goes
stale silently. The README carried three wrong facts by the time anyone
noticed — two feeds where there are seven, six gold tables where there are
thirteen — and a 140-column dictionary would rot faster than prose does.

Every fact here comes from something that cannot drift from the code:

- the purpose line from each model's own `-- Gold:` / `-- Silver:` header,
- bronze columns from the `TableSpec`s the ingesters declare, including the
  AEMO column each one is parsed from,
- gold and silver columns and types from the Parquet actually on disk.

A table whose model exists but whose Parquet does not is reported as not built,
because that is a real state: gold skips a model whose inputs are missing.

Deliberately **schema only** — no row counts, no date spans. Those change on
every ingest, which would make `--check` fail hourly and train everyone to
ignore it. Volume belongs in the repo size report, not here.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Final

import duckdb

LOG: Final = logging.getLogger("data_dictionary")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
SQL_DIR: Final = REPO_ROOT / "sql"
DATA_DIR: Final = REPO_ROOT / "data"
OUTPUT: Final = REPO_ROOT / "DATA_DICTIONARY.md"

# The first comment line of a model, which every one of them carries.
PURPOSE_RE: Final = re.compile(r"^--\s*(?:Gold|Silver|Dashboard|ML):\s*(.+?)\s*$", re.MULTILINE)


def purpose(path: Path) -> str:
    """The model's own one-line description, or a placeholder."""
    if not path.exists():
        return "_no model file_"
    found = PURPOSE_RE.search(path.read_text())
    return found.group(1) if found else "_no purpose line_"


def columns_of(parquet: Path) -> list[tuple[str, str]]:
    """Column names and types straight from the Parquet footer."""
    con = duckdb.connect()
    try:
        literal = str(parquet).replace("'", "''")
        rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{literal}')").fetchall()
        return [(row[0], row[1]) for row in rows]
    finally:
        con.close()


def bronze_specs() -> list[tuple[str, str, list[tuple[str, str, str]]]]:
    """(key, report, [(column, type, AEMO source)]) for every declared bronze table.

    Read from the ingesters' own ReportSpecs, so a new report documents itself
    the moment it is declared and cannot be forgotten here.
    """
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    out = []
    for module_name, report in (
        ("ingest_scada", "Dispatch_SCADA"),
        ("ingest_dispatchis", "DispatchIS_Reports"),
        ("ingest_p5min", "P5_Reports"),
        ("ingest_predispatch", "PredispatchIS_Reports"),
    ):
        try:
            module = __import__(module_name)
        except ImportError as exc:  # pragma: no cover - a missing ingester is a bug
            LOG.warning("skipping %s: %s", module_name, exc)
            continue
        spec = next(
            value for name, value in vars(module).items()
            if name.isupper() and hasattr(value, "tables")
        )
        for table in spec.tables:
            out.append((
                table.key,
                report,
                [(c.name, c.sql_type, c.source + (" (optional)" if c.optional else ""))
                 for c in table.columns],
            ))
    return out


def render() -> str:
    """Build the whole document."""
    lines: list[str] = [
        "# Data dictionary",
        "",
        "**Generated — do not edit.** Run `python scripts/data_dictionary.py` to refresh;",
        "`--check` fails if it is out of date. Purpose lines come from each model's own",
        "header comment, bronze columns from the `TableSpec`s the ingesters declare, and",
        "gold and silver columns from the Parquet on disk. Schema only: row counts and",
        "date spans change on every ingest and would make `--check` fail hourly.",
        "",
        "See [`CLAUDE.md`](CLAUDE.md) for the layer contract these tables obey:",
        "bronze is a faithful copy and immutable, silver is wind-only and immutable per",
        "day, gold is rewritten in full every run.",
        "",
    ]

    # --- bronze -----------------------------------------------------------
    lines += ["## Bronze", "",
              "Every DUID and region as AEMO published it, one Parquet per closed NEM hour.",
              "`source_file` and `ingested_at` are added to every table by the writer.", ""]
    for key, report, cols in bronze_specs():
        lines += [f"### `{key}`", "",
                  f"Parsed from `{report}`.", "",
                  "| Column | Type | AEMO source |", "|---|---|---|"]
        lines += [f"| `{n}` | {t} | `{s}` |" for n, t, s in cols]
        lines += [""]

    # --- silver and gold --------------------------------------------------
    for layer, blurb in (
        ("silver", "Wind only, typed and deduplicated, UTC and NEM local time. "
                   "One immutable file per trading date."),
        ("gold", "Aggregates and dashboard-ready tables, rewritten in full each run."),
    ):
        lines += [f"## {layer.capitalize()}", "", blurb, ""]
        models = sorted((SQL_DIR / layer).glob("*.sql"))
        for model in models:
            name = model.stem
            if layer == "silver":
                files = sorted((DATA_DIR / "silver" / name).glob("*.parquet"))
                parquet = files[-1] if files else None
            else:
                parquet = DATA_DIR / "gold" / f"{name}.parquet"
                parquet = parquet if parquet.exists() else None

            lines += [f"### `{name}`", "", purpose(model), ""]
            if parquet is None:
                lines += ["_Not built: its inputs are not present._", ""]
                continue
            lines += ["| Column | Type |", "|---|---|"]
            lines += [f"| `{n}` | {t} |" for n, t in columns_of(parquet)]
            lines += [""]

    # quality is written by check_quality.py, not by a sql/gold model
    quality = DATA_DIR / "gold" / "quality.parquet"
    if quality.exists():
        lines += ["### `quality`", "",
                  "One row per data-quality finding, plus a row for every check that found",
                  "nothing, so a reader can tell \"ran and passed\" from \"did not run\".",
                  "Written by `scripts/check_quality.py` from `sql/quality/`, not from a",
                  "`sql/gold/` model.", "", "| Column | Type |", "|---|---|"]
        lines += [f"| `{n}` | {t} |" for n, t in columns_of(quality)]
        lines += [""]

    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true",
                        help="exit non-zero if DATA_DICTIONARY.md is out of date")
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    rendered = render()
    if args.check:
        current = args.output.read_text() if args.output.exists() else ""
        if current != rendered:
            LOG.error("%s is out of date; run python scripts/data_dictionary.py",
                      args.output.name)
            return 1
        LOG.info("%s is up to date", args.output.name)
        return 0

    args.output.write_text(rendered)
    LOG.info("wrote %s (%d bytes)", args.output.name, args.output.stat().st_size)
    return 0


if __name__ == "__main__":
    sys.exit(main())
