"""Report what the committed data costs, and how long the budget lasts.

    python scripts/repo_size.py            # human-readable report
    python scripts/repo_size.py --quiet    # one summary line

CLAUDE.md calls repo size a hard budget but names no number, which makes it
impossible to act on. This gives it one, and converts the thing nobody can
intuit — bytes per day across seven sources at three grains — into the only
figure that drives a decision: **months of runway left**.

The arithmetic that matters is not total size, it is how each layer accumulates
in git history, which is append-only:

- **Write-once layers** (bronze, silver) cost their own size, once. A year of
  them is a year of bytes.
- **The rewritten layer** (gold) costs its size *every time it changes*. 98 KB
  of gold rewritten hourly is 860 MB a year; the same 98 KB rewritten once a
  day is 36 MB. This is why gold determinism was worth fixing, and why the
  figure below is reported per rewrite rather than as a single number.

Measured from the working tree rather than from `.git`: the transform job
checks out shallow, so `.git` there reflects fetch depth and not history.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

LOG: Final = logging.getLogger("repo_size")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
DATA_DIR: Final = REPO_ROOT / "data"

DATE_RE: Final = re.compile(r"date=(\d{4}-\d{2}-\d{2})")

# GitHub recommends staying under 1 GB per repository and starts warning well
# before its hard limits. A portfolio repo is also something people clone, so
# the practical ceiling is lower than the technical one. This is the number
# CLAUDE.md's "hard budget" was missing.
BUDGET_BYTES: Final = 1024 ** 3

# Below this, say so loudly: a grain change costs more the longer it waits,
# because there is more history to migrate.
RUNWAY_WARN_MONTHS: Final = 12

# Gold is rewritten in full, so its history cost is size x rewrites. Silver
# advances once a day, which is when every gold table changes; the rest of the
# time only the few that read fresh bronze do.
GOLD_FULL_REWRITES_PER_YEAR: Final = 365


@dataclass(frozen=True)
class Layer:
    """One directory of committed data."""

    name: str
    files: int
    bytes_: int
    days: int

    @property
    def bytes_per_day(self) -> float:
        return self.bytes_ / self.days if self.days else 0.0

    @property
    def per_year(self) -> float:
        return self.bytes_per_day * 365


def human(n: float) -> str:
    for unit, size in (("GB", 1024 ** 3), ("MB", 1024 ** 2), ("KB", 1024)):
        if abs(n) >= size:
            return f"{n / size:.1f} {unit}"
    return f"{n:.0f} B"


def measure(path: Path) -> tuple[int, int, set[str]]:
    """(files, bytes, dates seen) under a directory."""
    files = total = 0
    dates: set[str] = set()
    for item in path.rglob("*.parquet"):
        files += 1
        total += item.stat().st_size
        found = DATE_RE.search(str(item))
        if found:
            dates.add(found.group(1))
    return files, total, dates


def survey(data_dir: Path = DATA_DIR) -> tuple[list[Layer], Layer | None]:
    """Measure each write-once source, and gold separately. Gold is not a rate."""
    layers: list[Layer] = []
    bronze = data_dir / "bronze"
    if bronze.exists():
        for source in sorted(p for p in bronze.iterdir() if p.is_dir()):
            files, total, dates = measure(source)
            if files:
                layers.append(Layer(f"bronze/{source.name}", files, total, len(dates)))

    silver = data_dir / "silver"
    if silver.exists():
        for table in sorted(p for p in silver.iterdir() if p.is_dir()):
            files, total, dates = measure(table)
            if files:
                layers.append(Layer(f"silver/{table.name}", files, total, len(dates)))

    gold_files, gold_bytes, _ = measure(data_dir / "gold")
    gold = Layer("gold", gold_files, gold_bytes, 0) if gold_files else None
    return layers, gold


def report(layers: list[Layer], gold: Layer | None) -> tuple[float, float]:
    """Print the table. Returns (annual bytes, months of runway)."""
    print(f"\n  {'layer':24} {'files':>6} {'on disk':>10} {'per day':>10} {'per year':>10}")
    print(f"  {'-' * 24} {'-' * 6} {'-' * 10} {'-' * 10} {'-' * 10}")
    write_once = 0.0
    for layer in sorted(layers, key=lambda x: -x.per_year):
        write_once += layer.per_year
        print(f"  {layer.name:24} {layer.files:>6} {human(layer.bytes_):>10} "
              f"{human(layer.bytes_per_day):>10} {human(layer.per_year):>10}")

    gold_year = 0.0
    if gold:
        gold_year = gold.bytes_ * GOLD_FULL_REWRITES_PER_YEAR
        print(f"  {'-' * 24} {'-' * 6} {'-' * 10} {'-' * 10} {'-' * 10}")
        print(f"  {'gold (per rewrite)':24} {gold.files:>6} {human(gold.bytes_):>10} "
              f"{'n/a':>10} {human(gold_year):>10}")
        print(f"  {'':24} {'':>6} {'':>10} {'':>10} "
              f"at {GOLD_FULL_REWRITES_PER_YEAR}/yr")

    annual = write_once + gold_year
    on_disk = sum(x.bytes_ for x in layers) + (gold.bytes_ if gold else 0)
    print(f"\n  on disk now        {human(on_disk)}")
    print(f"  annual growth      {human(annual)}  "
          f"(write-once {human(write_once)} + gold {human(gold_year)})")

    remaining = BUDGET_BYTES - on_disk
    months = (remaining / annual * 12) if annual > 0 else float("inf")
    print(f"  budget             {human(BUDGET_BYTES)}")
    print(f"  runway             {months:.1f} months at this rate")
    return annual, months


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--quiet", action="store_true", help="one summary line only")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    layers, gold = survey(args.data_dir)
    if not layers and gold is None:
        LOG.warning("no committed data found under %s", args.data_dir)
        return 0

    if args.quiet:
        on_disk = sum(x.bytes_ for x in layers) + (gold.bytes_ if gold else 0)
        annual = sum(x.per_year for x in layers) + (
            gold.bytes_ * GOLD_FULL_REWRITES_PER_YEAR if gold else 0)
        months = (BUDGET_BYTES - on_disk) / annual * 12 if annual else float("inf")
        print(f"data {human(on_disk)} on disk, ~{human(annual)}/yr, "
              f"{months:.1f} months of a {human(BUDGET_BYTES)} budget")
        return 0

    _, months = report(layers, gold)
    if months < RUNWAY_WARN_MONTHS:
        print(f"\n  Under {RUNWAY_WARN_MONTHS} months of runway. See PLAN.md section 6 for\n"
              "  the pruning options, cheapest first. A grain change costs more the\n"
              "  longer it waits, because there is more history to migrate.\n")
    else:
        print()
    # Reporting, never gating: a size trend is a decision for a person, and a
    # red hourly run over a slow-moving number would be ignored within a week.
    return 0


if __name__ == "__main__":
    sys.exit(main())
