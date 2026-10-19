"""Rolling-origin folds with purge and embargo.

Random k-fold on a time series is meaningless, and an ordinary chronological
split is still not enough here, because the targets overlap: the row at 13:00
has a target covering 13:05–15:00, so a naive split that puts 13:00 in train
and 14:00 in test has already shown the training set what the test period did.

Two corrections, both from PLAN.md 4.5:

**Purge.** Drop training rows whose target window extends past the cutoff. With
a 120-minute horizon that is the last 24 training intervals — they know what
happened on the other side of the line.

**Embargo.** Skip the first `horizon` minutes of the test period, so no test
row's *features* overlap a training row's target. Purge protects the training
set from the future; embargo protects the test set from the past. Doing only
one is the common half-measure, and it leaks in the direction nobody checks.

The window expands rather than slides, to start. The NEM changes structurally —
new batteries, new transmission — so a sliding window may well prove better,
but that is an experiment to run, not an assumption to bake in.

Nothing here touches the data. Folds are boundaries in time, so they are
computed from the span and asserted against, which keeps them testable without
a dataset.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

LOG: Final = logging.getLogger("walkforward")

INTERVAL: Final = timedelta(minutes=5)

# PLAN 4.5: origins advance a fortnight at a time, and each fold tests on the
# fortnight that follows.
DEFAULT_TEST_DAYS: Final = 14
DEFAULT_STEP_DAYS: Final = 14

# The longest target window in PLAN 4.2. Purge and embargo are both sized from
# it, because it is the furthest any single row can see.
DEFAULT_HORIZON: Final = timedelta(minutes=120)


class NotEnoughHistory(Exception):
    """The span is too short to cut even one honest fold."""


@dataclass(frozen=True)
class Fold:
    """One rolling origin: what may be trained on, and what it is tested on."""

    index: int
    train_start: datetime
    train_end: datetime  # exclusive; already purged
    test_start: datetime  # already embargoed
    test_end: datetime  # exclusive
    cutoff: datetime  # the origin itself, before purge and embargo

    @property
    def train_days(self) -> float:
        return (self.train_end - self.train_start).total_seconds() / 86400

    @property
    def test_days(self) -> float:
        return (self.test_end - self.test_start).total_seconds() / 86400

    def covers_train(self, moment: datetime) -> bool:
        return self.train_start <= moment < self.train_end

    def covers_test(self, moment: datetime) -> bool:
        return self.test_start <= moment < self.test_end


def folds(
    first: datetime,
    last: datetime,
    horizon: timedelta = DEFAULT_HORIZON,
    test_days: int = DEFAULT_TEST_DAYS,
    step_days: int = DEFAULT_STEP_DAYS,
    min_train_days: int = 28,
) -> list[Fold]:
    """Cut rolling-origin folds across ``first``..``last``.

    The first cutoff sits ``min_train_days`` after the start, so no fold trains
    on less history than that. Raises rather than returning an empty list when
    the span cannot support one: a silent zero-fold run is the shape of a
    pipeline that reports nothing and looks successful.
    """
    if last <= first:
        raise NotEnoughHistory(f"last ({last}) is not after first ({first})")

    test = timedelta(days=test_days)
    step = timedelta(days=step_days)
    cutoff = first + timedelta(days=min_train_days)

    out: list[Fold] = []
    index = 0
    while cutoff + horizon + test <= last:
        # Purge: a training row at time x has a target reaching x + horizon, so
        # the last admissible training row starts one interval before
        # cutoff - horizon.
        train_end = cutoff - horizon
        # Embargo: the test period opens a horizon after the cutoff, so no test
        # row's features reach back across it into a training target.
        test_start = cutoff + horizon
        test_end = test_start + test

        if train_end > first:
            out.append(Fold(index, first, train_end, test_start, test_end, cutoff))
            index += 1
        cutoff += step

    if not out:
        span = (last - first).total_seconds() / 86400
        needed = min_train_days + test_days + 2 * horizon.total_seconds() / 86400
        raise NotEnoughHistory(
            f"{span:.1f} days of history cannot support a fold: "
            f"{min_train_days}d train + {test_days}d test + purge and embargo "
            f"needs about {needed:.1f} days."
        )
    return out


def purged(
    rows: list[datetime], fold: Fold, horizon: timedelta = DEFAULT_HORIZON
) -> list[datetime]:
    """The training rows of ``fold``, with the purge applied.

    Kept as a function over timestamps rather than a dataframe operation so the
    rule can be tested on its own, which is the only way anyone will believe it.
    """
    return [r for r in rows if fold.train_start <= r and r + horizon <= fold.cutoff]


def embargoed(rows: list[datetime], fold: Fold) -> list[datetime]:
    """The test rows of ``fold``, with the embargo applied."""
    return [r for r in rows if fold.test_start <= r < fold.test_end]


def describe(fold: Fold) -> str:
    """One line a human can check a fold against."""
    return (
        f"fold {fold.index}: train {fold.train_start:%Y-%m-%d} .. "
        f"{fold.train_end:%Y-%m-%d %H:%M} ({fold.train_days:.1f}d) | "
        f"cutoff {fold.cutoff:%Y-%m-%d} | "
        f"test {fold.test_start:%Y-%m-%d %H:%M} .. {fold.test_end:%Y-%m-%d} "
        f"({fold.test_days:.1f}d)"
    )
