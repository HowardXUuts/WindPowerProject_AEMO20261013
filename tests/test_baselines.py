"""Tests for the baseline scorer and the split readiness gate.

The baselines are the only part of the forecasting phase that can be measured
today, so they are also the only part whose tests can check real behaviour
rather than a refusal path. Two things are worth pinning: that every predictor
is scored on the same rows, and that P5MIN is joined on issue time.
"""

from dataclasses import replace

import pytest
from baselines import PREDICTORS, _f
from build_features import MIN_DAYS, MIN_SPIKE_EPISODES, Readiness
from metrics import regression


def ready(**kwargs) -> Readiness:
    base = Readiness(region="X", days=MIN_DAYS, rows=1000,
                     spike_intervals=100, spike_episodes=MIN_SPIKE_EPISODES, reasons=())
    return replace(base, **kwargs)


# --- the split gate -------------------------------------------------------


def test_the_mean_model_does_not_wait_for_spikes():
    """The flaw this split fixes.

    SA1 and VIC1 have had zero spike episodes in nine days. Gating their
    mean-price model on spikes they may never see would park two of the three
    large wind regions indefinitely, for a model that does not use spikes.
    """
    r = ready(spike_episodes=0, spike_intervals=0)
    assert r.ready_mean is True
    assert r.ready_spike is False


def test_the_spike_model_needs_both():
    assert ready(days=MIN_DAYS - 1).ready_spike is False
    assert ready(spike_episodes=MIN_SPIKE_EPISODES - 1).ready_spike is False
    assert ready().ready_spike is True


def test_neither_is_ready_without_history():
    r = ready(days=1, spike_episodes=0)
    assert r.ready_mean is False and r.ready_spike is False


def test_ready_remains_the_strictest_reading():
    """Existing callers asking `ready` should keep getting the strict answer."""
    assert ready(spike_episodes=0).ready is False
    assert ready().ready is True


def test_mean_reasons_mention_only_days():
    r = Readiness("X", 9, 100, 0, 0,
                  ("9 days of history, need 60", "0 spike episodes, need 30"))
    assert r.mean_reasons == ("9 days of history, need 60",)
    assert len(r.spike_reasons) == 2


# --- the scorer -----------------------------------------------------------


def test_p5min_is_absent_from_the_120_minute_horizon():
    """AEMO's run reaches 55 minutes, so there is no comparator at 120.

    Emitting one would mean averaging eleven of twenty-four intervals and
    calling it a forecast of the window.
    """
    assert "p5min" in PREDICTORS[60]
    assert "p5min" not in PREDICTORS[120]


def test_every_horizon_scores_the_same_three_computable_baselines():
    for horizon in (60, 120):
        assert {"persistence", "yesterday", "climatology"} <= set(PREDICTORS[horizon])


def test_decimal_values_are_coerced_for_the_stdlib_metrics():
    """DuckDB returns DECIMAL for some aggregates; metrics.py is plain floats."""
    from decimal import Decimal
    assert _f(Decimal("1.5")) == 1.5
    assert isinstance(_f(Decimal("1.5")), float)
    assert _f(None) is None


def test_median_absolute_error_separates_a_tail_from_a_bias():
    """Why MedAE was added, with the shape that motivated it.

    Nine of 1,854 Queensland rows, where P5MIN forecast a market-cap price that
    never arrived, carried 112 of its 123 MAE. The mean calls that forecast
    useless; the median shows the other 1,845 rows were fine.
    """
    predicted = [10.0] * 99 + [10_000.0]
    actual = [10.0] * 100
    reg = regression(predicted, actual)
    assert reg.mae == pytest.approx(99.9, abs=0.1)   # the tail dominates
    assert reg.medae == pytest.approx(0.0)           # the typical row is exact
    assert reg.n == 100
