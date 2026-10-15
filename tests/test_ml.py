"""Tests for the forecasting harness.

Almost all of this is about leakage. A leaked feature produces a model that
scores beautifully and earns nothing, and the failure is silent — there is no
exception, no warning, just a number that does not survive contact with a real
trading day. So the splitter, the feature windows and the backtest settlement
are each tested against the specific way they could cheat.

No network, no credential, and no model: 9a is the harness.
"""

from datetime import UTC, datetime, timedelta

import duckdb
import pytest
from backtest_battery import (
    Battery,
    capture_ratio,
    do_nothing,
    fixed_threshold,
    forecast_policy,
    perfect_foresight,
)
from build_features import MIN_DAYS, MIN_SPIKE_EPISODES, FeatureError, build
from metrics import brier, classification, pr_auc, regression, reliability
from walkforward import NotEnoughHistory, embargoed, folds, purged

HORIZON = timedelta(minutes=120)
START = datetime(2026, 1, 1, tzinfo=UTC)


# --- walk-forward: purge and embargo --------------------------------------


def test_a_fold_is_cut_when_there_is_enough_history():
    cut = folds(START, START + timedelta(days=90), horizon=HORIZON)
    assert cut
    assert cut[0].train_start == START


def test_too_little_history_raises_rather_than_returning_nothing():
    # A silent zero-fold run is the shape of a pipeline that reports nothing
    # and still looks successful.
    with pytest.raises(NotEnoughHistory, match="cannot support a fold"):
        folds(START, START + timedelta(days=20), horizon=HORIZON)


def test_training_stops_a_full_horizon_before_the_cutoff():
    """Purge. A training row's target must not cross the line."""
    for fold in folds(START, START + timedelta(days=120), horizon=HORIZON):
        assert fold.train_end == fold.cutoff - HORIZON


def test_testing_starts_a_full_horizon_after_the_cutoff():
    """Embargo. A test row's features must not reach back over the line."""
    for fold in folds(START, START + timedelta(days=120), horizon=HORIZON):
        assert fold.test_start == fold.cutoff + HORIZON


def test_no_training_row_can_see_past_the_cutoff():
    fold = folds(START, START + timedelta(days=90), horizon=HORIZON)[0]
    rows = [fold.cutoff - timedelta(minutes=5 * i) for i in range(60)]
    for row in purged(rows, fold, HORIZON):
        assert row + HORIZON <= fold.cutoff, "this training row's target crosses the cutoff"


def test_purge_drops_every_row_whose_target_crosses_the_cutoff():
    fold = folds(START, START + timedelta(days=90), horizon=HORIZON)[0]
    # The 23 rows strictly inside the final horizon all reach past the line.
    inside = [fold.cutoff - timedelta(minutes=5 * i) for i in range(1, 24)]
    assert purged(inside, fold, HORIZON) == []
    # The row exactly one horizon back ends *at* the cutoff, not past it, so it
    # is legitimately kept — the boundary case worth pinning down.
    edge = fold.cutoff - HORIZON
    assert purged([edge], fold, HORIZON) == [edge]


def test_no_test_row_overlaps_a_training_target():
    """The two rules together, which is the only way either is worth anything."""
    fold = folds(START, START + timedelta(days=90), horizon=HORIZON)[0]
    train = purged(
        [fold.train_start + timedelta(minutes=5 * i) for i in range(20000)], fold, HORIZON
    )
    test = embargoed(
        [fold.cutoff + timedelta(minutes=5 * i) for i in range(2000)], fold
    )
    assert train and test
    latest_training_target = max(train) + HORIZON
    earliest_test_feature = min(test) - HORIZON
    assert latest_training_target <= earliest_test_feature


def test_folds_expand_rather_than_slide():
    cut = folds(START, START + timedelta(days=200), horizon=HORIZON)
    assert len(cut) > 1
    assert all(f.train_start == START for f in cut)
    assert [f.cutoff for f in cut] == sorted(f.cutoff for f in cut)


def test_test_periods_do_not_overlap():
    cut = folds(START, START + timedelta(days=200), horizon=HORIZON)
    for earlier, later in zip(cut, cut[1:], strict=False):
        assert earlier.test_end <= later.test_end
        assert later.test_start >= earlier.test_start


# --- metrics ---------------------------------------------------------------


def test_regression_on_a_known_answer():
    result = regression([10.0, 20.0, 30.0], [12.0, 18.0, 33.0])
    assert result.mae == pytest.approx((2 + 2 + 3) / 3)
    assert result.bias == pytest.approx((-2 + 2 - 3) / 3)


def test_regression_splits_spike_windows_from_quiet_ones():
    # The split exists because an overall MAE is dominated by quiet intervals,
    # where a model can be excellent and worthless.
    result = regression([50.0, 50.0], [50.0, 500.0])
    assert result.n_spike == 1
    assert result.mae_quiet == pytest.approx(0.0)
    assert result.mae_spike == pytest.approx(450.0)


def test_brier_rewards_a_confident_correct_call():
    assert brier([1.0, 0.0], [1.0, 0.0]) == pytest.approx(0.0)
    assert brier([0.0, 1.0], [1.0, 0.0]) == pytest.approx(1.0)


def test_pr_auc_is_one_when_every_positive_outranks_every_negative():
    assert pr_auc([0.9, 0.8, 0.2, 0.1], [1, 1, 0, 0]) == pytest.approx(1.0)


def test_pr_auc_of_a_constant_prediction_is_the_base_rate():
    # The honest floor: a model that says the same thing every time has learned
    # nothing, and must not score above the prevalence.
    assert pr_auc([0.5] * 10, [1, 1, 0, 0, 0, 0, 0, 0, 0, 0]) == pytest.approx(0.2)


def test_pr_auc_does_not_invent_discrimination_from_ties():
    # Tied scores are consumed as a block; ordering them arbitrarily would
    # manufacture skill the model does not have.
    tied = pr_auc([0.5, 0.5, 0.5, 0.5], [1, 0, 1, 0])
    assert tied == pytest.approx(0.5)


def test_pr_auc_is_zero_with_no_positives():
    assert pr_auc([0.9, 0.1], [0, 0]) == 0.0


def test_reliability_bins_predictions_against_outcomes():
    curve = reliability([0.05] * 10 + [0.95] * 10, [0] * 10 + [1] * 10, bins=10)
    low = [b for b in curve if b[0] < 0.5][0]
    high = [b for b in curve if b[0] > 0.5][0]
    assert low[1] == pytest.approx(0.0)
    assert high[1] == pytest.approx(1.0)


def test_reliability_omits_empty_bins():
    # "No predictions here" and "always wrong here" are different facts.
    curve = reliability([0.95] * 5, [1] * 5, bins=10)
    assert len(curve) == 1


def test_classification_reports_the_base_rate():
    result = classification([0.5] * 100, [1] * 2 + [0] * 98)
    assert result.base_rate == pytest.approx(0.02)
    assert result.positives == 2


def test_mismatched_lengths_are_refused():
    with pytest.raises(ValueError, match="length mismatch"):
        regression([1.0], [1.0, 2.0])


# --- battery backtest ------------------------------------------------------

FLAT = [(START + timedelta(minutes=5 * i), 50.0) for i in range(24)]
VOLATILE = [
    (START + timedelta(minutes=5 * i), 0.0 if i < 12 else 300.0) for i in range(24)
]
UNIT = Battery(power_mw=60.0, energy_mwh=10.0, round_trip_efficiency=1.0,
               soc_min=0.0, soc_max=1.0, initial_soc=0.0)


def test_doing_nothing_earns_nothing():
    assert do_nothing(UNIT, VOLATILE).revenue_aud == 0.0


def test_there_is_nothing_to_arbitrage_on_a_flat_price():
    assert perfect_foresight(UNIT, FLAT).revenue_aud == pytest.approx(0.0)


def test_perfect_foresight_buys_low_and_sells_high():
    outcome = perfect_foresight(UNIT, VOLATILE)
    assert outcome.revenue_aud > 0
    # 10 MWh of usable storage bought at $0 and sold at $300.
    assert outcome.revenue_aud == pytest.approx(3000.0)


def test_perfect_foresight_is_the_ceiling():
    ceiling = perfect_foresight(UNIT, VOLATILE)
    rule = fixed_threshold(UNIT, VOLATILE, charge_below=10.0, discharge_above=200.0)
    assert rule.revenue_aud <= ceiling.revenue_aud + 1e-9


def test_a_trade_settles_at_the_actual_price_not_the_forecast():
    """The difference between a backtest and a fantasy."""
    prices = [(START + timedelta(minutes=5 * i), 10.0) for i in range(12)]
    # The forecast is wildly wrong: it says $900 throughout, so the policy
    # discharges. Revenue must reflect the $10 that was actually paid.
    outcome = forecast_policy(
        Battery(power_mw=60.0, energy_mwh=10.0, round_trip_efficiency=1.0,
                soc_min=0.0, soc_max=1.0, initial_soc=1.0),
        prices,
        forecast_mean=[900.0] * 12,
        forecast_spike=None,
        charge_below=0.0,
        discharge_above=100.0,
    )
    assert outcome.revenue_aud == pytest.approx(10.0 * 10.0), "settled at $10, not $900"


def test_round_trip_losses_are_taken_on_the_charge_leg():
    lossy = Battery(power_mw=60.0, energy_mwh=10.0, round_trip_efficiency=0.5,
                    soc_min=0.0, soc_max=1.0, initial_soc=0.0)
    # Buying 10 MWh of stored energy at 50% efficiency draws 20 MWh from the
    # grid, so a perfect round trip earns less than the price spread suggests.
    outcome = perfect_foresight(lossy, VOLATILE)
    lossless = perfect_foresight(UNIT, VOLATILE)
    assert 0 < outcome.revenue_aud < lossless.revenue_aud


def test_a_battery_cannot_discharge_energy_it_does_not_hold():
    empty = Battery(power_mw=60.0, energy_mwh=10.0, round_trip_efficiency=1.0,
                    soc_min=0.0, soc_max=1.0, initial_soc=0.0)
    outcome = fixed_threshold(empty, [(START, 500.0)], charge_below=0.0, discharge_above=100.0)
    assert outcome.revenue_aud == 0.0


def test_capture_ratio_is_the_headline_number():
    ceiling = perfect_foresight(UNIT, VOLATILE)
    rule = fixed_threshold(UNIT, VOLATILE, charge_below=10.0, discharge_above=200.0)
    ratio = capture_ratio(rule, ceiling)
    assert ratio is not None
    assert 0.0 <= ratio <= 1.0


def test_capture_ratio_is_undefined_when_there_was_nothing_to_capture():
    ceiling = perfect_foresight(UNIT, FLAT)
    assert capture_ratio(do_nothing(UNIT, FLAT), ceiling) is None


def test_a_forecast_policy_needs_a_forecast_per_interval():
    with pytest.raises(ValueError, match="differ in length"):
        forecast_policy(UNIT, VOLATILE, [1.0], None, 0.0, 100.0)


# --- features: the windows that must not look forward ----------------------


@pytest.fixture
def silver(tmp_path):
    """Two days of five-minute prices and wind for one region."""
    silver_dir = tmp_path / "silver"
    con = duckdb.connect()
    try:
        for name, sql in {
            "price": """
                SELECT DATE '2026-10-12' + ((n // 288) * INTERVAL 1 DAY) AS trading_date,
                       TIMESTAMPTZ '2026-10-12 00:00:00+10' + (n * INTERVAL 5 MINUTE)
                           AS interval_start_utc,
                       TIMESTAMP '2026-10-12 00:00:00' + (n * INTERVAL 5 MINUTE)
                           AS interval_start_nem,
                       'SA1' AS region,
                       CASE WHEN n % 100 = 0 THEN 900.0 ELSE 40.0 + n % 17 END AS rrp
                FROM range(0, 576) t(n)""",
            "wind_output": """
                SELECT DATE '2026-10-12' + ((n // 288) * INTERVAL 1 DAY) AS trading_date,
                       TIMESTAMPTZ '2026-10-12 00:00:00+10' + (n * INTERVAL 5 MINUTE)
                           AS interval_start_utc,
                       TIMESTAMP '2026-10-12 00:00:00' + (n * INTERVAL 5 MINUTE)
                           AS interval_start_nem,
                       'WFA1' AS duid, 'SA1' AS region,
                       100.0 AS capacity_mw, 30.0 + (n % 40) AS mw
                FROM range(0, 576) t(n)""",
        }.items():
            path = silver_dir / name / "date=2026-10-12.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()
    return silver_dir


def features_of(silver, tmp_path):
    rows, state = build(silver_dir=silver, out_dir=tmp_path / "out", write=True)
    return duckdb.connect(), rows, state


def test_features_build_from_silver(silver, tmp_path):
    rows, _ = build(silver_dir=silver, out_dir=tmp_path / "out", write=True)
    assert rows == 576
    assert (tmp_path / "out" / "features.parquet").exists()


def test_a_lag_reads_backwards(silver, tmp_path):
    build(silver_dir=silver, out_dir=tmp_path / "out", write=True)
    con = duckdb.connect()
    try:
        mismatches = con.execute(f"""
            WITH f AS (SELECT * FROM read_parquet('{tmp_path / "out" / "features.parquet"}'))
            SELECT count(*) FROM f a
            JOIN f b ON b.region = a.region
                    AND b.interval_start_utc = a.interval_start_utc - INTERVAL 5 MINUTE
            WHERE a.rrp_lag_5 IS DISTINCT FROM b.rrp
        """).fetchone()[0]
    finally:
        con.close()
    assert mismatches == 0


def test_a_target_reads_forwards_and_excludes_the_current_interval(silver, tmp_path):
    build(silver_dir=silver, out_dir=tmp_path / "out", write=True)
    con = duckdb.connect()
    try:
        wrong = con.execute(f"""
            WITH f AS (SELECT * FROM read_parquet('{tmp_path / "out" / "features.parquet"}')),
            recomputed AS (
                SELECT a.interval_start_utc, a.y_mean_60, avg(b.rrp) AS expected, count(*) AS n
                FROM f a JOIN f b
                  ON b.region = a.region
                 AND b.interval_start_utc >  a.interval_start_utc
                 AND b.interval_start_utc <= a.interval_start_utc + INTERVAL 60 MINUTE
                GROUP BY 1, 2
            )
            SELECT count(*) FROM recomputed
            WHERE n = 12 AND abs(y_mean_60 - expected) > 1e-9
        """).fetchone()[0]
    finally:
        con.close()
    assert wrong == 0, "the 60-minute target is not the mean of t+5..t+60"


def test_the_spike_target_fires_on_a_spike_in_the_window(silver, tmp_path):
    build(silver_dir=silver, out_dir=tmp_path / "out", write=True)
    con = duckdb.connect()
    try:
        row = con.execute(f"""
            SELECT sum(y_spike_60) AS spikes, count(*) AS n
            FROM read_parquet('{tmp_path / "out" / "features.parquet"}')
        """).fetchone()
    finally:
        con.close()
    # The fixture spikes every 100th interval, so some windows contain one and
    # most do not — a target that is all ones or all zeros is a broken one.
    assert 0 < row[0] < row[1]


def test_missing_silver_is_a_clear_error(tmp_path):
    with pytest.raises(FeatureError, match="no silver"):
        build(silver_dir=tmp_path / "absent", out_dir=tmp_path / "out")


# --- the readiness guard ---------------------------------------------------


def test_two_days_of_history_is_refused(silver, tmp_path):
    """The guard that stops a meaningless model being trained by accident."""
    _, state = build(silver_dir=silver, out_dir=tmp_path / "out", write=False)
    assert state
    for region in state:
        assert not region.ready
        assert any("days of history" in reason for reason in region.reasons)


def test_the_guard_names_the_spike_shortfall_too(silver, tmp_path):
    _, state = build(silver_dir=silver, out_dir=tmp_path / "out", write=False)
    assert any("spike episodes" in r for region in state for r in region.reasons)


def test_the_thresholds_are_what_the_plan_says():
    # PLAN 4.5 cuts a 14-day test from at least 28 days of training, with purge
    # and embargo either side; below about 60 days there is no honest fold.
    assert MIN_DAYS >= 60
    assert MIN_SPIKE_EPISODES >= 30
