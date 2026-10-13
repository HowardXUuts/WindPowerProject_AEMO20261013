"""Tests for the dashboard JSON export.

The page is static and cannot query Parquet, so these JSON files are its only
contract with the pipeline. A renamed column here is a blank panel there, with
nothing in CI to catch it unless these assertions do.
"""

import json
from datetime import date, datetime

import duckdb
import pytest
from export_dashboard import export
from nemweb import NEM_TZ

DAY = date(2026, 10, 12)


def _write(con, sql, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({sql}) TO '{destination}' (FORMAT PARQUET)")


@pytest.fixture
def gold(tmp_path):
    """A minimal but complete gold layer."""
    gold_dir = tmp_path / "gold"
    start = datetime(2026, 10, 12, 0, 0, tzinfo=NEM_TZ)
    con = duckdb.connect()
    try:
        _write(con, f"""
            SELECT DATE '{DAY}' AS trading_date, 'WFA1' AS duid, 'Alpha' AS station_name,
                   'SA1' AS region, 100.0 AS capacity_mw, 1200.0 AS energy_mwh,
                   0.5 AS capacity_factor, 80.0 AS peak_mw, 20.0 AS min_mw,
                   288 AS intervals, true AS is_complete
        """, gold_dir / "farm_daily.parquet")
        _write(con, f"""
            SELECT DATE '{DAY}' AS trading_date, 'WFA1' AS duid, 'Alpha' AS station_name,
                   'SA1' AS region, 100.0 AS capacity_mw, 1200.0 AS energy_mwh,
                   93600.0 AS revenue_aud, 78.0 AS realised_price_aud_mwh,
                   240.0 AS negative_price_mwh, -2400.0 AS negative_price_revenue_aud,
                   288 AS intervals
        """, gold_dir / "farm_revenue_daily.parquet")
        _write(con, f"""
            SELECT DATE '{DAY}' AS trading_date, 'SA1' AS region, 78.0 AS capture_price_aud_mwh,
                   45.0 AS twap_aud_mwh, 1.7333 AS capture_ratio, 1200.0 AS wind_energy_mwh,
                   288 AS intervals, 288 AS price_intervals
        """, gold_dir / "capture_price.parquet")
        _write(con, f"""
            SELECT DATE '{DAY}' AS trading_date, 'SA1' AS region,
                   TIMESTAMP '2026-10-12 00:00:00' AS hour_nem,
                   TIMESTAMPTZ '{start}' AS hour_start_utc, 80.0 AS avg_mw, 80.0 AS peak_mw,
                   80.0 AS min_mw, 80.0 AS energy_mwh, 12 AS intervals
        """, gold_dir / "region_hourly.parquet")
        _write(con, f"""
            SELECT TIMESTAMPTZ '{start}' AS interval_start_utc,
                   TIMESTAMP '2026-10-12 00:00:00' AS interval_start_nem, 'WFA1' AS duid,
                   'Alpha' AS station_name, 'SA1' AS region, 100.0 AS capacity_mw,
                   20.0 AS mw, 0.2 AS pct_of_capacity, 'bronze' AS source_layer
        """, gold_dir / "latest_snapshot.parquet")
        _write(con, f"""
            SELECT TIMESTAMPTZ '{start}' AS checked_at, 'price_out_of_band' AS check_name,
                   'soft' AS severity, 'warn' AS status, DATE '{DAY}' AS trading_date,
                   'SA1' AS subject, 6000.0 AS observed, 'out of band' AS message
        """, gold_dir / "quality.parquet")
    finally:
        con.close()
    return gold_dir


def run(gold, tmp_path, **kwargs):
    out = tmp_path / "out"
    export(gold_dir=gold, out_dir=out, **kwargs)
    return out


def load(out, name):
    return json.loads((out / f"{name}.json").read_text())


def test_every_slice_is_written(gold, tmp_path):
    out = run(gold, tmp_path)
    for name in ("region_today", "cf_trend", "farm_cf_week", "revenue_region",
                 "capture", "quality", "freshness", "meta"):
        assert (out / f"{name}.json").exists(), name


def test_freshness_carries_what_the_tiles_need(gold, tmp_path):
    row = load(run(gold, tmp_path), "freshness")[0]
    assert row["latest_interval_nem"] == "2026-10-12 00:00"
    assert row["fleet_mw"] == 20.0
    assert row["fleet_capacity_mw"] == 100.0
    assert row["source_layer"] == "bronze"


def test_cf_trend_is_capacity_weighted(gold, tmp_path):
    row = load(run(gold, tmp_path), "cf_trend")[0]
    assert row["capacity_factor"] == pytest.approx(0.5)
    assert row["region"] == "SA1"
    assert row["is_complete"] is True


def test_revenue_slice_totals(gold, tmp_path):
    row = load(run(gold, tmp_path), "revenue_region")[0]
    assert row["revenue_aud"] == 93600.0
    assert row["negative_mwh"] == 240.0
    assert row["negative_pct"] == pytest.approx(20.0)  # 240 of 1200 MWh


def test_farm_week_has_both_bands(gold, tmp_path):
    rows = load(run(gold, tmp_path), "farm_cf_week")
    assert {r["band"] for r in rows} == {"top", "bottom"}
    assert rows[0]["days_seen"] == 1


def test_dates_are_strings_not_objects(gold, tmp_path):
    # JSON has no date type; the page formats them exactly as given.
    out = run(gold, tmp_path)
    assert isinstance(load(out, "cf_trend")[0]["trading_date"], str)
    assert isinstance(load(out, "region_today")[0]["trading_date"], str)


def test_slices_stay_small_enough_for_a_phone(gold, tmp_path):
    out = run(gold, tmp_path)
    total = sum(f.stat().st_size for f in out.glob("*.json"))
    assert total < 500_000, f"{total} bytes is too much to fetch on mobile"


def test_missing_gold_table_skips_its_slice(gold, tmp_path):
    (gold / "capture_price.parquet").unlink()
    out = run(gold, tmp_path)
    assert not (out / "capture.json").exists()
    assert (out / "revenue_region.json").exists()


def test_dry_run_writes_nothing(gold, tmp_path):
    out = run(gold, tmp_path, dry_run=True)
    assert not out.exists() or not list(out.glob("*.json"))
