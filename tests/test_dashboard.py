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


@pytest.fixture
def silver(tmp_path):
    """Two farms over one complete day, with a price series to settle against.

    WFA1 generates flat through the night and WFB1 only in the evening peak, so
    a test can tell a capture-price bug from an averaging bug: both have the
    same energy, and only one of them earns anything for it.
    """
    silver_dir = tmp_path / "silver"
    con = duckdb.connect()
    try:
        _write(con, f"""
            WITH i AS (SELECT range AS n FROM range(0, 288))
            SELECT DATE '{DAY}' AS trading_date,
                   TIMESTAMPTZ '2026-10-12 00:00:00+10'
                       + (n * INTERVAL 5 MINUTE) AS interval_start_utc,
                   TIMESTAMP '2026-10-12 00:00:00'
                       + (n * INTERVAL 5 MINUTE) AS interval_start_nem,
                   duid, station_name, 'SA1' AS region, 100.0 AS capacity_mw,
                   CASE WHEN duid = 'WFA1' THEN 50.0
                        WHEN n BETWEEN 204 AND 227 THEN 600.0 ELSE 0.0 END AS mw,
                   false AS is_negative, false AS is_above_capacity,
                   'f.zip' AS source_file, 288 AS intervals_present, true AS is_complete
            FROM i CROSS JOIN (VALUES ('WFA1', 'Alpha'), ('WFB1', 'Beta')) AS f(duid, station_name)
        """, silver_dir / "wind_output" / f"date={DAY}.parquet")
        _write(con, f"""
            WITH i AS (SELECT range AS n FROM range(0, 288))
            SELECT DATE '{DAY}' AS trading_date,
                   TIMESTAMPTZ '2026-10-12 00:00:00+10'
                       + (n * INTERVAL 5 MINUTE) AS interval_start_utc,
                   TIMESTAMP '2026-10-12 00:00:00'
                       + (n * INTERVAL 5 MINUTE) AS interval_start_nem,
                   'SA1' AS region,
                   CASE WHEN n BETWEEN 204 AND 227 THEN 300.0 ELSE 10.0 END AS rrp,
                   false AS is_negative, 0 AS intervention, 0 AS apcflag,
                   0 AS marketsuspendedflag, 'p.zip' AS source_file,
                   288 AS intervals_present, true AS is_complete
            FROM i
        """, silver_dir / "price" / f"date={DAY}.parquet")
    finally:
        con.close()
    return silver_dir


def run(gold, tmp_path, **kwargs):
    """Export with no silver, so a test cannot quietly read the real repo."""
    out = tmp_path / "out"
    kwargs.setdefault("silver_dir", tmp_path / "no-silver")
    export(gold_dir=gold, out_dir=out, **kwargs)
    return out


def run_full(gold, silver, tmp_path, **kwargs):
    out = tmp_path / "out"
    export(gold_dir=gold, silver_dir=silver, out_dir=out, **kwargs)
    return out


def farm_file(out, duid):
    return json.loads((out / "farm" / f"{duid}.json").read_text())


@pytest.fixture
def built_gold(silver, tmp_path):
    """Gold built from the silver fixture by the real models."""
    from build_gold import build

    gold_dir = tmp_path / "built-gold"
    build(
        silver_dir=silver,
        bronze_dir=tmp_path / "no-bronze",
        gold_dir=gold_dir,
        registry=tmp_path / "no-registry.csv",
    )
    return gold_dir


def load(out, name):
    return json.loads((out / f"{name}.json").read_text())


def test_every_slice_is_written(gold, tmp_path):
    out = run(gold, tmp_path)
    for name in ("region_today", "cf_trend", "farm_cf_week", "revenue_region",
                 "capture", "quality", "freshness", "farm_index", "meta"):
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


# --- farm detail ----------------------------------------------------------
#
# The farm page fetches one file per farm by DUID. A renamed key here is a
# blank page there, and nothing else in CI looks at these files.


def test_a_file_is_written_for_every_farm_in_the_index(built_gold, silver, tmp_path):
    out = run_full(built_gold, silver, tmp_path)
    index = load(out, "farm_index")
    assert index, "the index is what the dropdown is built from"
    for row in index:
        assert (out / "farm" / f"{row['duid']}.json").exists(), row["duid"]


def test_farm_file_carries_every_section_the_page_reads(built_gold, silver, tmp_path):
    detail = farm_file(run_full(built_gold, silver, tmp_path), "WFA1")
    assert set(detail) == {"farm", "daily", "profile", "shape"}
    assert detail["farm"]["duid"] == "WFA1"
    assert len(detail["profile"]) == 288, "a complete day is 288 five-minute intervals"
    assert len(detail["shape"]) == 24, "the merchant profile is one point per hour"


def test_profile_pairs_output_with_the_price_of_the_same_interval(built_gold, silver, tmp_path):
    profile = farm_file(run_full(built_gold, silver, tmp_path), "WFB1")["profile"]
    peak = [row for row in profile if row["mw"] > 0]
    assert len(peak) == 24, "WFB1 generates for two hours only"
    # Settlement is interval by interval: every generating interval is a $300 one.
    assert all(row["rrp"] == 300.0 for row in peak)
    assert all(row["rrp"] == 10.0 for row in profile if row["mw"] == 0)


def test_capture_price_rewards_generating_at_the_right_time(built_gold, silver, tmp_path):
    index = {row["duid"]: row for row in load(run_full(built_gold, silver, tmp_path), "farm_index")}
    # Both farms produce 1,200 MWh. WFB1 sells all of it at $300 and WFA1 almost
    # none of it, which is the entire point of the page.
    assert index["WFA1"]["energy_mwh"] == pytest.approx(1200.0)
    assert index["WFB1"]["energy_mwh"] == pytest.approx(1200.0)
    assert index["WFB1"]["capture_price_aud_mwh"] == pytest.approx(300.0)
    assert index["WFA1"]["capture_price_aud_mwh"] < 40
    assert index["WFB1"]["revenue_per_mw_day"] > index["WFA1"]["revenue_per_mw_day"] * 5


def test_shape_shows_when_each_farm_generates(built_gold, silver, tmp_path):
    out = run_full(built_gold, silver, tmp_path)
    flat = {r["hour"]: r["avg_mw"] for r in farm_file(out, "WFA1")["shape"]}
    peaky = {r["hour"]: r["avg_mw"] for r in farm_file(out, "WFB1")["shape"]}
    assert set(flat.values()) == {50.0}, "WFA1 is flat around the clock"
    assert peaky["17:00"] == 600.0 and peaky["18:00"] == 600.0
    assert peaky["03:00"] == 0.0


def test_duid_is_not_repeated_on_every_row(built_gold, silver, tmp_path):
    # The duid is the file name; repeating it 288 times is wasted bytes.
    detail = farm_file(run_full(built_gold, silver, tmp_path), "WFA1")
    assert "duid" not in detail["profile"][0]
    assert "duid" not in detail["shape"][0]


def test_each_farm_file_stays_small_enough_for_a_phone(built_gold, silver, tmp_path):
    out = run_full(built_gold, silver, tmp_path)
    for path in (out / "farm").glob("*.json"):
        assert path.stat().st_size < 120_000, f"{path.name} is {path.stat().st_size} bytes"


def test_without_silver_the_farm_files_keep_what_gold_can_still_answer(gold, tmp_path):
    """Sections degrade one at a time rather than the page disappearing.

    Only the five-minute profile and the hourly shape come from silver. The
    daily record is gold, so it survives, and the page renders the sections it
    was given — a farm with no complete day yet hits the same path.
    """
    out = run(gold, tmp_path)
    assert (out / "farm_index.json").exists()
    assert (out / "cf_trend.json").exists()

    detail = farm_file(out, "WFA1")
    assert detail["daily"], "the daily record comes from gold and must survive"
    assert detail["profile"] == []
    assert detail["shape"] == []


def test_a_duid_that_could_escape_the_directory_is_refused(gold, silver, tmp_path):
    from export_dashboard import group_by_duid

    rows = [{"duid": "../../etc/passwd", "mw": 1}, {"duid": "WFA1", "mw": 2}]
    assert set(group_by_duid(rows)) == {"WFA1"}
