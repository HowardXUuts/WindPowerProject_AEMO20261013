"""Tests for the gold layer, asserting exact totals on a fixed fixture day.

The fixture is built so every figure can be checked by hand:

  WFA1  SA1   100 MW  80 MW for the first 12 hours, 20 MW for the second
  WFB1  VIC1   50 MW  25 MW all day
  SA1   price        $100 for the first 12 hours, -$10 for the second
  VIC1  price         $40 all day

  WFA1 energy    (80 x 144 + 20 x 144) / 12            = 1,200 MWh
  WFA1 factor    1200 / (100 x 24)                     = 0.50
  WFA1 revenue   (80/12 x 100 + 20/12 x -10) x 144     = $93,600
  WFA1 realised  93,600 / 1,200                        = $78.00/MWh
  WFA1 negative  20 x 144 / 12                         = 240 MWh
  SA1  capture   1,123,200 / 14,400                    = $78.00/MWh
  SA1  TWAP      (100 x 144 + -10 x 144) / 288         = $45.00/MWh
  SA1  ratio     78 / 45                               = 1.7333

Approximate assertions would pass against an average taken the wrong way
round, so these are exact.
"""

from datetime import date, datetime, timedelta

import duckdb
import pytest
from build_gold import build
from ingest_scada import SCADA
from nemweb import NEM_TZ, bronze_path, write_bronze

DAY = date(2026, 10, 12)
INTERVALS = 288
HALF = 144

WIND_DDL = """
CREATE OR REPLACE TABLE wind (
    trading_date DATE, interval_start_utc TIMESTAMPTZ, interval_start_nem TIMESTAMP,
    interval_end_utc TIMESTAMPTZ, interval_end_nem TIMESTAMP, duid VARCHAR,
    station_name VARCHAR, region VARCHAR, capacity_mw DOUBLE, mw DOUBLE,
    is_negative BOOLEAN, is_above_capacity BOOLEAN, source_file VARCHAR,
    intervals_present SMALLINT, is_complete BOOLEAN
)
"""

PRICE_DDL = """
CREATE OR REPLACE TABLE price (
    trading_date DATE, interval_start_utc TIMESTAMPTZ, interval_start_nem TIMESTAMP,
    interval_end_utc TIMESTAMPTZ, interval_end_nem TIMESTAMP, region VARCHAR,
    rrp DOUBLE, is_negative BOOLEAN, intervention INTEGER, apcflag INTEGER,
    marketsuspendedflag INTEGER, source_file VARCHAR,
    intervals_present SMALLINT, is_complete BOOLEAN
)
"""

REGISTRY = """duid,station_name,region,fuel_type,capacity_mw,in_scada
WFA1,Alpha Wind Farm,SA1,Wind,100.0,true
WFB1,Beta Wind Farm,VIC1,Wind,50.0,true
WFOFF1,Offline Wind Farm,VIC1,Wind,20.0,false
"""


def starts(i):
    nem_local = datetime(DAY.year, DAY.month, DAY.day) + timedelta(minutes=5 * i)
    return nem_local.replace(tzinfo=NEM_TZ), nem_local


def _write(ddl, table, rows, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(ddl)
        con.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * len(rows[0]))})", rows)
        con.execute(f"COPY {table} TO '{destination}' (FORMAT PARQUET)")
    finally:
        con.close()


def wind_rows():
    rows = []
    for duid, station, region, capacity, profile in (
        ("WFA1", "Alpha Wind Farm", "SA1", 100.0, lambda i: 80.0 if i < HALF else 20.0),
        ("WFB1", "Beta Wind Farm", "VIC1", 50.0, lambda i: 25.0),
    ):
        for i in range(INTERVALS):
            utc, nem_local = starts(i)
            mw = profile(i)
            rows.append(
                (
                    DAY, utc, nem_local, utc + timedelta(minutes=5),
                    nem_local + timedelta(minutes=5), duid, station, region, capacity, mw,
                    mw < 0, False, "f.zip", INTERVALS, True,
                )
            )
    return rows


def price_rows():
    rows = []
    for region, profile in (
        ("SA1", lambda i: 100.0 if i < HALF else -10.0),
        ("VIC1", lambda i: 40.0),
    ):
        for i in range(INTERVALS):
            utc, nem_local = starts(i)
            rrp = profile(i)
            rows.append(
                (
                    DAY, utc, nem_local, utc + timedelta(minutes=5),
                    nem_local + timedelta(minutes=5), region, rrp, rrp < 0, 0, 0, 0,
                    "p.zip", INTERVALS, True,
                )
            )
    return rows


@pytest.fixture
def gold(tmp_path):
    """Build every gold table from the fixture day, and return the directory."""
    silver, bronze = tmp_path / "silver", tmp_path / "bronze"
    _write(WIND_DDL, "wind", wind_rows(), silver / "wind_output" / f"date={DAY}.parquet")
    _write(PRICE_DDL, "price", price_rows(), silver / "price" / f"date={DAY}.parquet")

    # One bronze hour NEWER than silver, so the snapshot has something fresher.
    tomorrow = datetime(2026, 10, 13, 0, 5, tzinfo=NEM_TZ)
    write_bronze(
        SCADA,
        [
            (tomorrow, "WFA1", 99.0, "fresh.zip", tomorrow),
            (tomorrow, "WFB1", 10.0, "fresh.zip", tomorrow),
        ],
        bronze_path(SCADA, tomorrow - timedelta(minutes=5), bronze),
    )

    registry = tmp_path / "wind_duids.csv"
    registry.write_text(REGISTRY)

    build(
        silver_dir=silver,
        bronze_dir=bronze,
        gold_dir=tmp_path / "gold",
        registry=registry,
    )
    return tmp_path / "gold"


def rows(gold, table, order="1"):
    con = duckdb.connect()
    try:
        return con.execute(
            f"SELECT * FROM read_parquet('{gold / f'{table}.parquet'}') ORDER BY {order}"
        ).fetchdf()
    finally:
        con.close()


# --- farm_daily -----------------------------------------------------------


def test_farm_daily_energy_is_exact(gold):
    frame = rows(gold, "farm_daily", "duid").set_index("duid")
    assert frame.loc["WFA1", "energy_mwh"] == pytest.approx(1200.0)
    assert frame.loc["WFB1", "energy_mwh"] == pytest.approx(600.0)


def test_farm_daily_capacity_factor_is_exact(gold):
    frame = rows(gold, "farm_daily", "duid").set_index("duid")
    # 1200 MWh against 100 MW x 24 h, and 600 against 50 x 24: both exactly 0.5.
    assert frame.loc["WFA1", "capacity_factor"] == pytest.approx(0.5)
    assert frame.loc["WFB1", "capacity_factor"] == pytest.approx(0.5)


def test_farm_daily_peak_and_intervals(gold):
    frame = rows(gold, "farm_daily", "duid").set_index("duid")
    assert frame.loc["WFA1", "peak_mw"] == 80.0
    assert frame.loc["WFA1", "min_mw"] == 20.0
    assert frame.loc["WFA1", "intervals"] == INTERVALS
    assert bool(frame.loc["WFA1", "is_complete"]) is True


def test_farm_daily_has_one_row_per_farm_day(gold):
    assert len(rows(gold, "farm_daily")) == 2


# --- farm_revenue_daily ---------------------------------------------------


def test_revenue_is_settled_interval_by_interval(gold):
    frame = rows(gold, "farm_revenue_daily", "duid").set_index("duid")
    # Not 1200 MWh x the day's average price ($45 = $54,000), which is what
    # multiplying daily energy by a daily average would wrongly give.
    assert frame.loc["WFA1", "revenue_aud"] == pytest.approx(93600.0)
    assert frame.loc["WFB1", "revenue_aud"] == pytest.approx(24000.0)


def test_realised_price_is_exact(gold):
    frame = rows(gold, "farm_revenue_daily", "duid").set_index("duid")
    assert frame.loc["WFA1", "realised_price_aud_mwh"] == pytest.approx(78.0)
    assert frame.loc["WFB1", "realised_price_aud_mwh"] == pytest.approx(40.0)


def test_energy_generated_at_negative_prices(gold):
    frame = rows(gold, "farm_revenue_daily", "duid").set_index("duid")
    # 20 MW for 144 intervals, all of them below zero.
    assert frame.loc["WFA1", "negative_price_mwh"] == pytest.approx(240.0)
    assert frame.loc["WFA1", "negative_price_revenue_aud"] == pytest.approx(-2400.0)
    assert frame.loc["WFB1", "negative_price_mwh"] == pytest.approx(0.0)


def test_revenue_energy_matches_farm_daily(gold):
    daily = rows(gold, "farm_daily", "duid").set_index("duid")
    revenue = rows(gold, "farm_revenue_daily", "duid").set_index("duid")
    for duid in ("WFA1", "WFB1"):
        assert revenue.loc[duid, "energy_mwh"] == pytest.approx(daily.loc[duid, "energy_mwh"])


# --- capture_price --------------------------------------------------------


def test_capture_price_is_output_weighted(gold):
    frame = rows(gold, "capture_price", "region").set_index("region")
    assert frame.loc["SA1", "capture_price_aud_mwh"] == pytest.approx(78.0)
    assert frame.loc["VIC1", "capture_price_aud_mwh"] == pytest.approx(40.0)


def test_time_weighted_average_counts_each_interval_once(gold):
    frame = rows(gold, "capture_price", "region").set_index("region")
    # ($100 x 144 + -$10 x 144) / 288, independent of how much wind ran.
    assert frame.loc["SA1", "twap_aud_mwh"] == pytest.approx(45.0)
    assert frame.loc["VIC1", "twap_aud_mwh"] == pytest.approx(40.0)


def test_capture_ratio_is_exact(gold):
    frame = rows(gold, "capture_price", "region").set_index("region")
    assert frame.loc["SA1", "capture_ratio"] == pytest.approx(78.0 / 45.0)
    # Flat price and flat output: wind captures exactly the average.
    assert frame.loc["VIC1", "capture_ratio"] == pytest.approx(1.0)


def test_capture_price_matches_realised_price_for_a_single_farm_region(gold):
    # With one farm in the region the two are the same quantity, computed by
    # different routes; disagreement would mean one of them is weighted wrong.
    capture = rows(gold, "capture_price", "region").set_index("region")
    revenue = rows(gold, "farm_revenue_daily", "duid").set_index("duid")
    assert capture.loc["SA1", "capture_price_aud_mwh"] == pytest.approx(
        revenue.loc["WFA1", "realised_price_aud_mwh"]
    )


# --- region_hourly --------------------------------------------------------


def test_region_hourly_covers_every_hour_of_both_regions(gold):
    assert len(rows(gold, "region_hourly")) == 24 * 2


def test_region_hourly_values_are_exact(gold):
    frame = rows(gold, "region_hourly", "region, hour_nem")
    sa1 = frame[frame["region"] == "SA1"].reset_index(drop=True)

    # Hour 0 sits in the 80 MW half, hour 12 in the 20 MW half.
    assert sa1.loc[0, "avg_mw"] == pytest.approx(80.0)
    assert sa1.loc[0, "energy_mwh"] == pytest.approx(80.0)  # 80 MW for one hour
    assert sa1.loc[12, "avg_mw"] == pytest.approx(20.0)
    assert sa1.loc[12, "energy_mwh"] == pytest.approx(20.0)
    assert sa1.loc[0, "intervals"] == 12


def test_region_hourly_energy_sums_to_the_daily_total(gold):
    frame = rows(gold, "region_hourly")
    sa1 = frame[frame["region"] == "SA1"]["energy_mwh"].sum()
    assert sa1 == pytest.approx(1200.0)


def test_region_hourly_sums_farms_before_averaging(gold):
    # VIC1 has one farm at 25 MW; if farms were averaged after being pooled
    # with SA1's, this would not be 25.
    frame = rows(gold, "region_hourly", "region, hour_nem")
    vic = frame[frame["region"] == "VIC1"]
    assert set(vic["avg_mw"].round(6)) == {25.0}


# --- latest_snapshot ------------------------------------------------------


def test_snapshot_uses_the_freshest_reading_available(gold):
    frame = rows(gold, "latest_snapshot", "duid").set_index("duid")
    # Bronze holds 2026-10-13 00:00, newer than anything in silver.
    assert set(frame["source_layer"]) == {"bronze"}
    assert frame.loc["WFA1", "mw"] == 99.0
    assert frame.loc["WFB1", "mw"] == 10.0


def test_snapshot_percent_of_capacity_is_exact(gold):
    frame = rows(gold, "latest_snapshot", "duid").set_index("duid")
    assert frame.loc["WFA1", "pct_of_capacity"] == pytest.approx(0.99)  # 99 of 100
    assert frame.loc["WFB1", "pct_of_capacity"] == pytest.approx(0.20)  # 10 of 50


def test_snapshot_holds_one_row_per_scheduled_farm(gold):
    frame = rows(gold, "latest_snapshot")
    assert len(frame) == 2
    assert "WFOFF1" not in set(frame["duid"])  # in_scada = false


def test_snapshot_falls_back_to_silver_without_fresh_bronze(tmp_path):
    silver = tmp_path / "silver"
    _write(WIND_DDL, "wind", wind_rows(), silver / "wind_output" / f"date={DAY}.parquet")
    _write(PRICE_DDL, "price", price_rows(), silver / "price" / f"date={DAY}.parquet")

    # Bronze that only repeats what silver already has.
    bronze = tmp_path / "bronze"
    stale = datetime(2026, 10, 12, 0, 5, tzinfo=NEM_TZ)
    write_bronze(
        SCADA, [(stale, "WFA1", 80.0, "old.zip", stale)],
        bronze_path(SCADA, stale - timedelta(minutes=5), bronze),
    )
    registry = tmp_path / "wind_duids.csv"
    registry.write_text(REGISTRY)
    build(silver_dir=silver, bronze_dir=bronze, gold_dir=tmp_path / "gold", registry=registry)

    frame = rows(tmp_path / "gold", "latest_snapshot", "duid").set_index("duid")
    # The newest interval is silver's last one: WFA1 at 20 MW, WFB1 at 25.
    assert set(frame["source_layer"]) == {"silver"}
    assert frame.loc["WFA1", "mw"] == 20.0


# --- the runner -----------------------------------------------------------


def test_models_without_inputs_are_skipped_not_emptied(tmp_path):
    silver = tmp_path / "silver"
    _write(WIND_DDL, "wind", wind_rows(), silver / "wind_output" / f"date={DAY}.parquet")
    registry = tmp_path / "wind_duids.csv"
    registry.write_text(REGISTRY)

    written = build(
        silver_dir=silver, bronze_dir=tmp_path / "bronze",
        gold_dir=tmp_path / "gold", registry=registry,
    )

    # No price silver, so the two revenue models are absent rather than empty.
    assert "farm_daily" in written
    assert "farm_revenue_daily" not in written
    assert not (tmp_path / "gold" / "farm_revenue_daily.parquet").exists()


def test_rebuild_overwrites_in_place(gold):
    before = (gold / "farm_daily.parquet").read_bytes()
    build(
        silver_dir=gold.parent / "silver",
        bronze_dir=gold.parent / "bronze",
        gold_dir=gold,
        registry=gold.parent / "wind_duids.csv",
    )
    # Gold is a full rewrite: same inputs must give the same bytes back.
    assert (gold / "farm_daily.parquet").read_bytes() == before
