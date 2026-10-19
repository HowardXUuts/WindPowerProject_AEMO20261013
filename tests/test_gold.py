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

Regional demand is fixed so the penetration bands are exact, and solar is set
so the two SA1 bands differ in it — the control column has to be shown to be
carried through, not merely present:

  SA1   demand 200 MW, solar 50 MW then 0 MW
        wind penetration  80/200 = 40%  (band 4)  then  20/200 = 10%  (band 2)
        solar penetration 50/200 = 25%            then   0/200 =  0%
  VIC1  demand 100 MW, solar 10 MW
        wind penetration  25/100 = 25%  (band 3), solar 10%

Approximate assertions would pass against an average taken the wrong way
round, so these are exact.
"""

import math
from datetime import date, datetime, timedelta

import duckdb
import pytest
from build_gold import build
from ingest_dispatchis import REGIONSUM_TABLE
from ingest_scada import SCADA
from nemweb import NEM_TZ, bronze_path, write_bronze

UNIT = SCADA.tables[0]  # the bronze table SCADA writes

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


def pd_isna(value):
    return value != value


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


def regionsum_rows():
    """Bronze regional demand. settlementdate is interval-ENDING, as AEMO sends it."""
    rows = []
    for region, demand, solar in (
        ("SA1", 200.0, lambda i: 50.0 if i < HALF else 0.0),
        ("VIC1", 100.0, lambda i: 10.0),
    ):
        for i in range(INTERVALS):
            utc, _ = starts(i)
            end = utc + timedelta(minutes=5)
            rows.append(
                (
                    end, region, 0, demand, demand * 2, 0.0,
                    None, solar(i), None, None, None, end,
                    "d.zip", end,
                )
            )
    return rows


def write_regionsum(bronze):
    """Write the demand rows one file per NEM hour, as the real ingest does."""
    by_hour: dict = {}
    for row in regionsum_rows():
        hour = row[0].replace(minute=0, second=0, microsecond=0)
        by_hour.setdefault(hour, []).append(row)
    for hour, rows in by_hour.items():
        write_bronze(REGIONSUM_TABLE, rows, bronze_path(REGIONSUM_TABLE, hour, bronze))


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
    write_bronze(UNIT,
        [
            (tomorrow, "WFA1", 99.0, "fresh.zip", tomorrow),
            (tomorrow, "WFB1", 10.0, "fresh.zip", tomorrow),
        ],
        bronze_path(UNIT, tomorrow - timedelta(minutes=5), bronze),
    )

    write_regionsum(bronze)

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
    write_bronze(UNIT, [(stale, "WFA1", 80.0, "old.zip", stale)],
        bronze_path(UNIT, stale - timedelta(minutes=5), bronze),
    )
    registry = tmp_path / "wind_duids.csv"
    registry.write_text(REGISTRY)
    build(silver_dir=silver, bronze_dir=bronze, gold_dir=tmp_path / "gold", registry=registry)

    frame = rows(tmp_path / "gold", "latest_snapshot", "duid").set_index("duid")
    # The newest interval is silver's last one: WFA1 at 20 MW, WFB1 at 25.
    assert set(frame["source_layer"]) == {"silver"}
    assert frame.loc["WFA1", "mw"] == 20.0


# --- cannibalisation ------------------------------------------------------


def test_cannibalisation_buckets_by_wind_penetration(gold):
    df = rows(gold, "cannibalisation", "region, bucket")
    assert list(zip(df.region, df.bucket, strict=True)) == [("SA1", 2), ("SA1", 4), ("VIC1", 3)]
    assert list(df.bucket_label) == ["10-20%", "30-45%", "20-30%"]


def test_cannibalisation_penetration_is_exact(gold):
    df = rows(gold, "cannibalisation", "region, bucket").set_index(["region", "bucket"])
    assert df.loc[("SA1", 4), "median_penetration_pct"] == pytest.approx(40.0)
    assert df.loc[("SA1", 2), "median_penetration_pct"] == pytest.approx(10.0)
    assert df.loc[("VIC1", 3), "median_penetration_pct"] == pytest.approx(25.0)


def test_cannibalisation_carries_the_solar_control(gold):
    """The column the chart cannot be read honestly without."""
    df = rows(gold, "cannibalisation", "region, bucket").set_index(["region", "bucket"])
    assert df.loc[("SA1", 4), "median_solar_penetration_pct"] == pytest.approx(25.0)
    assert df.loc[("SA1", 2), "median_solar_penetration_pct"] == pytest.approx(0.0)
    assert df.loc[("VIC1", 3), "median_solar_penetration_pct"] == pytest.approx(10.0)
    # Combined, because a reader comparing regions wants total VRE pressure.
    assert df.loc[("SA1", 4), "median_vre_penetration_pct"] == pytest.approx(65.0)


def test_cannibalisation_price_is_output_weighted_within_the_bucket(gold):
    df = rows(gold, "cannibalisation", "region, bucket").set_index(["region", "bucket"])
    # Price is constant inside each fixture bucket, so capture equals the
    # simple average and a weighting mistake cannot hide.
    assert df.loc[("SA1", 4), "capture_price_aud_mwh"] == pytest.approx(100.0)
    assert df.loc[("SA1", 4), "avg_rrp_aud_mwh"] == pytest.approx(100.0)
    assert df.loc[("SA1", 2), "capture_price_aud_mwh"] == pytest.approx(-10.0)
    assert df.loc[("VIC1", 3), "capture_price_aud_mwh"] == pytest.approx(40.0)


def test_cannibalisation_energy_share_splits_the_region(gold):
    df = rows(gold, "cannibalisation", "region, bucket").set_index(["region", "bucket"])
    # 80 MW for half a day against 20 MW for the other: 80% of the energy sits
    # in the high-penetration bucket. This is the column that says whether a
    # steep curve matters.
    assert df.loc[("SA1", 4), "pct_of_wind_energy"] == pytest.approx(80.0)
    assert df.loc[("SA1", 2), "pct_of_wind_energy"] == pytest.approx(20.0)
    assert df.loc[("SA1", 4), "wind_energy_mwh"] == pytest.approx(960.0)
    assert df.loc[("SA1", 2), "wind_energy_mwh"] == pytest.approx(240.0)
    # One bucket, so the whole region.
    assert df.loc[("VIC1", 3), "pct_of_wind_energy"] == pytest.approx(100.0)


def test_cannibalisation_counts_negative_intervals(gold):
    df = rows(gold, "cannibalisation", "region, bucket").set_index(["region", "bucket"])
    assert df.loc[("SA1", 2), "pct_intervals_negative"] == pytest.approx(100.0)
    assert df.loc[("SA1", 4), "pct_intervals_negative"] == pytest.approx(0.0)
    assert list(df.intervals) == [HALF, HALF, INTERVALS]


def test_cannibalisation_shifts_aemos_interval_ending_stamp(gold):
    """settlementdate is interval-ending and the model shifts it to a start.

    The fixture writes demand stamped at interval ends, so a model that joined
    it unshifted would pair each demand figure with the interval before it and
    lose the first and last. An off-by-one shows up here as a short count
    rather than as a subtly wrong average, which is the point of asserting it.
    """
    df = rows(gold, "cannibalisation", "region, bucket")
    assert int(df[df.region == "SA1"].intervals.sum()) == INTERVALS
    assert int(df[df.region == "VIC1"].intervals.sum()) == INTERVALS


def test_cannibalisation_is_skipped_without_regional_demand(tmp_path):
    """No demand, no denominator — absent rather than silently unbucketed."""
    silver = tmp_path / "silver"
    _write(WIND_DDL, "wind", wind_rows(), silver / "wind_output" / f"date={DAY}.parquet")
    _write(PRICE_DDL, "price", price_rows(), silver / "price" / f"date={DAY}.parquet")
    registry = tmp_path / "wind_duids.csv"
    registry.write_text(REGISTRY)

    written = build(
        silver_dir=silver, bronze_dir=tmp_path / "bronze",
        gold_dir=tmp_path / "gold", registry=registry,
    )
    assert "cannibalisation" not in written
    assert not (tmp_path / "gold" / "cannibalisation.parquet").exists()
    # Revenue concentration needs only silver, so it still builds.
    assert "revenue_concentration" in written


# --- revenue_concentration ------------------------------------------------


def test_concentration_totals_match_the_revenue_model(gold):
    df = rows(gold, "revenue_concentration", "region").set_index("region")
    assert df.loc["SA1", "revenue_aud"] == pytest.approx(93_600.0)
    assert df.loc["VIC1", "revenue_aud"] == pytest.approx(24_000.0)
    assert df.loc["SA1", "intervals"] == INTERVALS


def test_concentration_slice_is_rounded_up_never_empty(gold):
    """1% of 288 intervals is 2.88, and a slice of zero intervals says nothing."""
    df = rows(gold, "revenue_concentration", "region").set_index("region")
    assert df.loc["SA1", "top_1pct_intervals"] == 3


def test_concentration_shares_are_exact(gold):
    df = rows(gold, "revenue_concentration", "region").set_index("region")
    # SA1 earns 80/12 x $100 = $666.67 in each of its good intervals.
    #   top 1%  = 3  x 666.67 =  2,000 / 93,600 =  2.137%
    #   top 5%  = 15 x 666.67 = 10,000 / 93,600 = 10.684%
    #   top 10% = 29 x 666.67 = 19,333 / 93,600 = 20.655%
    # Tolerance is half the last stored decimal: the model rounds to four so
    # that a parallel float sum cannot move the committed bytes.
    assert df.loc["SA1", "pct_from_top_1pct"] == pytest.approx(2.136752, abs=5e-5)
    assert df.loc["SA1", "pct_from_top_5pct"] == pytest.approx(10.683761, abs=5e-5)
    assert df.loc["SA1", "pct_from_top_10pct"] == pytest.approx(20.655271, abs=5e-5)


def test_concentration_of_a_flat_region_tracks_the_interval_share(gold):
    """A region earning the same every interval must show no concentration.

    VIC1 is flat by construction, so each share should equal the share of
    intervals it covers. This is the control that proves the metric measures
    concentration rather than simply counting intervals.
    """
    df = rows(gold, "revenue_concentration", "region").set_index("region")
    assert df.loc["VIC1", "pct_from_top_1pct"] == pytest.approx(100 * 3 / INTERVALS, abs=5e-5)
    assert df.loc["VIC1", "pct_from_top_5pct"] == pytest.approx(100 * 15 / INTERVALS, abs=5e-5)
    assert df.loc["VIC1", "pct_from_top_10pct"] == pytest.approx(100 * 29 / INTERVALS, abs=5e-5)


def test_concentration_reports_the_negative_tail(gold):
    df = rows(gold, "revenue_concentration", "region").set_index("region")
    # 20 MW for 144 intervals at -$10: (20/12 x -10) x 144 = -$2,400.
    assert df.loc["SA1", "negative_revenue_aud"] == pytest.approx(-2_400.0)
    assert df.loc["SA1", "pct_intervals_negative"] == pytest.approx(50.0)
    # No negative interval at all is null, not zero: "none happened" and
    # "they netted to nothing" are different facts.
    assert df.loc["VIC1", "negative_revenue_aud"] is None or pd_isna(
        df.loc["VIC1", "negative_revenue_aud"]
    )


def test_concentration_ranks_by_revenue_not_price(gold):
    """A high price in an interval with no output is not where money came from.

    SA1's good intervals all carry the same price, so ranking by price alone
    would be free to pick the 20 MW intervals first and the top-slice shares
    would come out lower than the hand-computed figures above.
    """
    df = rows(gold, "revenue_concentration", "region").set_index("region")
    best = 3 * (80.0 / 12.0) * 100.0
    assert df.loc["SA1", "pct_from_top_1pct"] == pytest.approx(
        100.0 * best / 93_600.0, abs=5e-5
    )


# --- negative-price runs --------------------------------------------------

# A second fixture, because the main one yields a single 144-interval run and
# says nothing about where one run ends and the next begins. This one is built
# so every boundary case is hand-checkable:
#
#   RUN1   interval 0        negative           -> 1 run  of 1   (bucket 1)
#          intervals 2-4     negative           -> 1 run  of 3   (bucket 2)
#          intervals 6-11    negative           -> 1 run  of 6   (bucket 3)
#          intervals 20,21   negative   ]  interval 22 is ABSENT from the data,
#          intervals 23,24   negative   ]  so these are two runs of 2, not one
#                                          run of 4
#   OPEN1  final 3 intervals negative           -> a run still in progress
#
#   RUN1 negative intervals  1 + 3 + 6 + 2 + 2            = 14
#   RUN1 runs                1 + 1 + 1 + 1 + 1            = 5
#   bucket 2 holds the 3 and both 2s                      = 3 runs, 7 intervals
#
# Wind is a flat 10 MW and the negative price a flat -$50, so energy is
# 10/12 MWh an interval and revenue follows the interval count exactly.

RUN_DAY = date(2026, 10, 14)
RUN_WIND_MW = 10.0
RUN_NEG_RRP = -50.0
RUN_POS_RRP = 100.0
RUN_NEGATIVE = {0, 2, 3, 4, 6, 7, 8, 9, 10, 11, 20, 21, 23, 24}
RUN_ABSENT = 22  # dropped entirely, to prove a gap breaks a run


def run_starts(i):
    nem_local = datetime(RUN_DAY.year, RUN_DAY.month, RUN_DAY.day) + timedelta(minutes=5 * i)
    return nem_local.replace(tzinfo=NEM_TZ), nem_local


@pytest.fixture
def runs_gold(tmp_path):
    """Gold built from the run-boundary fixture above."""
    silver = tmp_path / "silver"
    wind, price = [], []

    for i in range(INTERVALS):
        if i == RUN_ABSENT:
            continue
        utc, nem_local = run_starts(i)
        rrp = RUN_NEG_RRP if i in RUN_NEGATIVE else RUN_POS_RRP
        wind.append((
            RUN_DAY, utc, nem_local, utc + timedelta(minutes=5), nem_local + timedelta(minutes=5),
            "WFR1", "Run Wind Farm", "RUN1", 50.0, RUN_WIND_MW, False, False,
            "f.zip", INTERVALS, True,
        ))
        price.append((
            RUN_DAY, utc, nem_local, utc + timedelta(minutes=5), nem_local + timedelta(minutes=5),
            "RUN1", rrp, rrp < 0, 0, 0, 0, "p.zip", INTERVALS, True,
        ))

    # A region whose last interval is negative, so one run is left open.
    for i in range(INTERVALS):
        utc, nem_local = run_starts(i)
        rrp = RUN_NEG_RRP if i >= INTERVALS - 3 else RUN_POS_RRP
        wind.append((
            RUN_DAY, utc, nem_local, utc + timedelta(minutes=5), nem_local + timedelta(minutes=5),
            "WFO1", "Open Wind Farm", "OPEN1", 50.0, RUN_WIND_MW, False, False,
            "f.zip", INTERVALS, True,
        ))
        price.append((
            RUN_DAY, utc, nem_local, utc + timedelta(minutes=5), nem_local + timedelta(minutes=5),
            "OPEN1", rrp, rrp < 0, 0, 0, 0, "p.zip", INTERVALS, True,
        ))

    _write(WIND_DDL, "wind", wind, silver / "wind_output" / f"date={RUN_DAY}.parquet")
    _write(PRICE_DDL, "price", price, silver / "price" / f"date={RUN_DAY}.parquet")
    registry = tmp_path / "wind_duids.csv"
    registry.write_text(REGISTRY)
    build(
        silver_dir=silver, bronze_dir=tmp_path / "bronze",
        gold_dir=tmp_path / "gold", registry=registry,
    )
    return tmp_path / "gold"


def test_a_missing_interval_breaks_a_run(runs_gold):
    """The point of islanding on a time sequence rather than on row order.

    Intervals 20, 21, 23 and 24 are negative and 22 is absent from the data.
    Islanded by row order these weld into one run of four; islanded by the
    five-minute sequence they stay two runs of two, which is what the data
    actually supports.
    """
    df = rows(runs_gold, "negative_price_runs", "region, bucket")
    df = df[df.region == "RUN1"].set_index("bucket")
    assert df.loc[2, "runs"] == 3          # the 3, and both 2s
    assert df.loc[2, "intervals"] == 7     # 3 + 2 + 2
    assert 3 not in df.index or df.loc[3, "runs"] == 1
    assert df.loc[3, "intervals"] == 6     # the 4-6 bucket holds only the six
    assert int(df.runs.sum()) == 5
    assert int(df.intervals.sum()) == 14


def test_run_buckets_are_assigned_by_length(runs_gold):
    df = rows(runs_gold, "negative_price_runs", "region, bucket")
    df = df[df.region == "RUN1"].set_index("bucket")
    assert list(df.index) == [1, 2, 3]
    assert list(df.bucket_label) == ["5 min", "10-15 min", "20-30 min"]
    assert df.loc[1, "runs"] == 1 and df.loc[1, "intervals"] == 1


def test_run_shares_are_exact(runs_gold):
    df = rows(runs_gold, "negative_price_runs", "region, bucket")
    df = df[df.region == "RUN1"].set_index("bucket")
    # 1, 7 and 6 of 14 negative intervals; 1, 3 and 1 of 5 runs.
    assert df.loc[1, "pct_of_negative_intervals"] == pytest.approx(100 / 14, abs=5e-5)
    assert df.loc[2, "pct_of_negative_intervals"] == pytest.approx(50.0, abs=5e-5)
    assert df.loc[3, "pct_of_negative_intervals"] == pytest.approx(600 / 14, abs=5e-5)
    assert df.loc[2, "pct_of_runs"] == pytest.approx(60.0, abs=5e-5)
    assert df.loc[1, "pct_of_runs"] == pytest.approx(20.0, abs=5e-5)
    # The contrast the whole measure exists for: 60% of the runs, 50% of the
    # time; 20% of the runs, 43% of the time.
    assert df.loc[3, "pct_of_runs"] < df.loc[3, "pct_of_negative_intervals"]


def test_run_lengths_are_reported_in_minutes(runs_gold):
    df = rows(runs_gold, "negative_price_runs", "region, bucket")
    df = df[df.region == "RUN1"].set_index("bucket")
    # Bucket 2 holds runs of 3, 2 and 2 intervals: median 2 -> 10 minutes,
    # longest 3 -> 15 minutes.
    assert df.loc[2, "median_run_minutes"] == pytest.approx(10.0)
    assert df.loc[2, "longest_run_minutes"] == 15
    assert df.loc[3, "longest_run_minutes"] == 30


def test_run_energy_and_revenue_are_exact(runs_gold):
    df = rows(runs_gold, "negative_price_runs", "region, bucket")
    df = df[df.region == "RUN1"].set_index("bucket")
    per_interval = RUN_WIND_MW * 5.0 / 60.0  # 0.8333 MWh
    assert df.loc[2, "wind_energy_mwh"] == pytest.approx(7 * per_interval, abs=5e-4)
    assert df.loc[3, "wind_energy_mwh"] == pytest.approx(6 * per_interval, abs=5e-4)
    assert df.loc[3, "revenue_aud"] == pytest.approx(6 * per_interval * RUN_NEG_RRP, abs=5e-3)
    assert df.loc[1, "deepest_rrp"] == pytest.approx(RUN_NEG_RRP)
    # Wind is flat, so the energy share must equal the interval share. A
    # weighting mistake would separate them.
    for bucket in (1, 2, 3):
        assert df.loc[bucket, "pct_of_negative_energy"] == pytest.approx(
            df.loc[bucket, "pct_of_negative_intervals"], abs=5e-4
        )


def test_worst_runs_are_ranked_longest_first(runs_gold):
    df = rows(runs_gold, "negative_price_worst", "region, rank")
    run1 = df[df.region == "RUN1"]
    assert list(run1["rank"]) == [1, 2, 3, 4, 5]
    assert list(run1.intervals) == [6, 3, 2, 2, 1]
    assert run1.iloc[0].duration_hours == pytest.approx(0.5)


def test_worst_run_window_is_the_interval_it_covers(runs_gold):
    """A run ends when its last interval ends, not when that interval starts."""
    df = rows(runs_gold, "negative_price_worst", "region, rank")
    longest = df[df.region == "RUN1"].iloc[0]
    # Intervals 6..11 on the fixture day: 00:30 through to the end of 00:55.
    assert str(longest.started_nem) == "2026-10-14 00:30:00"
    assert str(longest.ended_nem) == "2026-10-14 01:00:00"


def test_an_unfinished_run_is_flagged_not_hidden(runs_gold):
    """Its duration is a floor, so ranking it silently would overstate nothing
    but understate it — the flag is what lets a reader tell."""
    df = rows(runs_gold, "negative_price_worst", "region, rank")
    assert not df[df.region == "RUN1"].is_open.any()
    open_region = df[df.region == "OPEN1"]
    assert len(open_region) == 1
    assert bool(open_region.iloc[0].is_open) is True
    assert open_region.iloc[0].intervals == 3


def test_a_region_that_never_goes_negative_has_no_rows(gold):
    """VIC1 holds a flat $40 all day, so it must be absent rather than zeroed."""
    df = rows(gold, "negative_price_runs", "region, bucket")
    assert "VIC1" not in set(df.region)
    assert "SA1" in set(df.region)


def test_the_main_fixtures_single_run_lands_in_the_top_bucket(gold):
    """SA1 is negative for 144 consecutive intervals: one run of twelve hours."""
    df = rows(gold, "negative_price_runs", "region, bucket")
    sa = df[df.region == "SA1"]
    assert len(sa) == 1
    assert sa.iloc[0].bucket == 8
    assert sa.iloc[0].bucket_label == ">8 h"
    assert sa.iloc[0].runs == 1
    assert sa.iloc[0].intervals == HALF
    assert sa.iloc[0].pct_of_negative_intervals == pytest.approx(100.0)
    worst = rows(gold, "negative_price_worst", "region, rank")
    assert worst[worst.region == "SA1"].iloc[0].duration_hours == pytest.approx(12.0)


# --- output correlation ---------------------------------------------------

# Correlation is the one measure here that cannot be checked by arithmetic on a
# flat fixture, so this one is built from signals whose correlation is known in
# advance by construction:
#
#   CORRA / CORRB   region COR1, identical sine waves           -> r = +1
#   CORRC           region COR2, the same wave inverted         -> r = -1 against A and B
#   CORRD           region COR2, a quarter-period phase shift   -> r = 0 against A (cos vs sin)
#   CORRFLAT        region COR1, constant output                -> excluded, no variation
#   CORRSHORT       region COR1, one hour only                  -> excluded, too little history
#
# Capacity differs between A and B on purpose: correlation is computed on
# capacity factor, so a model that correlated raw MW would still return 1 here,
# but the *regional* aggregate figures would move. The phase pair is what
# actually pins the maths — +1 and -1 survive almost any mistake, 0 does not.

CORR_DAY = date(2026, 10, 15)
CORR_PERIOD = 72  # intervals per full cycle: six hours


def corr_starts(i):
    nem_local = datetime(CORR_DAY.year, CORR_DAY.month, CORR_DAY.day) + timedelta(minutes=5 * i)
    return nem_local.replace(tzinfo=NEM_TZ), nem_local


CORR_FARMS = (
    # duid, name, region, capacity, profile(i) -> MW
    ("CORRA", "Corr A", "COR1", 100.0,
     lambda i: 50.0 + 40.0 * math.sin(2 * math.pi * i / CORR_PERIOD)),
    ("CORRB", "Corr B", "COR1", 200.0,
     lambda i: 100.0 + 80.0 * math.sin(2 * math.pi * i / CORR_PERIOD)),
    ("CORRC", "Corr C", "COR2", 100.0,
     lambda i: 50.0 - 40.0 * math.sin(2 * math.pi * i / CORR_PERIOD)),
    ("CORRD", "Corr D", "COR2", 100.0,
     lambda i: 50.0 + 40.0 * math.cos(2 * math.pi * i / CORR_PERIOD)),
    ("CORRFLAT", "Corr Flat", "COR1", 100.0, lambda i: 50.0),
)


@pytest.fixture
def corr_gold(tmp_path):
    """Gold built from signals whose pairwise correlation is known exactly."""
    silver = tmp_path / "silver"
    wind = []
    for duid, name, region, capacity, profile in CORR_FARMS:
        for i in range(INTERVALS):
            utc, nem_local = corr_starts(i)
            wind.append((
                CORR_DAY, utc, nem_local, utc + timedelta(minutes=5),
                nem_local + timedelta(minutes=5), duid, name, region, capacity,
                profile(i), False, False, "f.zip", INTERVALS, True,
            ))
    # Seen for only twelve intervals, so below the one-day floor.
    for i in range(12):
        utc, nem_local = corr_starts(i)
        wind.append((
            CORR_DAY, utc, nem_local, utc + timedelta(minutes=5),
            nem_local + timedelta(minutes=5), "CORRSHORT", "Corr Short", "COR1", 100.0,
            float(i), False, False, "f.zip", INTERVALS, True,
        ))

    _write(WIND_DDL, "wind", wind, silver / "wind_output" / f"date={CORR_DAY}.parquet")
    registry = tmp_path / "wind_duids.csv"
    registry.write_text(REGISTRY)
    build(
        silver_dir=silver, bronze_dir=tmp_path / "bronze",
        gold_dir=tmp_path / "gold", registry=registry,
    )
    return tmp_path / "gold"


def farm_r(gold, a, b):
    """The stored correlation between two farms, from the per-farm table."""
    df = rows(gold, "farm_correlation", "duid, relation, rank")
    match = df[(df.duid == a) & (df.other_duid == b)]
    assert not match.empty, f"no stored pair {a}-{b}"
    return float(match.iloc[0].r)


def test_identical_shapes_correlate_perfectly(corr_gold):
    """Different capacities, same shape: capacity factor must give exactly 1."""
    assert farm_r(corr_gold, "CORRA", "CORRB") == pytest.approx(1.0, abs=5e-5)


def test_inverted_shape_correlates_negatively(corr_gold):
    assert farm_r(corr_gold, "CORRA", "CORRC") == pytest.approx(-1.0, abs=5e-5)


def test_a_quarter_period_apart_is_uncorrelated(corr_gold):
    """The assertion that actually pins the maths.

    +1 and -1 survive most mistakes; a sine against a cosine of the same period
    integrates to zero and will not come out right by accident.
    """
    assert farm_r(corr_gold, "CORRA", "CORRD") == pytest.approx(0.0, abs=1e-3)


def test_correlation_is_stored_both_ways(corr_gold):
    """A page showing one farm should not need to know how the pair was keyed."""
    assert farm_r(corr_gold, "CORRB", "CORRA") == pytest.approx(
        farm_r(corr_gold, "CORRA", "CORRB"), abs=5e-5
    )


def test_degenerate_farms_are_excluded_and_counted(corr_gold):
    """A farm that never varied, and one seen for an hour, are both unusable."""
    farms = set(rows(corr_gold, "farm_correlation", "duid").duid)
    assert "CORRFLAT" not in farms      # no variation: correlation undefined
    assert "CORRSHORT" not in farms     # under a day of intervals
    assert {"CORRA", "CORRB", "CORRC", "CORRD"} <= farms
    matrix = rows(corr_gold, "output_correlation", "region_a, region_b")
    # Both exclusions are reported rather than silently applied.
    assert int(matrix.iloc[0].farms_excluded) == 2


def test_matrix_covers_every_region_pair_in_both_directions(corr_gold):
    df = rows(corr_gold, "output_correlation", "region_a, region_b")
    assert set(zip(df.region_a, df.region_b, strict=True)) == {
        ("COR1", "COR1"), ("COR2", "COR2"), ("COR1", "COR2"), ("COR2", "COR1"),
    }
    # The mirrored pair carries the same statistic.
    cross = df[df.region_a != df.region_b]
    assert cross.mean_r.nunique() == 1


def test_matrix_means_match_the_pairs_behind_them(corr_gold):
    df = rows(corr_gold, "output_correlation", "region_a, region_b").set_index(
        ["region_a", "region_b"]
    )
    # COR1 holds exactly one usable pair, A-B, which is +1.
    assert df.loc[("COR1", "COR1"), "farm_pairs"] == 1
    assert df.loc[("COR1", "COR1"), "mean_r"] == pytest.approx(1.0, abs=5e-5)
    # COR2 holds C-D: an inverted sine against a cosine, so also zero.
    assert df.loc[("COR2", "COR2"), "farm_pairs"] == 1
    assert df.loc[("COR2", "COR2"), "mean_r"] == pytest.approx(0.0, abs=1e-3)
    # Across the two regions: A-C, A-D, B-C, B-D = -1, 0, -1, 0 -> mean -0.5.
    assert df.loc[("COR1", "COR2"), "farm_pairs"] == 4
    assert df.loc[("COR1", "COR2"), "mean_r"] == pytest.approx(-0.5, abs=1e-3)
    assert df.loc[("COR1", "COR2"), "min_r"] == pytest.approx(-1.0, abs=5e-5)
    assert df.loc[("COR1", "COR2"), "max_r"] == pytest.approx(0.0, abs=1e-3)


def test_portfolio_correlation_is_the_aggregate_not_the_mean_pair(corr_gold):
    """The two columns are different statistics and must not be conflated.

    COR1's aggregate is A+B, a pure sine. COR2's is C+D, an inverted sine plus a
    cosine, which sums to another sinusoid of the same period shifted between
    the two — so the aggregates correlate at -1/sqrt(2) while the mean farm pair
    is -0.5. Equal values here would mean one column was computed twice.
    """
    df = rows(corr_gold, "output_correlation", "region_a, region_b").set_index(
        ["region_a", "region_b"]
    )
    assert df.loc[("COR1", "COR2"), "portfolio_r"] == pytest.approx(
        -1 / math.sqrt(2), abs=1e-3
    )
    assert df.loc[("COR1", "COR1"), "portfolio_r"] == pytest.approx(1.0, abs=5e-5)


def test_each_farm_keeps_five_of_each_tail_at_most(corr_gold):
    df = rows(corr_gold, "farm_correlation", "duid, relation, rank")
    for _duid, group in df.groupby("duid"):
        for relation in ("most", "least"):
            picked = group[group.relation == relation]
            # Only three counterparts exist in this fixture, so three is correct;
            # the cap matters on the real fleet of 96.
            assert len(picked) == 3
            assert list(picked["rank"]) == [1, 2, 3]
    # Most-correlated is ordered downward, least upward.
    a_most = df[(df.duid == "CORRA") & (df.relation == "most")].sort_values("rank")
    assert list(a_most.r) == sorted(a_most.r, reverse=True)
    a_least = df[(df.duid == "CORRA") & (df.relation == "least")].sort_values("rank")
    assert list(a_least.r) == sorted(a_least.r)


# --- demand shape ---------------------------------------------------------

# The main fixture already supplies both halves: SA1 wind is 80 MW for the
# first twelve NEM hours and 20 MW for the second, and regionsum_rows() holds a
# flat 200 MW of demand all day. So the penetration shape is known exactly:
#
#   hours 00-11   80 / 200  = 40%
#   hours 12-23   20 / 200  = 10%
#   VIC1          25 / 100  = 25%, every hour
#
# Flat demand is the point: it isolates the numerator, so a bug in the hour
# bucketing or in the interval-ending shift shows up as a step in the wrong
# place rather than as a plausible curve.


def test_demand_shape_covers_every_hour_of_every_region(gold):
    df = rows(gold, "demand_shape", "region, hour_nem")
    assert len(df) == 48, "two regions x 24 NEM hours"
    assert sorted(df[df.region == "SA1"].hour_nem) == list(range(24))


def test_penetration_is_exact_per_hour(gold):
    df = rows(gold, "demand_shape", "region, hour_nem").set_index(["region", "hour_nem"])
    for hour in range(12):
        assert df.loc[("SA1", hour), "penetration_pct"] == pytest.approx(40.0, abs=5e-4)
    for hour in range(12, 24):
        assert df.loc[("SA1", hour), "penetration_pct"] == pytest.approx(10.0, abs=5e-4)
    for hour in range(24):
        assert df.loc[("VIC1", hour), "penetration_pct"] == pytest.approx(25.0, abs=5e-4)


def test_the_step_lands_on_the_right_hour(gold):
    """The fixture changes output exactly at NEM noon.

    An off-by-one in the interval-ending shift, or bucketing on UTC instead of
    NEM time, moves this boundary — which a smooth real-world curve would hide
    but a step cannot.
    """
    df = rows(gold, "demand_shape", "region, hour_nem").set_index(["region", "hour_nem"])
    assert df.loc[("SA1", 11), "penetration_pct"] == pytest.approx(40.0, abs=5e-4)
    assert df.loc[("SA1", 12), "penetration_pct"] == pytest.approx(10.0, abs=5e-4)


def test_shape_keeps_the_two_means_behind_the_ratio(gold):
    df = rows(gold, "demand_shape", "region, hour_nem").set_index(["region", "hour_nem"])
    assert df.loc[("SA1", 0), "mean_wind_mw"] == pytest.approx(80.0, abs=5e-4)
    assert df.loc[("SA1", 0), "mean_demand_mw"] == pytest.approx(200.0, abs=5e-4)
    assert df.loc[("SA1", 0), "intervals"] == 12, "twelve five-minute intervals an hour"
    assert df.loc[("SA1", 0), "days"] == 1


def test_shape_is_skipped_without_regional_demand(tmp_path):
    """No demand, no denominator — absent rather than a wind-only shape."""
    silver = tmp_path / "silver"
    _write(WIND_DDL, "wind", wind_rows(), silver / "wind_output" / f"date={DAY}.parquet")
    registry = tmp_path / "wind_duids.csv"
    registry.write_text(REGISTRY)
    written = build(
        silver_dir=silver, bronze_dir=tmp_path / "bronze",
        gold_dir=tmp_path / "gold", registry=registry,
    )
    assert "demand_shape" not in written
    assert not (tmp_path / "gold" / "demand_shape.parquet").exists()


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


def test_rebuild_is_byte_identical_for_every_table(gold):
    """Same inputs, same bytes — for all of gold, not just one table.

    Gold is rewritten on every hourly run and committed, so a table whose bytes
    move without its data moving adds a Parquet blob to git history every hour,
    permanently. On live data this was real: capture_price and region_hourly
    drifted in the last bit of their float aggregates, 34 KB a run, because
    floating-point addition is not associative and a parallel sum adds its
    partials in thread-completion order.

    Be clear about what this test does and does not buy. The fixture is two
    regions over one day, which is far too small to schedule a parallel sum, so
    it would pass even with the bug present — the property is actually secured
    by `SET threads TO 1` in build_gold, and by rounding each aggregate to a
    precision coarser than the drift. What this catches is the cruder
    regression: a model that writes a timestamp, a row order, or anything else
    that moves on every run regardless of thread count.
    """
    before = {p.name: p.read_bytes() for p in sorted(gold.glob("*.parquet"))}
    assert len(before) >= 7, f"expected every gold table, saw {sorted(before)}"

    for _ in range(3):
        build(
            silver_dir=gold.parent / "silver",
            bronze_dir=gold.parent / "bronze",
            gold_dir=gold,
            registry=gold.parent / "wind_duids.csv",
        )
        after = {p.name: p.read_bytes() for p in sorted(gold.glob("*.parquet"))}
        assert sorted(after) == sorted(before)
        unstable = [name for name in before if after[name] != before[name]]
        assert not unstable, f"bytes moved with no change in inputs: {unstable}"
