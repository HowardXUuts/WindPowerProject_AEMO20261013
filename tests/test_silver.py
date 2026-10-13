"""Tests for the silver layer.

The awkward parts are the ones covered hardest: AEMO's interval-ending
timestamps around midnight, and the rule deciding when a day may be written at
all, since a silver file is immutable once it exists.
"""

from datetime import date, datetime, timedelta

import duckdb
import pytest
from build_silver import (
    INTERVALS_PER_DAY,
    SilverError,
    build,
    is_sealed,
    ready_dates,
    silver_path,
)
from ingest_price import PRICE
from ingest_scada import SCADA
from nemweb import NEM_TZ, bronze_path, write_bronze

INGESTED = datetime(2026, 10, 14, 0, 0, tzinfo=NEM_TZ)

REGISTRY = """duid,station_name,region,fuel_type,capacity_mw,in_scada
WFA1,Alpha Wind Farm,SA1,Wind,100.0,true
WFB1,Beta Wind Farm,VIC1,Wind,50.0,true
WFOFF1,Offline Wind Farm,VIC1,Wind,20.0,false
"""


def write_registry(tmp_path, text=REGISTRY):
    path = tmp_path / "wind_duids.csv"
    path.write_text(text)
    return path


def nem(day: date, hour: int, minute: int) -> datetime:
    """An interval-ENDING timestamp in NEM time."""
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=NEM_TZ)


def write_scada(bronze_dir, rows):
    """Write bronze SCADA rows, grouped into hour files like the ingester."""
    by_hour: dict[datetime, list] = {}
    for settlement, duid, mw, source, ingested in rows:
        start = settlement - timedelta(minutes=5)
        by_hour.setdefault(start.replace(minute=0), []).append(
            (settlement, duid, mw, source, ingested)
        )
    for hour, hour_rows in by_hour.items():
        write_bronze(SCADA, hour_rows, bronze_path(SCADA, hour, bronze_dir))


def write_price(bronze_dir, rows):
    by_hour: dict[datetime, list] = {}
    for settlement, region, intervention, rrp, source, ingested in rows:
        start = settlement - timedelta(minutes=5)
        by_hour.setdefault(start.replace(minute=0), []).append(
            (settlement, region, intervention, 1, "i", rrp, 0.0, rrp, 0, 0, source, ingested)
        )
    for hour, hour_rows in by_hour.items():
        write_bronze(PRICE, hour_rows, bronze_path(PRICE, hour, bronze_dir))


def full_day(day: date, duid="WFA1", mw=10.0):
    """288 intervals, ending 00:05 on `day` through 00:00 the next day."""
    rows = []
    first_end = nem(day, 0, 5)
    for i in range(INTERVALS_PER_DAY):
        settlement = first_end + timedelta(minutes=5 * i)
        rows.append((settlement, duid, mw, f"f{i}.zip", INGESTED))
    return rows


def full_day_price(day: date, region="SA1", rrp=50.0):
    rows = []
    first_end = nem(day, 0, 5)
    for i in range(INTERVALS_PER_DAY):
        rows.append((first_end + timedelta(minutes=5 * i), region, 0, rrp, f"p{i}.zip", INGESTED))
    return rows


def read(path):
    con = duckdb.connect()
    try:
        return con.execute(
            f"SELECT * FROM read_parquet('{path}') ORDER BY interval_start_utc"
        ).fetchdf()
    finally:
        con.close()


def build_one(tmp_path, scada=None, price=None, **kwargs):
    """Write bronze, then build silver, returning the silver directory."""
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    if scada:
        write_scada(bronze, scada)
    if price:
        write_price(bronze, price)
    build(
        bronze_dir=bronze,
        silver_dir=silver,
        registry=write_registry(tmp_path),
        **kwargs,
    )
    return silver


# --- the midnight boundary -----------------------------------------------


def test_interval_ending_midnight_belongs_to_the_previous_day(tmp_path):
    # The headline case: 00:00 ends an interval that began at 23:55 yesterday.
    day = date(2026, 10, 12)
    silver = build_one(tmp_path, scada=full_day(day))
    frame = read(silver_path(silver, "wind_output", day))

    assert len(frame) == INTERVALS_PER_DAY
    assert str(frame.iloc[0]["interval_start_nem"])[:16] == "2026-10-12 00:00"
    assert str(frame.iloc[0]["interval_end_nem"])[:16] == "2026-10-12 00:05"
    # The last row of the day ends at midnight the FOLLOWING morning.
    assert str(frame.iloc[-1]["interval_start_nem"])[:16] == "2026-10-12 23:55"
    assert str(frame.iloc[-1]["interval_end_nem"])[:16] == "2026-10-13 00:00"
    assert set(frame["trading_date"].astype(str)) == {"2026-10-12"}


def test_the_0005_reading_starts_the_new_day(tmp_path):
    day = date(2026, 10, 12)
    silver = build_one(tmp_path, scada=full_day(day))
    frame = read(silver_path(silver, "wind_output", day))
    starts = {str(value)[:16] for value in frame["interval_start_nem"]}

    assert "2026-10-12 00:00" in starts  # the 00:05 reading
    assert "2026-10-11 23:55" not in starts  # that belongs to the 11th


def test_interval_start_is_five_minutes_before_the_end(tmp_path):
    day = date(2026, 10, 12)
    silver = build_one(tmp_path, scada=full_day(day))
    frame = read(silver_path(silver, "wind_output", day))
    gaps = frame["interval_end_utc"] - frame["interval_start_utc"]

    assert set(gaps.unique()) == {timedelta(minutes=5)}


def test_nem_local_is_utc_plus_ten(tmp_path):
    day = date(2026, 10, 12)
    silver = build_one(tmp_path, scada=full_day(day))
    frame = read(silver_path(silver, "wind_output", day))
    row = frame.iloc[0]

    # NEM time never shifts for daylight saving, so the offset is always +10.
    offset = row["interval_start_nem"].tz_localize("UTC") - row["interval_start_utc"]
    assert offset == timedelta(hours=10)


# --- the join -------------------------------------------------------------


def test_only_registry_wind_duids_survive(tmp_path):
    day = date(2026, 10, 12)
    rows = full_day(day, "WFA1") + full_day(day, "COAL1") + full_day(day, "WFB1")
    silver = build_one(tmp_path, scada=rows)
    frame = read(silver_path(silver, "wind_output", day))

    assert set(frame["duid"]) == {"WFA1", "WFB1"}  # COAL1 is not in the registry


def test_units_flagged_not_in_scada_are_excluded(tmp_path):
    day = date(2026, 10, 12)
    silver = build_one(tmp_path, scada=full_day(day, "WFA1") + full_day(day, "WFOFF1"))
    frame = read(silver_path(silver, "wind_output", day))

    # WFOFF1 has in_scada = false: a non-scheduled farm, excluded by the join.
    assert set(frame["duid"]) == {"WFA1"}


def test_registry_metadata_is_attached(tmp_path):
    day = date(2026, 10, 12)
    silver = build_one(tmp_path, scada=full_day(day))
    row = read(silver_path(silver, "wind_output", day)).iloc[0]

    assert row["station_name"] == "Alpha Wind Farm"
    assert row["region"] == "SA1"
    assert row["capacity_mw"] == 100.0


# --- flags, not filters ---------------------------------------------------


def test_negative_output_is_flagged_and_kept(tmp_path):
    day = date(2026, 10, 12)
    rows = full_day(day)
    rows[5] = (rows[5][0], "WFA1", -3.5, "neg.zip", INGESTED)
    silver = build_one(tmp_path, scada=rows)
    frame = read(silver_path(silver, "wind_output", day))

    assert len(frame) == INTERVALS_PER_DAY  # nothing dropped
    assert frame["is_negative"].sum() == 1
    assert frame.loc[frame["is_negative"], "mw"].iloc[0] == -3.5


def test_above_capacity_is_flagged_beyond_two_percent(tmp_path):
    day = date(2026, 10, 12)
    rows = full_day(day)
    rows[1] = (rows[1][0], "WFA1", 102.0, "at.zip", INGESTED)  # exactly 1.02x
    rows[2] = (rows[2][0], "WFA1", 102.5, "over.zip", INGESTED)  # beyond it
    silver = build_one(tmp_path, scada=rows)
    frame = read(silver_path(silver, "wind_output", day))

    flagged = set(frame.loc[frame["is_above_capacity"], "mw"])
    assert flagged == {102.5}  # the boundary itself is not flagged


def test_rows_with_no_reading_are_kept(tmp_path):
    day = date(2026, 10, 12)
    rows = full_day(day)
    rows[7] = (rows[7][0], "WFA1", None, "blank.zip", INGESTED)
    silver = build_one(tmp_path, scada=rows)
    frame = read(silver_path(silver, "wind_output", day))

    assert len(frame) == INTERVALS_PER_DAY
    assert frame["mw"].isna().sum() == 1


# --- deduplication --------------------------------------------------------


def test_duplicate_interval_and_duid_keeps_one_row(tmp_path):
    day = date(2026, 10, 12)
    rows = full_day(day)
    later = INGESTED + timedelta(hours=1)
    # AEMO republished this interval; both copies are in bronze.
    rows.append((rows[3][0], "WFA1", 99.0, "republished.zip", later))
    silver = build_one(tmp_path, scada=rows)
    frame = read(silver_path(silver, "wind_output", day))

    assert len(frame) == INTERVALS_PER_DAY
    assert 99.0 in set(frame["mw"])  # the later ingest wins


# --- when a day may be written -------------------------------------------


def test_incomplete_recent_day_is_not_written(tmp_path):
    day = date(2026, 10, 12)
    silver = build_one(tmp_path, scada=full_day(day)[:100])
    assert not silver_path(silver, "wind_output", day).exists()


def test_complete_day_is_written(tmp_path):
    day = date(2026, 10, 12)
    silver = build_one(tmp_path, scada=full_day(day))
    frame = read(silver_path(silver, "wind_output", day))

    assert bool(frame["is_complete"].iloc[0]) is True
    assert int(frame["intervals_present"].iloc[0]) == INTERVALS_PER_DAY


def test_sealed_incomplete_day_is_written_and_marked(tmp_path):
    # A day with a permanent gap: later days exist, so AEMO no longer holds the
    # missing files. Writing it is the only way silver ever advances past it.
    gappy, later = date(2026, 10, 12), date(2026, 10, 15)
    silver = build_one(tmp_path, scada=full_day(gappy)[:100] + full_day(later))
    frame = read(silver_path(silver, "wind_output", gappy))

    assert len(frame) == 100
    assert bool(frame["is_complete"].iloc[0]) is False
    assert int(frame["intervals_present"].iloc[0]) == 100


def test_existing_silver_file_is_never_rewritten(tmp_path):
    day = date(2026, 10, 12)
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    write_scada(bronze, full_day(day))
    registry = write_registry(tmp_path)

    build(bronze_dir=bronze, silver_dir=silver, registry=registry)
    destination = silver_path(silver, "wind_output", day)
    before = destination.stat().st_mtime_ns

    # More bronze arrives for the same day; the file must not move.
    write_scada(bronze, [(nem(day, 12, 5), "WFB1", 5.0, "extra.zip", INGESTED)])
    assert build(bronze_dir=bronze, silver_dir=silver, registry=registry) == 0
    assert destination.stat().st_mtime_ns == before


def test_dry_run_writes_nothing(tmp_path):
    day = date(2026, 10, 12)
    silver = build_one(tmp_path, scada=full_day(day), dry_run=True)
    assert not silver_path(silver, "wind_output", day).exists()


@pytest.mark.parametrize(
    ("trading_date", "latest", "expected"),
    [
        (date(2026, 10, 12), date(2026, 10, 13), False),  # still within retention
        (date(2026, 10, 12), date(2026, 10, 14), False),  # exactly at the edge
        (date(2026, 10, 12), date(2026, 10, 15), True),  # past it: unrecoverable
        (date(2026, 10, 12), None, False),  # no data at all
    ],
)
def test_is_sealed(trading_date, latest, expected):
    assert is_sealed(trading_date, latest, seal_after_days=2) is expected


def test_ready_dates_picks_complete_and_sealed_only():
    coverage = {
        date(2026, 10, 10): 100,  # sealed, incomplete
        date(2026, 10, 12): INTERVALS_PER_DAY,  # complete
        date(2026, 10, 15): 50,  # recent, incomplete
    }
    ready = ready_dates(coverage, latest_date=date(2026, 10, 15), seal_after_days=2)

    assert ready == [(date(2026, 10, 10), 100, False), (date(2026, 10, 12), 288, True)]


# --- price ---------------------------------------------------------------


def test_price_keeps_the_pricing_run(tmp_path):
    day = date(2026, 10, 12)
    rows = full_day_price(day)
    # An intervention interval: AEMO published a physical run too. Settlement
    # follows the pricing run (intervention = 0), so that is the one kept.
    rows.append((nem(day, 0, 5), "SA1", 1, 999.0, "phys.zip", INGESTED))
    silver = build_one(tmp_path, price=rows)
    frame = read(silver_path(silver, "price", day))

    assert len(frame) == INTERVALS_PER_DAY
    assert 999.0 not in set(frame["rrp"])
    assert set(frame["intervention"]) == {0}


def test_price_flags_negative_and_shares_the_day_boundary(tmp_path):
    day = date(2026, 10, 12)
    rows = full_day_price(day)
    rows[0] = (rows[0][0], "SA1", 0, -45.2, "neg.zip", INGESTED)
    silver = build_one(tmp_path, price=rows)
    frame = read(silver_path(silver, "price", day))

    assert frame["is_negative"].sum() == 1
    assert str(frame.iloc[0]["interval_start_nem"])[:16] == "2026-10-12 00:00"
    assert str(frame.iloc[-1]["interval_end_nem"])[:16] == "2026-10-13 00:00"


def test_price_and_wind_align_on_interval_start(tmp_path):
    # Gold will join these two; the timestamps must line up exactly.
    day = date(2026, 10, 12)
    silver = build_one(tmp_path, scada=full_day(day), price=full_day_price(day))

    con = duckdb.connect()
    try:
        (matched,) = con.execute(
            f"""SELECT count(*) FROM read_parquet('{silver_path(silver, "wind_output", day)}') w
                JOIN read_parquet('{silver_path(silver, "price", day)}') p
                  ON w.interval_start_utc = p.interval_start_utc AND w.region = p.region"""
        ).fetchone()
    finally:
        con.close()

    assert matched == INTERVALS_PER_DAY


# --- guards ---------------------------------------------------------------


def test_registry_without_in_scada_is_rejected(tmp_path):
    bronze = tmp_path / "bronze"
    write_scada(bronze, full_day(date(2026, 10, 12)))
    stale = write_registry(
        tmp_path, "duid,station_name,region,fuel_type,capacity_mw\nWFA1,A,SA1,Wind,1\n"
    )

    with pytest.raises(SilverError, match="no in_scada column"):
        build(bronze_dir=bronze, silver_dir=tmp_path / "silver", registry=stale)


def test_missing_registry_is_rejected(tmp_path):
    with pytest.raises(SilverError, match="no wind DUID registry"):
        build(bronze_dir=tmp_path, silver_dir=tmp_path, registry=tmp_path / "absent.csv")
