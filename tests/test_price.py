"""Tests for the dispatch spot price (DISPATCHIS) ingester."""

from datetime import datetime

import duckdb
import pytest
from ingest_dispatchis import DISPATCHIS, PRICE_TABLE
from nemweb import (
    NEM_TZ,
    ParseError,
    bronze_path,
    extract_rows,
    hour_of,
    interval_from_filename,
)

SOURCE_FILE = "PUBLIC_DISPATCHIS_20261013000500_0000000123456789.zip"


@pytest.fixture
def price_csv(fixtures):
    return (fixtures / "sample_dispatchis.csv").read_text()


def _by_region(rows):
    return {row[1]: row for row in rows}


def test_selects_only_the_price_table(price_csv):
    rows = extract_rows(PRICE_TABLE, price_csv, SOURCE_FILE)
    # CASE_SOLUTION and REGIONSUM share the file and must not be picked up.
    assert len(rows) == 5
    assert set(_by_region(rows)) == {"SA1", "VIC1", "NSW1", "QLD1", "TAS1"}


def test_maps_declared_columns_and_ignores_the_rest(price_csv):
    rows = extract_rows(PRICE_TABLE, price_csv, SOURCE_FILE)
    settlement, regionid, intervention, runno, interval, rrp = _by_region(rows)["VIC1"][:6]

    assert settlement == datetime(2026, 10, 13, 0, 5, tzinfo=NEM_TZ)
    assert regionid == "VIC1"
    assert intervention == 0
    assert runno == 1
    assert interval == "20261013001"
    assert rrp == pytest.approx(88.4)
    # The FCAS columns are present in the fixture but deliberately not ingested.
    assert len(rows[0]) == len(PRICE_TABLE.columns) + 2


def test_keeps_negative_prices(price_csv):
    rows = extract_rows(PRICE_TABLE, price_csv, SOURCE_FILE)
    # Negative prices are real and matter most to wind: they are when a farm is
    # paying to generate. Never clamp them to zero.
    assert _by_region(rows)["SA1"][5] == pytest.approx(-45.21)


def test_keeps_intervention_and_flag_rows(price_csv):
    qld = _by_region(extract_rows(PRICE_TABLE, price_csv, SOURCE_FILE))["QLD1"]
    assert qld[2] == 1  # intervention run, kept for silver to resolve
    assert qld[5] == pytest.approx(15000.0)  # at the market price cap
    assert qld[8] == 1  # apcflag
    assert qld[9] == 1  # marketsuspendedflag


def test_blank_price_is_missing_not_zero(price_csv):
    assert _by_region(extract_rows(PRICE_TABLE, price_csv, SOURCE_FILE))["TAS1"][5] is None


def test_rejects_a_report_without_the_price_table():
    with pytest.raises(ParseError, match="no DISPATCH_PRICE rows"):
        extract_rows(PRICE_TABLE, 'C,"END OF REPORT",0\n', "empty.zip")


def test_price_and_scada_land_in_separate_bronze_trees(tmp_path):
    hour = hour_of(interval_from_filename(DISPATCHIS, SOURCE_FILE))
    path = bronze_path(PRICE_TABLE, hour, tmp_path)
    assert path.relative_to(tmp_path).parts == ("price", "date=2026-10-13", "hour=00.parquet")


def test_interval_from_filename_rejects_the_other_report():
    with pytest.raises(ParseError, match="not a PUBLIC_DISPATCHIS file name"):
        interval_from_filename(DISPATCHIS, "PUBLIC_DISPATCHSCADA_20261013000500_1.zip")


def test_write_roundtrips_through_parquet(price_csv, tmp_path):
    from nemweb import write_bronze

    rows = extract_rows(PRICE_TABLE, price_csv, SOURCE_FILE)
    destination = tmp_path / "price.parquet"
    assert write_bronze(PRICE_TABLE, rows, destination) == 5

    con = duckdb.connect()
    try:
        regions, min_rrp, nulls = con.execute(
            "SELECT count(DISTINCT regionid), min(rrp), count(*) FILTER (WHERE rrp IS NULL) "
            f"FROM read_parquet('{destination}')"
        ).fetchone()
    finally:
        con.close()

    assert regions == 5
    assert min_rrp == pytest.approx(-45.21)
    assert nulls == 1
