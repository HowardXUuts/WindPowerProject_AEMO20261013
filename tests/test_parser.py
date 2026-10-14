"""Tests for the AEMO nested-CSV parser."""


from datetime import UTC, datetime

import pytest
from ingest_scada import SCADA
from nemweb import (
    NEM_TZ,
    ParseError,
    as_float,
    as_settlement_date,
    extract_rows,
    interval_from_filename,
    parse_aemo_csv,
)


def extract_scada(text, source_file):
    """Parse SCADA rows out of a CSV payload, as the ingester does."""
    return extract_rows(SCADA.tables[0], text, source_file)


def test_keeps_only_d_rows(sample_csv):
    rows = list(parse_aemo_csv(sample_csv))
    assert len(rows) == 6  # six D rows; C rows and the I row are not data
    assert all(table == "DISPATCH_UNIT_SCADA" for table, _ in rows)


def test_applies_i_row_header_to_d_rows(sample_csv):
    _, first = next(iter(parse_aemo_csv(sample_csv)))
    assert first == {
        "SETTLEMENTDATE": "2026/10/13 00:05:00",
        "DUID": "ARWF1",
        "SCADAVALUE": "45.13",
    }


def test_tracks_headers_per_table(fixtures):
    text = (fixtures / "sample_two_tables.csv").read_text()
    rows = list(parse_aemo_csv(text))

    tables = [table for table, _ in rows]
    assert tables == [
        "DISPATCH_UNIT_SCADA",
        "DISPATCH_CASE_SOLUTION",
        "DISPATCH_UNIT_SCADA",
    ]
    # The CASE_SOLUTION header must not leak into the later UNIT_SCADA row.
    assert rows[2][1]["DUID"] == "CAPTL_WF"
    assert rows[1][1]["RUNNO"] == "1"


def test_pads_missing_trailing_fields(sample_csv):
    rows = {record["DUID"]: record for _, record in parse_aemo_csv(sample_csv)}
    assert rows["LOYYB1"]["SCADAVALUE"] == ""


def test_orphan_d_row_is_an_error(fixtures):
    text = (fixtures / "sample_orphan_d_row.csv").read_text()
    with pytest.raises(ParseError, match="no preceding I row"):
        list(parse_aemo_csv(text))


def test_extract_unit_scada_types_and_filters(fixtures):
    text = (fixtures / "sample_two_tables.csv").read_text()
    rows = extract_scada(text, "PUBLIC_DISPATCHSCADA_20261013000500_0000000123456789.zip")

    assert len(rows) == 2  # CASE_SOLUTION is excluded
    settlement, duid, value, source_file, ingested_at = rows[0]
    assert settlement == datetime(2026, 10, 13, 0, 5, tzinfo=NEM_TZ)
    assert duid == "ARWF1"
    assert value == pytest.approx(45.13)
    assert source_file.startswith("PUBLIC_DISPATCHSCADA_")
    assert ingested_at.tzinfo is not None


def test_extract_unit_scada_keeps_zero_and_negative(sample_csv):
    values = {duid: value for _, duid, value, _, _ in extract_scada(sample_csv, "f.zip")}
    assert values["BALDHWF1"] == 0.0
    assert values["MACARTH1"] == pytest.approx(-1.2)  # curtailment or own use
    assert values["HDWF2"] is None  # blank is missing, not zero
    assert values["LOYYB1"] is None


def test_extract_unit_scada_rejects_empty_report():
    with pytest.raises(ParseError, match="no DISPATCH_UNIT_SCADA rows"):
        extract_scada('C,"END OF REPORT",0\n', "empty.zip")


@pytest.mark.parametrize("raw", ["2026/10/13 00:05:00", '"2026/10/13 00:05:00"'])
def test_parse_settlement_date_is_nem_time(raw):
    parsed = as_settlement_date(raw)
    assert parsed == datetime(2026, 10, 13, 0, 5, tzinfo=NEM_TZ)
    # AEST is UTC+10 all year; 00:05 NEM time is 14:05 UTC the previous day.
    assert parsed.astimezone(UTC) == datetime(2026, 10, 12, 14, 5, tzinfo=UTC)


def test_parse_settlement_date_rejects_garbage():
    with pytest.raises(ParseError, match="SETTLEMENTDATE"):
        as_settlement_date("13-10-2026")


def test_parse_scada_value_rejects_garbage():
    with pytest.raises(ParseError, match="not a number"):
        as_float("n/a")


def test_interval_from_filename():
    name = "PUBLIC_DISPATCHSCADA_20261013142500_0000000123456789.zip"
    assert interval_from_filename(SCADA, name) == datetime(2026, 10, 13, 14, 25, tzinfo=NEM_TZ)


def test_interval_from_filename_rejects_other_reports():
    with pytest.raises(ParseError, match="not a PUBLIC_DISPATCHSCADA file name"):
        interval_from_filename(SCADA, "PUBLIC_DISPATCHIS_20261013142500_1.zip")
