"""Tests for the wind DUID registry extractor.

The registration list is an AEMO spreadsheet whose layout shifts between
editions, so these tests exercise the things that actually break: the header
not being on the first row, columns being renamed, and rows that are not wind.
"""

import io
import json

import pytest

openpyxl = pytest.importorskip("openpyxl", reason="reference extras not installed")
pytest.importorskip("pandas", reason="reference extras not installed")

from refresh_reference import (  # noqa: E402
    RegistryError,
    extract_wind,
    normalise_capacity,
    normalise_region,
    write_csv,
    write_meta,
)

HEADERS = [
    "Participant",
    "Station Name",
    "Region",
    "Dispatch Type",
    "Fuel Source - Descriptor",
    "DUID",
    "Reg Cap (MW)",
]

ROWS = [
    ["Acme Energy", "Example Wind Farm", "SA1", "Generator", "Wind", "EXWF1", 150.0],
    ["Acme Energy", "Example Wind Farm", "SA1", "Generator", "Wind", "EXWF2", 90.5],
    ["Beta Power", "Coal Creek", "NSW1", "Generator", "Black Coal", "COAL1", 700.0],
    ["Gamma Co", "Sunny Solar", "QLD1", "Generator", "Solar", "SOLAR1", 200.0],
    ["Delta Wind", "Hill Wind", "VIC1", "Generator", "Wind - Onshore", "HILLWF1", 320.0],
    ["Epsilon", "Island Wind", "TAS1", "Generator", "WIND", "ISLWF1", ""],
    ["Zeta", "Offshore Trial", "", "Generator", "Wind", "NOREGION1", 50.0],
    ["Eta", "Battery", "SA1", "Load", "Battery storage", "BATT1", 100.0],
]


def workbook_bytes(headers=HEADERS, rows=ROWS, sheet="Generators and Scheduled Loads") -> bytes:
    """Build a workbook shaped like AEMO's: title rows above the real header."""
    book = openpyxl.Workbook()
    page = book.active
    page.title = sheet
    page.append(["NEM Registration and Exemption List"])  # title
    page.append([])  # blank spacer
    page.append(headers)
    for row in rows:
        page.append(row)

    # A second sheet, so sheet selection is actually exercised.
    book.create_sheet("Notes").append(["not the table you want"])

    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


def test_extracts_only_wind_rows():
    rows = extract_wind(workbook_bytes())
    assert [row["duid"] for row in rows] == ["EXWF1", "EXWF2", "HILLWF1", "ISLWF1"]


def test_excludes_coal_solar_and_storage():
    duids = {row["duid"] for row in extract_wind(workbook_bytes())}
    assert duids.isdisjoint({"COAL1", "SOLAR1", "BATT1"})


def test_drops_rows_without_a_nem_region():
    # A wind row with no region cannot be joined to a price, so it is dropped.
    assert "NOREGION1" not in {row["duid"] for row in extract_wind(workbook_bytes())}


def test_captures_station_region_and_capacity():
    row = next(r for r in extract_wind(workbook_bytes()) if r["duid"] == "HILLWF1")
    assert row["station_name"] == "Hill Wind"
    assert row["region"] == "VIC1"
    assert row["fuel_type"] == "Wind - Onshore"
    assert row["capacity_mw"] == 320.0


def test_missing_capacity_becomes_none_not_zero():
    # Zero would read as a real 0 MW farm and silently break capacity factors.
    row = next(r for r in extract_wind(workbook_bytes()) if r["duid"] == "ISLWF1")
    assert row["capacity_mw"] is None


def test_matches_fuel_case_insensitively():
    assert "ISLWF1" in {row["duid"] for row in extract_wind(workbook_bytes())}  # "WIND"


def test_finds_the_header_even_when_columns_are_renamed():
    headers = list(HEADERS)
    headers[4] = "Technology Type - Primary"  # an older edition's spelling
    headers[6] = "Registered Capacity (MW)"
    rows = extract_wind(workbook_bytes(headers=headers))
    assert len(rows) == 4


def test_reports_what_it_saw_when_a_column_is_missing():
    headers = [h for h in HEADERS if h != "Reg Cap (MW)"] + ["Something Else"]
    rows = [row[:6] + ["x"] for row in ROWS]
    with pytest.raises(RegistryError, match="columns not found"):
        extract_wind(workbook_bytes(headers=headers, rows=rows))


def test_rejects_a_workbook_with_no_wind():
    rows = [r for r in ROWS if "Wind" not in str(r[4]) and "WIND" not in str(r[4])]
    with pytest.raises(RegistryError, match="no wind rows"):
        extract_wind(workbook_bytes(rows=rows))


def test_rejects_a_workbook_with_no_duid_column():
    with pytest.raises(RegistryError, match="no sheet looks like|no header row"):
        extract_wind(workbook_bytes(headers=["A", "B", "C"], rows=[[1, 2, 3]], sheet="Notes"))


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("SA1", "SA1"), ("sa1", "SA1"), (" NSW1 ", "NSW1"), ("VIC", "VIC1"), ("WA", None), ("", None)],
)
def test_normalise_region(raw, expected):
    assert normalise_region(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("150", 150.0), ("150.5 MW", 150.5), ("", None), ("-", None), ("0", None), ("n/a", None)],
)
def test_normalise_capacity(raw, expected):
    assert normalise_capacity(raw) == expected


def test_csv_is_written_sorted_with_a_stable_header(tmp_path):
    destination = tmp_path / "wind_duids.csv"
    write_csv(extract_wind(workbook_bytes()), destination)

    lines = destination.read_text().splitlines()
    assert lines[0] == "duid,station_name,region,fuel_type,capacity_mw"
    assert [line.split(",")[0] for line in lines[1:]] == ["EXWF1", "EXWF2", "HILLWF1", "ISLWF1"]


def test_meta_records_provenance(tmp_path):
    payload = workbook_bytes()
    rows = extract_wind(payload)
    destination = tmp_path / "wind_duids.meta.json"
    write_meta(rows, payload, "https://example.invalid/list.xls", destination)

    meta = json.loads(destination.read_text())
    assert meta["wind_duids"] == 4
    assert meta["regions"] == ["SA1", "TAS1", "VIC1"]
    assert meta["total_capacity_mw"] == 560.5  # ISLWF1 has none
    assert len(meta["source_sha256"]) == 64
    assert meta["source_url"].startswith("https://")
