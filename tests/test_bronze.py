"""Tests for the bronze write, hour selection, and the watermark."""

import io
import zipfile
from datetime import datetime

import duckdb
import pytest
from ingest_scada import SCADA
from nemweb import (
    NEM_TZ,
    append_watermark,
    bronze_path,
    extract_rows,
    group_by_hour,
    hour_of,
    ingest_hour,
    load_watermark,
    pending_files,
    select_closed_hours,
    stale_hours,
    watermark_path,
    write_bronze,
)

SOURCE_FILE = "PUBLIC_DISPATCHSCADA_202610130005_0000000123456789.zip"
HOUR = datetime(2026, 10, 13, 0, tzinfo=NEM_TZ)


def name_at(hhmm: str, seq: int = 1) -> str:
    return f"PUBLIC_DISPATCHSCADA_20261013{hhmm}_{seq}.zip"


def zip_payload(csv_text: str, member: str = "PUBLIC_DISPATCHSCADA_202610130005.CSV") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, csv_text)
    return buffer.getvalue()


# --- paths ----------------------------------------------------------------


def test_bronze_path_is_one_file_per_hour(tmp_path):
    path = bronze_path(SCADA, HOUR, tmp_path)
    assert path.relative_to(tmp_path).parts == ("scada", "date=2026-10-13", "hour=00.parquet")


def test_every_interval_in_an_hour_maps_to_one_path(tmp_path):
    paths = {
        bronze_path(SCADA, hour_of(datetime(2026, 10, 13, 7, m, tzinfo=NEM_TZ)), tmp_path)
        for m in (0, 5, 30, 55)
    }
    assert len(paths) == 1


# --- writing --------------------------------------------------------------


def test_write_bronze_roundtrips_through_parquet(sample_csv, tmp_path):
    rows = extract_rows(SCADA, sample_csv, SOURCE_FILE)
    destination = tmp_path / "hour=00.parquet"

    assert write_bronze(SCADA, rows, destination) == 6

    con = duckdb.connect()
    try:
        count, duids, nulls = con.execute(
            "SELECT count(*), count(DISTINCT duid), count(*) FILTER (WHERE scadavalue IS NULL) "
            f"FROM read_parquet('{destination}')"
        ).fetchone()
    finally:
        con.close()

    assert (count, duids, nulls) == (6, 6, 2)


def test_write_bronze_preserves_timezone(sample_csv, tmp_path):
    write_bronze(SCADA, extract_rows(SCADA, sample_csv, SOURCE_FILE), tmp_path / "h.parquet")

    con = duckdb.connect()
    try:
        (utc_text,) = con.execute(
            "SELECT strftime(settlementdate AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M') "
            f"FROM read_parquet('{tmp_path / 'h.parquet'}') LIMIT 1"
        ).fetchone()
    finally:
        con.close()

    assert utc_text == "2026-10-12 14:05"


def test_ingest_hour_concatenates_every_file_in_the_hour(sample_csv, tmp_path):
    payloads = [(name_at(f"00{m:02d}"), zip_payload(sample_csv)) for m in (0, 5, 10)]
    destination, count = ingest_hour(SCADA, HOUR, payloads, tmp_path)

    assert count == 18  # three files x six rows
    assert destination.name == "hour=00.parquet"

    con = duckdb.connect()
    try:
        (sources,) = con.execute(
            f"SELECT count(DISTINCT source_file) FROM read_parquet('{destination}')"
        ).fetchone()
    finally:
        con.close()
    assert sources == 3  # provenance survives the merge


# --- hour grouping and selection -----------------------------------------


def test_group_by_hour_buckets_intervals():
    names = [name_at("0000"), name_at("0055"), name_at("0100")]
    groups = group_by_hour(SCADA, names)

    assert list(groups) == [HOUR, datetime(2026, 10, 13, 1, tzinfo=NEM_TZ)]
    assert len(groups[HOUR]) == 2


def test_only_closed_hours_are_selected():
    # AEMO has published into hour 01, so hour 00 is closed and 01 is not.
    available = [name_at("0000"), name_at("0055"), name_at("0100")]
    selected = select_closed_hours(SCADA, available, available)

    assert [hour for hour, _ in selected] == [HOUR]


def test_the_hour_still_being_published_into_is_never_written():
    # One hour only, still open: nothing is ready, so nothing is written.
    available = [name_at("0000"), name_at("0005")]
    assert select_closed_hours(SCADA, available, available) == []


def test_limit_rounds_down_to_whole_hours():
    available = [name_at(f"{h:02d}{m:02d}") for h in (0, 1, 2) for m in (0, 5, 10)]
    # Hours 00 and 01 are closed (02 is open). A limit of 4 fits one whole
    # hour of 3 files; taking part of the next would split it.
    selected = select_closed_hours(SCADA, available, available, limit=4)

    assert [hour for hour, _ in selected] == [HOUR]


def test_one_hour_is_returned_even_when_larger_than_the_limit():
    available = [name_at(f"00{m:02d}") for m in (0, 5, 10)] + [name_at("0100")]
    selected = select_closed_hours(SCADA, available, available, limit=1)

    # Never stall: a limit smaller than an hour still makes progress.
    assert [hour for hour, _ in selected] == [HOUR]
    assert len(selected[0][1]) == 3


def test_already_ingested_files_are_not_reselected():
    available = [name_at(f"{h:02d}{m:02d}") for h in (0, 1, 2) for m in (0, 5)]
    ingested = {name_at("0000"), name_at("0005")}
    pending = pending_files(SCADA, available, ingested)
    selected = select_closed_hours(SCADA, pending, available)

    assert [hour for hour, _ in selected] == [datetime(2026, 10, 13, 1, tzinfo=NEM_TZ)]


def test_nothing_pending_selects_nothing():
    available = [name_at("0000"), name_at("0100")]
    assert select_closed_hours(SCADA, [], available) == []


# --- falling behind -------------------------------------------------------
#
# A capped run writes real data and exits 0 while leaving closed hours on
# AEMO's two-day clock. Nothing else in the pipeline notices, so these guard
# the only signal there is.


def test_nothing_is_stale_when_every_closed_hour_is_ingested():
    available = [name_at(f"{h:02d}{m:02d}") for h in (0, 1, 2) for m in (0, 5)]
    # Hours 00 and 01 ingested; 02 is the hour AEMO is publishing into.
    ingested = {name_at(f"{h:02d}{m:02d}") for h in (0, 1) for m in (0, 5)}

    assert stale_hours(SCADA, available, ingested) == []


def test_the_open_hour_is_never_stale():
    available = [name_at("0000"), name_at("0005")]
    assert stale_hours(SCADA, available, set()) == []


def test_closed_hours_left_behind_are_reported_oldest_first():
    available = [name_at(f"{h:02d}{m:02d}") for h in (0, 1, 2, 3) for m in (0, 5)]
    ingested = {name_at("0200"), name_at("0205")}

    # Hours 00 and 01 were skipped; 03 is open, so only the two are stale.
    assert stale_hours(SCADA, available, ingested) == [
        HOUR,
        datetime(2026, 10, 13, 1, tzinfo=NEM_TZ),
    ]


def test_a_partially_ingested_hour_is_stale():
    available = [name_at(f"{h:02d}{m:02d}") for h in (0, 1) for m in (0, 5)]
    ingested = {name_at("0000")}

    # The 00:05 file is still outstanding, so the hour is not done.
    assert stale_hours(SCADA, available, ingested) == [HOUR]


# --- watermark ------------------------------------------------------------


def test_watermark_roundtrip(tmp_path):
    path = watermark_path(SCADA, tmp_path)
    assert load_watermark(path) == set()

    append_watermark([SOURCE_FILE], path)
    append_watermark([name_at("0010")], path)

    assert load_watermark(path) == {SOURCE_FILE, name_at("0010")}


def test_watermark_ignores_blank_lines(tmp_path):
    path = watermark_path(SCADA, tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"\n{SOURCE_FILE}\n\n")
    assert load_watermark(path) == {SOURCE_FILE}


def test_append_watermark_of_nothing_creates_no_file(tmp_path):
    path = watermark_path(SCADA, tmp_path)
    append_watermark([], path)
    assert not path.exists()


def test_watermark_covers_every_file_in_a_written_hour(sample_csv, tmp_path):
    # The hour file is the unit of work, but the watermark stays per source
    # file so a changed hour boundary cannot silently re-ingest.
    files = [name_at(f"00{m:02d}") for m in (0, 5, 10)]
    ingest_hour(SCADA, HOUR, [(n, zip_payload(sample_csv)) for n in files], tmp_path)
    append_watermark(files, watermark_path(SCADA, tmp_path))

    assert load_watermark(watermark_path(SCADA, tmp_path)) == set(files)
    assert pending_files(SCADA, files, load_watermark(watermark_path(SCADA, tmp_path))) == []


def test_rerun_produces_the_same_path(sample_csv, tmp_path):
    payloads = [(SOURCE_FILE, zip_payload(sample_csv))]
    first, _ = ingest_hour(SCADA, HOUR, payloads, tmp_path)
    second, _ = ingest_hour(SCADA, HOUR, payloads, tmp_path)
    assert first == second


@pytest.mark.parametrize("minute", [0, 5, 59])
def test_hour_of_truncates(minute):
    moment = datetime(2026, 10, 13, 9, minute, 30, tzinfo=NEM_TZ)
    assert hour_of(moment) == datetime(2026, 10, 13, 9, tzinfo=NEM_TZ)
