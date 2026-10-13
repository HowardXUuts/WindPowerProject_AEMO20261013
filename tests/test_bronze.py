"""Tests for the bronze write, the watermark, and pending-file selection."""

import io
import zipfile

import duckdb
from ingest_scada import SCADA
from nemweb import (
    append_watermark,
    bronze_path,
    extract_rows,
    ingest_payload,
    load_watermark,
    pending_files,
    watermark_path,
    write_bronze,
)

SOURCE_FILE = "PUBLIC_DISPATCHSCADA_20261013000500_0000000123456789.zip"


def _zip_payload(csv_text: str, member: str = "PUBLIC_DISPATCHSCADA_20261013000500.CSV") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, csv_text)
    return buffer.getvalue()


def test_bronze_path_is_partitioned_by_nem_date(tmp_path):
    path = bronze_path(SCADA, SOURCE_FILE, tmp_path)
    assert path.parent.name == "date=2026-10-13"
    assert path.name == "PUBLIC_DISPATCHSCADA_20261013000500_0000000123456789.parquet"


def test_write_bronze_roundtrips_through_parquet(sample_csv, tmp_path):
    rows = extract_rows(SCADA, sample_csv, SOURCE_FILE)
    destination = tmp_path / "date=2026-10-13" / "part.parquet"

    assert write_bronze(SCADA, rows, destination) == 6
    assert destination.exists()

    con = duckdb.connect()
    try:
        count, duids, nulls = con.execute(
            "SELECT count(*), count(DISTINCT duid), count(*) FILTER (WHERE scadavalue IS NULL) "
            f"FROM read_parquet('{destination}')"
        ).fetchone()
    finally:
        con.close()

    assert count == 6
    assert duids == 6
    assert nulls == 2  # HDWF2 and LOYYB1


def test_write_bronze_preserves_timezone(sample_csv, tmp_path):
    rows = extract_rows(SCADA, sample_csv, SOURCE_FILE)
    destination = tmp_path / "part.parquet"
    write_bronze(SCADA, rows, destination)

    con = duckdb.connect()
    try:
        # Stored as an instant: read back in UTC it is 14:05 the previous day.
        (utc_text,) = con.execute(
            "SELECT strftime(settlementdate AT TIME ZONE 'UTC', '%Y-%m-%d %H:%M') "
            f"FROM read_parquet('{destination}') LIMIT 1"
        ).fetchone()
    finally:
        con.close()

    assert utc_text == "2026-10-12 14:05"


def test_ingest_payload_writes_one_file_per_source(sample_csv, tmp_path):
    destination, count = ingest_payload(SCADA, _zip_payload(sample_csv), SOURCE_FILE, tmp_path)

    assert count == 6
    assert destination.exists()
    assert destination.relative_to(tmp_path).parts[:2] == ("scada", "date=2026-10-13")


def test_ingest_payload_is_byte_identical_on_rerun(sample_csv, tmp_path):
    payload = _zip_payload(sample_csv)
    first, _ = ingest_payload(SCADA, payload, SOURCE_FILE, tmp_path)
    original = first.read_bytes()

    second, _ = ingest_payload(SCADA, payload, SOURCE_FILE, tmp_path)

    # Same path, and no spurious git diff beyond the ingested_at column.
    assert second == first
    assert len(second.read_bytes()) == len(original)


def test_watermark_roundtrip(tmp_path):
    path = watermark_path(SCADA, tmp_path)
    assert load_watermark(path) == set()

    append_watermark([SOURCE_FILE], path)
    append_watermark(["PUBLIC_DISPATCHSCADA_20261013001000_1.zip"], path)

    assert load_watermark(path) == {
        SOURCE_FILE,
        "PUBLIC_DISPATCHSCADA_20261013001000_1.zip",
    }


def test_watermark_ignores_blank_lines(tmp_path):
    path = watermark_path(SCADA, tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"\n{SOURCE_FILE}\n\n")
    assert load_watermark(path) == {SOURCE_FILE}


def test_append_watermark_of_nothing_creates_no_file(tmp_path):
    path = watermark_path(SCADA, tmp_path)
    append_watermark([], path)
    assert not path.exists()


def test_pending_files_skips_ingested_and_sorts_by_interval():
    available = [
        "PUBLIC_DISPATCHSCADA_20261013001000_2.zip",
        "PUBLIC_DISPATCHSCADA_20261013000500_1.zip",
        "PUBLIC_DISPATCHSCADA_20261013001500_3.zip",
    ]
    ingested = {"PUBLIC_DISPATCHSCADA_20261013001000_2.zip"}

    assert pending_files(SCADA, available, ingested) == [
        "PUBLIC_DISPATCHSCADA_20261013000500_1.zip",
        "PUBLIC_DISPATCHSCADA_20261013001500_3.zip",
    ]


def test_pending_files_respects_limit():
    available = [f"PUBLIC_DISPATCHSCADA_2026101300{i:02d}00_1.zip" for i in range(1, 6)]
    assert pending_files(SCADA, available, set(), limit=2) == available[:2]


def test_pending_files_empty_when_all_ingested():
    available = ["PUBLIC_DISPATCHSCADA_20261013000500_1.zip"]
    assert pending_files(SCADA, available, set(available)) == []
