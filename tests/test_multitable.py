"""Tests for a report that yields several bronze tables from one download.

DispatchIS carries the price, the regional summary and the interconnector flows
in one zip. The three must come out of a single download but stay independent
everywhere else: separate watermarks, separate bronze directories, and one
failing to parse must not cost the others their hour. AEMO retains about two
days, so an hour lost is lost for good.

The fixtures here are hand-written AEMO-shaped CSV, not real report extracts:
the point is the machinery, and the real column names are settled by
scripts/inspect_report.py against the live directory.
"""

import io
import zipfile
from datetime import datetime

import duckdb
import pytest
from nemweb import (
    NEM_TZ,
    Column,
    Horizon,
    ParseError,
    ReportSpec,
    TableSpec,
    as_float,
    as_settlement_date,
    as_text,
    bronze_path,
    extract_rows,
    ingest_hour,
    watermark_path,
    write_bronze,
)

HOUR = datetime(2026, 10, 13, 0, tzinfo=NEM_TZ)
SOURCE = "PUBLIC_TWOTABLE_202610130005_0000000123456789.zip"

ALPHA = TableSpec(
    key="alpha",
    table="REPORT_ALPHA",
    columns=(
        Column("settlementdate", "TIMESTAMPTZ", "SETTLEMENTDATE", as_settlement_date),
        Column("regionid", "VARCHAR", "REGIONID", as_text),
        Column("totaldemand", "DOUBLE", "TOTALDEMAND", as_float),
        Column("ss_wind_uigf", "DOUBLE", "SS_WIND_UIGF", as_float, optional=True),
    ),
)

BETA = TableSpec(
    key="beta",
    table="REPORT_BETA",
    columns=(
        Column("settlementdate", "TIMESTAMPTZ", "SETTLEMENTDATE", as_settlement_date),
        Column("linkid", "VARCHAR", "INTERCONNECTORID", as_text),
        Column("mwflow", "DOUBLE", "MWFLOW", as_float),
    ),
)

REPORT = ReportSpec(
    url="https://example.invalid/Reports/Current/TwoTable/",
    file_prefix="PUBLIC_TWOTABLE",
    tables=(ALPHA, BETA),
)

BOTH_TABLES = """\
C,NEMP,DISPATCH,AEMO,PUBLIC,2026/10/13 00:05:00,1,,
I,REPORT,ALPHA,1,SETTLEMENTDATE,REGIONID,TOTALDEMAND,SS_WIND_UIGF
D,REPORT,ALPHA,1,"2026/10/13 00:05:00",SA1,1234.5,410.2
D,REPORT,ALPHA,1,"2026/10/13 00:05:00",VIC1,4567.8,980.1
I,REPORT,BETA,1,SETTLEMENTDATE,INTERCONNECTORID,MWFLOW
D,REPORT,BETA,1,"2026/10/13 00:05:00",V-SA,-120.5
C,"END OF REPORT",6
"""

WITHOUT_OPTIONAL = """\
I,REPORT,ALPHA,1,SETTLEMENTDATE,REGIONID,TOTALDEMAND
D,REPORT,ALPHA,1,"2026/10/13 00:05:00",SA1,1234.5
C,"END OF REPORT",2
"""


def zipped(text: str, member: str = "PUBLIC_TWOTABLE_202610130005.CSV") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, text)
    return buffer.getvalue()


# --- one payload, several tables ------------------------------------------


def test_each_table_takes_only_its_own_rows():
    alpha = extract_rows(ALPHA, BOTH_TABLES, SOURCE)
    beta = extract_rows(BETA, BOTH_TABLES, SOURCE)

    assert len(alpha) == 2
    assert len(beta) == 1
    assert [row[1] for row in alpha] == ["SA1", "VIC1"]
    assert beta[0][1] == "V-SA"


def test_tables_land_in_separate_bronze_directories(tmp_path):
    assert bronze_path(ALPHA, HOUR, tmp_path) != bronze_path(BETA, HOUR, tmp_path)
    assert "alpha" in str(bronze_path(ALPHA, HOUR, tmp_path))
    assert "beta" in str(bronze_path(BETA, HOUR, tmp_path))


def test_tables_keep_separate_watermarks(tmp_path):
    # One download, but a shared watermark would mean a table that failed to
    # parse was recorded as ingested by its neighbour's success.
    assert watermark_path(ALPHA, tmp_path) != watermark_path(BETA, tmp_path)


def test_a_table_missing_from_the_file_fails_only_itself():
    gamma = TableSpec(key="gamma", table="REPORT_GAMMA", columns=ALPHA.columns)
    # Alpha still parses from the same text that has nothing for gamma.
    assert extract_rows(ALPHA, BOTH_TABLES, SOURCE)
    with pytest.raises(ParseError, match="no REPORT_GAMMA rows"):
        extract_rows(gamma, BOTH_TABLES, SOURCE)


def test_ingest_hour_writes_one_table_per_call(tmp_path):
    payloads = [(SOURCE, zipped(BOTH_TABLES))]
    alpha_path, alpha_rows = ingest_hour(ALPHA, HOUR, payloads, tmp_path)
    beta_path, beta_rows = ingest_hour(BETA, HOUR, payloads, tmp_path)

    assert (alpha_rows, beta_rows) == (2, 1)
    assert alpha_path.exists() and beta_path.exists()
    assert alpha_path != beta_path


# --- optional columns -----------------------------------------------------


def test_an_optional_column_that_is_absent_becomes_null(caplog):
    rows = extract_rows(ALPHA, WITHOUT_OPTIONAL, SOURCE)
    assert rows[0][3] is None


def test_an_absent_optional_column_is_logged_every_time(caplog):
    # A column that is always NULL and never mentioned is the quiet failure
    # this project keeps running into: the run is green and the data is empty.
    with caplog.at_level("WARNING"):
        extract_rows(ALPHA, WITHOUT_OPTIONAL, SOURCE)
    assert "SS_WIND_UIGF" in caplog.text


def test_a_required_column_that_is_absent_names_what_arrived():
    missing_required = WITHOUT_OPTIONAL.replace(",TOTALDEMAND", "").replace(",1234.5", "")
    with pytest.raises(ParseError) as exc:
        extract_rows(ALPHA, missing_required, SOURCE)
    # The error has to carry the real headers, or a wrong guess at a column
    # name costs a round trip to find out what the right one was.
    assert "TOTALDEMAND" in str(exc.value)
    assert "SETTLEMENTDATE" in str(exc.value)
    assert "REGIONID" in str(exc.value)


# --- forecast horizon -----------------------------------------------------

FORECAST = TableSpec(
    key="forecast",
    table="REPORT_FORECAST",
    columns=(
        Column("run_datetime", "TIMESTAMPTZ", "RUN_DATETIME", as_settlement_date),
        Column("interval_datetime", "TIMESTAMPTZ", "INTERVAL_DATETIME", as_settlement_date),
        Column("regionid", "VARCHAR", "REGIONID", as_text),
        Column("rrp", "DOUBLE", "RRP", as_float),
    ),
    horizon=Horizon(run_time="run_datetime", interval="interval_datetime", periods=2),
)


def forecast_csv(runs: int = 1, periods: int = 5, regions: tuple[str, ...] = ("SA1",)) -> str:
    lines = ["I,REPORT,FORECAST,1,RUN_DATETIME,INTERVAL_DATETIME,REGIONID,RRP"]
    for run in range(runs):
        for period in range(periods):
            for region in regions:
                lines.append(
                    f'D,REPORT,FORECAST,1,"2026/10/13 {run:02d}:00:00",'
                    f'"2026/10/13 {run:02d}:{(period + 1) * 5:02d}:00",{region},{50 + period}'
                )
    return "\n".join(lines) + "\n"


def test_horizon_keeps_only_the_nearest_intervals():
    rows = extract_rows(FORECAST, forecast_csv(periods=5), SOURCE)
    kept = sorted({row[1].strftime("%H:%M") for row in rows})
    assert kept == ["00:05", "00:10"], "periods=2 keeps the two nearest intervals"


def test_horizon_keeps_every_region_of_a_kept_interval():
    rows = extract_rows(FORECAST, forecast_csv(periods=5, regions=("SA1", "VIC1")), SOURCE)
    # The trim is by interval, not by row: dropping a region would make the
    # forecast unusable for the region that lost.
    assert len(rows) == 4
    assert {row[2] for row in rows} == {"SA1", "VIC1"}


def test_horizon_is_applied_per_run_not_per_file():
    rows = extract_rows(FORECAST, forecast_csv(runs=2, periods=5), SOURCE)
    by_run: dict[str, set] = {}
    for row in rows:
        by_run.setdefault(row[0].strftime("%H:%M"), set()).add(row[1])
    assert len(by_run) == 2
    assert all(len(intervals) == 2 for intervals in by_run.values())


def test_a_table_without_a_horizon_keeps_everything():
    assert len(extract_rows(ALPHA, BOTH_TABLES, SOURCE)) == 2


def test_a_horizon_naming_an_unknown_column_fails_loudly():
    broken = TableSpec(
        key="broken", table="REPORT_FORECAST", columns=FORECAST.columns,
        horizon=Horizon(run_time="not_a_column", interval="interval_datetime", periods=2),
    )
    with pytest.raises(ParseError, match="horizon column"):
        extract_rows(broken, forecast_csv(), SOURCE)


def test_the_kept_rows_round_trip_through_parquet(tmp_path):
    rows = extract_rows(FORECAST, forecast_csv(periods=5), SOURCE)
    destination = tmp_path / "forecast.parquet"
    assert write_bronze(FORECAST, rows, destination) == 2

    con = duckdb.connect()
    try:
        kept = con.execute(
            f"SELECT count(*), count(DISTINCT run_datetime) FROM read_parquet('{destination}')"
        ).fetchone()
    finally:
        con.close()
    assert kept == (2, 1)


# --- one download serves every table --------------------------------------


class CountingClient:
    """Serves a listing and the same payload for every file, counting requests."""

    def __init__(self, names, payload):
        self.names = names
        self.payload = payload
        self.downloads: list[str] = []
        self.listings = 0

    def get(self, url):
        if url.endswith("/"):
            self.listings += 1
            body = " ".join(f'<A HREF="{name}">{name}</A>' for name in self.names)
            return _Response(body.encode())
        self.downloads.append(url.rsplit("/", 1)[-1])
        return _Response(self.payload)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Response:
    def __init__(self, content: bytes):
        self.content = content

    @property
    def text(self) -> str:
        # Only the listing is ever read as text; a zip payload is not UTF-8.
        return self.content.decode("utf-8", errors="replace")

    def raise_for_status(self):
        return None


def names_for_hour(hour: int, count: int = 2) -> list[str]:
    return [
        f"PUBLIC_TWOTABLE_20261013{hour:02d}{m:02d}_000000000000000{i}.zip"
        for i, m in enumerate(range(0, count * 5, 5))
    ]


def test_two_tables_share_one_download(tmp_path, monkeypatch):
    """The whole reason the tables are declared together.

    Two tables, two bronze files, one fetch per source file — not two.
    """
    import httpx
    import nemweb

    # Hour 00 is closed because hour 01 has been published into.
    available = names_for_hour(0, 2) + names_for_hour(1, 1)
    client = CountingClient(available, zipped(BOTH_TABLES))
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client)

    written = nemweb.run(REPORT, bronze_dir=tmp_path)

    assert written == 1, "one closed hour"
    assert len(client.downloads) == 2, f"one fetch per source file, got {client.downloads}"
    assert bronze_path(ALPHA, HOUR, tmp_path).exists()
    assert bronze_path(BETA, HOUR, tmp_path).exists()


def test_a_table_already_written_does_not_refetch_for_the_other(tmp_path, monkeypatch):
    import httpx
    import nemweb

    available = names_for_hour(0, 2) + names_for_hour(1, 1)
    client = CountingClient(available, zipped(BOTH_TABLES))
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client)

    nemweb.run(REPORT, bronze_dir=tmp_path)
    before = len(client.downloads)
    # Everything is watermarked now, so a second run fetches nothing at all.
    nemweb.run(REPORT, bronze_dir=tmp_path)
    assert len(client.downloads) == before


def test_each_table_watermarks_only_what_it_wrote(tmp_path, monkeypatch):
    import httpx
    import nemweb

    available = names_for_hour(0, 2) + names_for_hour(1, 1)
    client = CountingClient(available, zipped(BOTH_TABLES))
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client)
    nemweb.run(REPORT, bronze_dir=tmp_path)

    alpha_mark = watermark_path(ALPHA, tmp_path).read_text().split()
    beta_mark = watermark_path(BETA, tmp_path).read_text().split()
    assert sorted(alpha_mark) == sorted(beta_mark) == sorted(names_for_hour(0, 2))
