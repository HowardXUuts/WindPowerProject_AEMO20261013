"""Shared ingestion core for AEMO NEMWeb reports.

Every NEMWeb report is published the same way: a directory of zipped CSVs, one
file per five-minute interval, each CSV packing several tables behind C/I/D row
markers. This module holds everything that does not vary between reports —
listing, downloading, parsing, the watermark, and the Parquet write — so adding
a report is a matter of declaring a ``ReportSpec``.

Each source file produces exactly one Parquet file, which is never rewritten.
Git history is append-only, so a file rewritten on every run leaves a permanent
blob behind each time; writing once per source interval keeps the repository
growing at roughly the rate of the data itself.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Final

import duckdb
import httpx

LOG: Final = logging.getLogger("nemweb")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
BRONZE_DIR: Final = REPO_ROOT / "data" / "bronze"
WATERMARK_NAME: Final = "_ingested.txt"

# AEMO publishes SETTLEMENTDATE in NEM time: AEST, UTC+10, no daylight saving.
NEM_TZ: Final = timezone(timedelta(hours=10), name="AEST")

SETTLEMENT_FORMATS: Final = ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S")

HTTP_TIMEOUT: Final = httpx.Timeout(30.0, connect=15.0)
USER_AGENT: Final = "aemo-wind/0.1 (+github.com/HowardXUuts/WindPowerProject_AEMO20261013)"


class ParseError(Exception):
    """The AEMO CSV payload could not be interpreted."""


class ListingError(Exception):
    """The AEMO directory listing could not be understood."""


# ---------------------------------------------------------------------------
# Report declaration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Column:
    """One bronze column, and how to get it from an AEMO source column.

    ``optional`` is for columns AEMO added at some point in the report's life
    and may not emit: an optional column that is absent becomes NULL and is
    logged, rather than failing the file. Everything else is required, because
    a silently missing column is a column of nulls nobody notices.
    """

    name: str
    sql_type: str
    source: str
    parse: Callable[[str], object]
    optional: bool = False


@dataclass(frozen=True)
class Horizon:
    """Keep only the nearest ``periods`` intervals of each forecast run.

    PredispatchIS publishes out to about 40 hours, which is 79 half-hour
    periods per run every half hour. The model only uses the near horizon, and
    keeping the rest would cost twenty times the rows for features nothing
    reads.
    """

    run_time: str  # bronze column holding when AEMO issued the run
    interval: str  # bronze column holding the interval the row forecasts
    periods: int


@dataclass(frozen=True)
class TableSpec:
    """One table inside a report, and the bronze table it becomes."""

    key: str  # bronze subdirectory and watermark name, e.g. "price"
    table: str  # table inside the report, e.g. "DISPATCH_PRICE"
    columns: tuple[Column, ...]
    horizon: Horizon | None = None


@dataclass(frozen=True)
class ReportSpec:
    """One NEMWeb report: a directory of zips, and the tables taken from them.

    A report can yield several bronze tables. DispatchIS carries the regional
    price, the regional demand summary and the interconnector flows in a single
    zip, so they are declared together and the zip is downloaded once. Each
    table still keeps its own watermark and its own bronze directory, so one
    falling behind or failing to parse never holds up another.
    """

    url: str
    file_prefix: str  # e.g. "PUBLIC_DISPATCHSCADA"
    tables: tuple[TableSpec, ...]

    @property
    def keys(self) -> tuple[str, ...]:
        return tuple(table.key for table in self.tables)

    @property
    def filename_re(self) -> re.Pattern[str]:
        # Dispatch reports stamp the interval as YYYYMMDDHHMM; some NEMWeb
        # reports add seconds. Accept either, longest first.
        return re.compile(
            rf"{self.file_prefix}_(\d{{14}}|\d{{12}})_\d+\.zip", re.IGNORECASE
        )


# ---------------------------------------------------------------------------
# Field parsers
# ---------------------------------------------------------------------------


def as_text(value: str) -> str | None:
    """Return a trimmed, unquoted string, or None when blank."""
    cleaned = value.strip().strip('"').strip()
    return cleaned or None


def as_float(value: str) -> float | None:
    """Parse a numeric field, treating a blank as missing rather than zero."""
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError as exc:
        raise ParseError(f"not a number: {value!r}") from exc


def as_int(value: str) -> int | None:
    """Parse an integer flag field, treating a blank as missing."""
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return int(float(cleaned))  # AEMO writes some flags as "0.0"
    except ValueError as exc:
        raise ParseError(f"not an integer: {value!r}") from exc


def as_settlement_date(value: str) -> datetime:
    """Parse an AEMO SETTLEMENTDATE into a timezone-aware NEM-time datetime."""
    cleaned = value.strip().strip('"')
    for fmt in SETTLEMENT_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).replace(tzinfo=NEM_TZ)
        except ValueError:
            continue
    raise ParseError(f"unrecognised SETTLEMENTDATE: {value!r}")


# ---------------------------------------------------------------------------
# AEMO CSV parsing
# ---------------------------------------------------------------------------


def parse_aemo_csv(text: str) -> Iterator[tuple[str, dict[str, str]]]:
    """Yield ``(table, row)`` pairs for every D row in an AEMO nested CSV.

    AEMO reports pack several tables into one file, distinguished by a marker in
    the first column: C rows are header and footer metadata, I rows declare the
    column names for a table, and D rows carry the data. An I row applies to
    every following D row sharing its category, subcategory and version, so
    headers have to be tracked as the file is read.
    """
    headers: dict[tuple[str, str, str], list[str]] = {}

    for row in csv.reader(io.StringIO(text)):
        if not row or not row[0].strip():
            continue

        marker = row[0].strip().upper()
        if marker == "C":  # report metadata and "END OF REPORT"
            continue
        if marker not in {"I", "D"} or len(row) < 4:
            LOG.debug("skipping unrecognised row: %.60s", ",".join(row))
            continue

        key = (row[1].strip(), row[2].strip(), row[3].strip())

        if marker == "I":
            headers[key] = [field.strip() for field in row[4:]]
            continue

        header = headers.get(key)
        if header is None:
            # A D row before its I row means the file is truncated or corrupt.
            raise ParseError(f"D row for {key} has no preceding I row")

        values = row[4:]
        if len(values) < len(header):
            # AEMO sometimes omits trailing empty fields.
            values = values + [""] * (len(header) - len(values))

        yield f"{key[0]}_{key[1]}", dict(zip(header, values[: len(header)], strict=True))


def extract_rows(spec: TableSpec, text: str, source_file: str) -> list[tuple]:
    """Return bronze rows for one table of a report."""
    ingested_at = datetime.now(UTC)
    rows: list[tuple] = []
    absent: set[str] = set()

    for table, record in parse_aemo_csv(text):
        if table.upper() != spec.table:
            continue
        values = []
        for column in spec.columns:
            if column.source in record:
                values.append(column.parse(record[column.source]))
            elif column.optional:
                absent.add(column.source)
                values.append(None)
            else:
                # Naming the columns AEMO did send turns a wrong guess at a
                # column name into a one-line fix instead of an investigation.
                raise ParseError(
                    f"{spec.table} in {source_file} has no column {column.source!r}. "
                    f"It has: {', '.join(sorted(record))}"
                )
        rows.append((*values, source_file, ingested_at))

    if absent:
        LOG.warning(
            "[%s] optional column(s) absent from %s, written as NULL: %s",
            spec.key, source_file, ", ".join(sorted(absent)),
        )
    if not rows:
        raise ParseError(f"no {spec.table} rows found in {source_file}")
    return apply_horizon(spec, rows)


def apply_horizon(spec: TableSpec, rows: list[tuple]) -> list[tuple]:
    """Trim each forecast run to the nearest ``periods`` intervals it covers.

    The trim is per run, not per file: one PredispatchIS file holds exactly one
    run, but keeping the rule per run means a file that ever held two is still
    trimmed correctly rather than losing the second run's near horizon.
    """
    horizon = spec.horizon
    if horizon is None:
        return rows

    names = [column.name for column in spec.columns]
    try:
        run_at = names.index(horizon.run_time)
        interval_at = names.index(horizon.interval)
    except ValueError as exc:  # a horizon naming a column the table lacks
        raise ParseError(f"{spec.key}: horizon column not in table: {exc}") from exc

    keep_by_run: dict[object, set] = {}
    for row in rows:
        keep_by_run.setdefault(row[run_at], set()).add(row[interval_at])
    nearest = {
        run: set(sorted(intervals)[: horizon.periods])
        for run, intervals in keep_by_run.items()
    }
    return [row for row in rows if row[interval_at] in nearest[row[run_at]]]


def read_zip_payload(payload: bytes, source_file: str) -> str:
    """Extract the single CSV member of an AEMO zip archive."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        members = [name for name in archive.namelist() if name.upper().endswith(".CSV")]
        if len(members) != 1:
            raise ParseError(f"expected one CSV in {source_file}, found {members}")
        return archive.read(members[0]).decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------


def watermark_path(spec: TableSpec, bronze_dir: Path = BRONZE_DIR) -> Path:
    """Return the watermark path for one bronze table.

    Each table keeps its own watermark, so one source failing or falling behind
    never blocks another — including two tables that share a download.
    """
    return bronze_dir / spec.key / WATERMARK_NAME


def load_watermark(path: Path) -> set[str]:
    """Return the set of source file names already ingested."""
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.strip()}


def append_watermark(names: list[str], path: Path) -> None:
    """Record source file names as ingested.

    Appended only after the Parquet write succeeds, so a crashed run retries the
    file on its next pass rather than skipping it forever.
    """
    if not names:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        for name in sorted(names):
            handle.write(f"{name}\n")


# ---------------------------------------------------------------------------
# Remote listing and download
# ---------------------------------------------------------------------------


def interval_from_filename(spec: ReportSpec, name: str) -> datetime:
    """Parse the interval timestamp embedded in an AEMO file name (NEM time)."""
    match = spec.filename_re.search(name)
    if match is None:
        raise ParseError(f"not a {spec.file_prefix} file name: {name!r}")
    stamp = match.group(1)
    fmt = "%Y%m%d%H%M%S" if len(stamp) == 14 else "%Y%m%d%H%M"
    return datetime.strptime(stamp, fmt).replace(tzinfo=NEM_TZ)


def list_remote_files(spec: ReportSpec, client: httpx.Client) -> list[str]:
    """Return the report's zip file names in the AEMO directory listing.

    A 200 response with no matching file names means the listing format moved
    out from under the pattern, not that AEMO published nothing: ``Current/``
    always holds about two days of files. Treating that as "nothing to ingest"
    produces a green run that silently ingests zero rows, so it raises instead.
    """
    response = client.get(spec.url)
    response.raise_for_status()
    names = sorted({match.group(0) for match in spec.filename_re.finditer(response.text)})
    if not names:
        excerpt = " ".join(response.text.split())[:200]
        raise ListingError(
            f"no {spec.file_prefix} file names in {spec.url} "
            f"({len(response.text)} bytes). First 200 chars: {excerpt!r}"
        )
    return names


def download(spec: ReportSpec, client: httpx.Client, name: str) -> bytes:
    """Download one source file."""
    response = client.get(spec.url + name)
    response.raise_for_status()
    return response.content


# ---------------------------------------------------------------------------
# Bronze write
# ---------------------------------------------------------------------------


def bronze_ddl(spec: TableSpec) -> str:
    """Return the CREATE TABLE statement for a report's bronze schema."""
    columns = [f"{column.name} {column.sql_type}" for column in spec.columns]
    columns += ["source_file VARCHAR", "ingested_at TIMESTAMPTZ"]
    return "CREATE OR REPLACE TABLE bronze_rows (" + ", ".join(columns) + ")"


def hour_of(moment: datetime) -> datetime:
    """Truncate a NEM-time instant to the start of its hour."""
    return moment.replace(minute=0, second=0, microsecond=0)


def bronze_path(spec: TableSpec, hour: datetime, bronze_dir: Path = BRONZE_DIR) -> Path:
    """Return the Parquet path for one closed NEM hour."""
    partition = f"date={hour.date().isoformat()}"
    return bronze_dir / spec.key / partition / f"hour={hour:%H}.parquet"


def write_bronze(spec: TableSpec, rows: list[tuple], destination: Path) -> int:
    """Write bronze rows to Parquet and return the row count."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    literal = str(destination).replace("'", "''")
    placeholders = ", ".join("?" * (len(spec.columns) + 2))
    order_by = ", ".join(column.name for column in spec.columns[:2])

    con = duckdb.connect()
    try:
        con.execute(bronze_ddl(spec))
        con.executemany(f"INSERT INTO bronze_rows VALUES ({placeholders})", rows)
        con.execute(
            f"COPY (SELECT * FROM bronze_rows ORDER BY {order_by}) "
            f"TO '{literal}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    return len(rows)


def ingest_hour(
    spec: TableSpec,
    hour: datetime,
    payloads: list[tuple[str, bytes]],
    bronze_dir: Path = BRONZE_DIR,
) -> tuple[Path, int]:
    """Parse one closed hour's archives and write a single Parquet file."""
    rows: list[tuple] = []
    for source_file, payload in payloads:
        text = read_zip_payload(payload, source_file)
        rows.extend(extract_rows(spec, text, source_file))

    destination = bronze_path(spec, hour, bronze_dir)
    return destination, write_bronze(spec, rows, destination)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def pending_files(spec: ReportSpec, available: list[str], ingested: set[str]) -> list[str]:
    """Return the files not yet ingested, oldest interval first."""
    return sorted(
        set(available) - ingested,
        key=lambda name: interval_from_filename(spec, name),
    )


def group_by_hour(spec: ReportSpec, names: list[str]) -> dict[datetime, list[str]]:
    """Group source file names by the NEM hour their interval falls in."""
    groups: dict[datetime, list[str]] = {}
    for name in names:
        groups.setdefault(hour_of(interval_from_filename(spec, name)), []).append(name)
    return {hour: sorted(files) for hour, files in sorted(groups.items())}


def select_closed_hours(
    spec: ReportSpec,
    pending: list[str],
    available: list[str],
    limit: int | None = None,
) -> list[tuple[datetime, list[str]]]:
    """Return whole, closed hours to ingest, oldest first.

    An hour is closed once AEMO has published an interval in a later hour, so
    no further file can land in it. Only closed hours are written, and always
    in full: a half-written hour would have to be rewritten when the rest
    arrived, and rewriting is what makes a committed-data repository grow
    without bound. The clock is never consulted — the listing itself says how
    far AEMO has got.

    ``limit`` caps the number of source files, rounded down to whole hours, so
    a cap can never split an hour across two runs. One hour is always returned
    if any is ready, even when it holds more files than the limit.
    """
    if not pending:
        return []

    latest = max(interval_from_filename(spec, name) for name in available)
    selected: list[tuple[datetime, list[str]]] = []
    taken = 0

    for hour, files in group_by_hour(spec, pending).items():
        if hour >= hour_of(latest):
            break  # the hour AEMO is still publishing into
        if limit is not None and selected and taken + len(files) > limit:
            break
        selected.append((hour, files))
        taken += len(files)

    return selected


def stale_hours(spec: ReportSpec, available: list[str], ingested: set[str]) -> list[datetime]:
    """Closed NEM hours AEMO still lists but bronze has not ingested, oldest first.

    The hour AEMO is currently publishing into is not stale: it is simply not
    closed yet. Anything older than that should have been written.
    """
    outstanding = set(available) - ingested
    if not outstanding:
        return []
    open_hour = hour_of(max(interval_from_filename(spec, name) for name in available))
    return sorted(
        {
            hour
            for hour in (hour_of(interval_from_filename(spec, name)) for name in outstanding)
            if hour < open_hour
        }
    )


def run(
    spec: ReportSpec,
    limit: int | None = None,
    list_only: bool = False,
    bronze_dir: Path = BRONZE_DIR,
) -> int:
    """Ingest all closed, un-ingested hours for every table. Returns hours written.

    One pass over the report's files serves every table it declares. The zip is
    downloaded and unzipped once per source file; each table then decides for
    itself, from its own watermark, whether it still needs that hour. A table
    that fails to parse is logged and skipped without disturbing the others,
    because they do not share a watermark — only a download.
    """
    state = {table.key: load_watermark(watermark_path(table, bronze_dir)) for table in spec.tables}
    for table in spec.tables:
        LOG.info("[%s] watermark holds %d previously ingested files", table.key,
                 len(state[table.key]))

    headers = {"User-Agent": USER_AGENT}
    with httpx.Client(timeout=HTTP_TIMEOUT, headers=headers, follow_redirects=True) as client:
        available = list_remote_files(spec, client)

        wanted: dict[str, list[tuple[datetime, list[str]]]] = {}
        for table in spec.tables:
            pending = pending_files(spec, available, state[table.key])
            wanted[table.key] = select_closed_hours(spec, pending, available, limit)
            LOG.info("[%s] %d listed, %d pending, %d closed hours selected",
                     table.key, len(available), len(pending), len(wanted[table.key]))

        if list_only:
            for table in spec.tables:
                for hour, files in wanted[table.key]:
                    print(f"{table.key:16} {hour:%Y-%m-%d %H}:00  {len(files)} files")
            return 0

        # The union of what the tables want, so a file is fetched once even when
        # two tables are at different points in the backlog.
        hours: dict[datetime, list[str]] = {}
        for selected in wanted.values():
            for hour, files in selected:
                hours.setdefault(hour, files)
        if not hours:
            LOG.info("[%s] nothing to ingest", spec.file_prefix)
            return 0

        done: dict[str, list[str]] = {table.key: [] for table in spec.tables}
        rows_by_key: dict[str, int] = dict.fromkeys(done, 0)
        written = 0

        for hour in sorted(hours):
            files = hours[hour]
            due = [
                table for table in spec.tables
                if any(hour == h for h, _ in wanted[table.key])
                and not bronze_path(table, hour, bronze_dir).exists()
            ]
            for table in spec.tables:
                if table not in due and any(hour == h for h, _ in wanted[table.key]):
                    # Write-once: the file exists, so this hour is already done.
                    LOG.warning("[%s] %s exists, skipping", table.key,
                                bronze_path(table, hour, bronze_dir).name)
                    done[table.key].extend(files)
            if not due:
                continue

            try:
                texts = [
                    (name, read_zip_payload(download(spec, client, name), name))
                    for name in files
                ]
            except (httpx.HTTPError, zipfile.BadZipFile, ParseError) as exc:
                # The download is shared, so a failure here costs every table
                # this hour. They retry it together on the next run.
                LOG.warning("[%s] skipping hour %s: %s", spec.file_prefix,
                            f"{hour:%Y-%m-%d %H}", exc)
                continue

            hour_written = False
            for table in due:
                try:
                    rows: list[tuple] = []
                    for name, text in texts:
                        rows.extend(extract_rows(table, text, name))
                    destination = bronze_path(table, hour, bronze_dir)
                    count = write_bronze(table, rows, destination)
                except ParseError as exc:
                    # One table's schema moving must not cost the others their
                    # hour: AEMO retains about two days and the data is gone.
                    LOG.warning("[%s] skipping hour %s: %s", table.key,
                                f"{hour:%Y-%m-%d %H}", exc)
                    continue

                LOG.info("[%s] wrote %s from %d files (%d rows)", table.key,
                         destination.relative_to(bronze_dir), len(files), count)
                done[table.key].extend(files)
                rows_by_key[table.key] += count
                hour_written = True
            if hour_written:
                written += 1

        for table in spec.tables:
            stale = stale_hours(spec, available, state[table.key] | set(done[table.key]))
            if stale:
                # The failure this catches is silent by nature: a run that writes
                # one hour and leaves ten behind exits 0 and commits real data.
                # AEMO retains about two days, so hours left in a closed window
                # are on a clock, and the only symptom is a log line nobody reads
                # unless it says so.
                LOG.warning(
                    "[%s] %d closed hour(s) left un-ingested, oldest %s. "
                    "AEMO retains about two days; these will be lost if not picked up.",
                    table.key, len(stale), f"{stale[0]:%Y-%m-%d %H}:00",
                )

    for table in spec.tables:
        append_watermark(done[table.key], watermark_path(table, bronze_dir))
        LOG.info("[%s] wrote %d files, %d rows", table.key,
                 len(done[table.key]), rows_by_key[table.key])
    return written

