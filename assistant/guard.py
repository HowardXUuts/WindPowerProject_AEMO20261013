"""Validate and execute model-generated SQL against a sandboxed copy of gold.

A question-to-SQL assistant executes text a language model wrote, which is text
an end user influenced. Two independent defences, because either alone is a
single point of failure:

1. **A statement allowlist.** Exactly one statement, and it must be a SELECT or
   a WITH ... SELECT. Everything else is refused before DuckDB sees it.
2. **A sandbox with no filesystem.** Gold is copied into an in-memory database,
   then `enable_external_access` is turned off. DuckDB refuses to turn it back
   on for the life of the connection, so a query that slips past the allowlist
   still cannot read `/etc/passwd`, ATTACH a database, COPY data out, or
   INSTALL an extension.

The sandbox is rebuilt per question and thrown away after, so nothing a query
does to the in-memory copy can reach the next one — or the Parquet files, which
are only ever read.

Gold is a few hundred kilobytes, so copying it costs milliseconds.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import duckdb

LOG: Final = logging.getLogger("assistant.guard")

REPO_ROOT: Final = Path(__file__).resolve().parent.parent
GOLD_DIR: Final = REPO_ROOT / "data" / "gold"

# The tables the assistant may query, and the only ones loaded into the sandbox.
TABLES: Final = (
    "farm_daily",
    "farm_revenue_daily",
    "capture_price",
    "region_hourly",
    "latest_snapshot",
    "quality",
)

# Rows returned to the caller. A model that forgets LIMIT should not be able to
# print a hundred thousand rows into a terminal.
MAX_ROWS: Final = 200

_COMMENT_BLOCK: Final = re.compile(r"/\*.*?\*/", re.S)
_COMMENT_LINE: Final = re.compile(r"--[^\n]*")
_FENCE: Final = re.compile(r"^\s*```(?:sql)?\s*|\s*```\s*$", re.I | re.M)


class UnsafeQuery(Exception):
    """The generated SQL is not a single read-only statement."""


@dataclass(frozen=True)
class Result:
    """One executed query and what it returned."""

    sql: str
    columns: tuple[str, ...]
    rows: list[tuple]
    truncated: bool


def strip_wrapping(text: str) -> str:
    """Remove markdown fences and surrounding whitespace from a model reply.

    Models add fences despite being told not to. Refusing over formatting the
    user never sees would be pedantry; refusing over a second statement is not.
    """
    return _FENCE.sub("", text).strip()


def statements(sql: str) -> list[str]:
    """Split SQL into statements, ignoring semicolons inside strings.

    A naive ``split(";")`` is what makes "one statement only" checks fail: a
    query containing `WHERE station_name = 'A; B'` would look like two.
    """
    out: list[str] = []
    current: list[str] = []
    quote: str | None = None
    i = 0
    while i < len(sql):
        char = sql[i]
        if quote:
            if char == quote:
                # '' and "" are escaped quotes inside a quoted literal.
                if i + 1 < len(sql) and sql[i + 1] == quote:
                    current.append(char)
                    i += 1
                else:
                    quote = None
            current.append(char)
        elif char in "'\"":
            quote = char
            current.append(char)
        elif char == ";":
            out.append("".join(current))
            current = []
        else:
            current.append(char)
        i += 1
    out.append("".join(current))
    return [s for s in (part.strip() for part in out) if s]


def check(sql: str) -> str:
    """Return the single read-only statement in ``sql``, or raise UnsafeQuery."""
    cleaned = strip_wrapping(sql)
    if not cleaned:
        raise UnsafeQuery("the model returned nothing")

    found = statements(cleaned)
    if len(found) != 1:
        raise UnsafeQuery(f"expected one statement, got {len(found)}")

    statement = found[0]
    # Comments are stripped only to find the leading keyword: a statement that
    # opens with a comment is still a SELECT, and one hidden behind a comment
    # is still refused.
    bare = _COMMENT_LINE.sub(" ", _COMMENT_BLOCK.sub(" ", statement)).strip()
    if not bare:
        raise UnsafeQuery("the statement is only a comment")

    first = bare.split(None, 1)[0].upper().lstrip("(")
    if first not in {"SELECT", "WITH"}:
        raise UnsafeQuery(f"only SELECT and WITH are allowed, got {first!r}")

    return statement


def sandbox(gold_dir: Path = GOLD_DIR) -> duckdb.DuckDBPyConnection:
    """An in-memory copy of gold with the filesystem switched off.

    The order matters: load every table first, then drop external access. Once
    dropped it cannot be restored on this connection, which is what makes the
    second defence hold even against SQL that defeats the first.
    """
    con = duckdb.connect()
    loaded = []
    for table in TABLES:
        path = gold_dir / f"{table}.parquet"
        if not path.exists():
            LOG.warning("gold table missing, not loaded: %s", path.name)
            continue
        literal = str(path).replace("'", "''")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{literal}')")
        loaded.append(table)
    if not loaded:
        con.close()
        raise UnsafeQuery(f"no gold tables found in {gold_dir} — has gold been built?")

    con.execute("SET enable_external_access=false")
    return con


def run(sql: str, gold_dir: Path = GOLD_DIR) -> Result:
    """Check ``sql``, run it in a fresh sandbox, and return the rows."""
    statement = check(sql)
    con = sandbox(gold_dir)
    try:
        cursor = con.execute(statement)
        columns = tuple(d[0] for d in cursor.description)
        rows = cursor.fetchmany(MAX_ROWS + 1)
    finally:
        con.close()

    truncated = len(rows) > MAX_ROWS
    return Result(statement, columns, rows[:MAX_ROWS], truncated)
