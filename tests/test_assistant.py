"""Tests for the question-to-SQL assistant.

The assistant executes SQL that a language model wrote from text an end user
supplied. Most of what follows is about that: the guard is the only thing
standing between a generated string and the filesystem, so it is tested against
the attacks rather than only the happy path.

Nothing here needs a credential or the network.
"""

import json
from pathlib import Path

import duckdb
import pytest
from eval import normalise, score_one
from guard import TABLES, Result, UnsafeQuery, check, run, sandbox, statements, strip_wrapping
from provider import Provider, ProviderError, ask, fingerprint, load_recording, save_recording
from qa import render

PROVIDER = Provider(
    name="test", base_url="https://example.invalid/v1", model="test-model", env_var="TEST_KEY_UNSET"
)


@pytest.fixture
def gold(tmp_path):
    """A tiny gold layer with the real table names."""
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir()
    con = duckdb.connect()
    try:
        con.execute(f"""COPY (
            SELECT DATE '2026-10-12' AS trading_date, 'WFA1' AS duid, 'Alpha' AS station_name,
                   'SA1' AS region, 100.0 AS capacity_mw, 1200.0 AS energy_mwh,
                   0.5 AS capacity_factor, 80.0 AS peak_mw, 0.0 AS min_mw,
                   288 AS intervals, true AS is_complete
        ) TO '{gold_dir / "farm_daily.parquet"}' (FORMAT PARQUET)""")
        con.execute(f"""COPY (
            SELECT 'secret-value' AS leaked
        ) TO '{gold_dir.parent / "secrets.parquet"}' (FORMAT PARQUET)""")
    finally:
        con.close()
    return gold_dir


# --- the allowlist --------------------------------------------------------


def test_a_plain_select_is_allowed():
    assert check("SELECT 1") == "SELECT 1"


def test_a_cte_is_allowed():
    sql = "WITH x AS (SELECT 1 AS n) SELECT n FROM x"
    assert check(sql) == sql


def test_markdown_fences_are_stripped_not_refused():
    # Models add fences despite being told not to. Refusing over formatting the
    # user never sees would be pedantry.
    assert check("```sql\nSELECT 1\n```") == "SELECT 1"
    assert strip_wrapping("```\nSELECT 2\n```") == "SELECT 2"


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE farm_daily",
        "DELETE FROM farm_daily",
        "UPDATE farm_daily SET energy_mwh = 0",
        "INSERT INTO farm_daily VALUES (1)",
        "CREATE TABLE evil AS SELECT 1",
        "ATTACH 'evil.db' AS evil",
        "COPY farm_daily TO '/tmp/leak.csv'",
        "INSTALL httpfs",
        "LOAD httpfs",
        "PRAGMA database_list",
        "SET enable_external_access=true",
    ],
)
def test_every_non_select_statement_is_refused(sql):
    with pytest.raises(UnsafeQuery):
        check(sql)


def test_a_second_statement_is_refused():
    # The classic: a harmless SELECT with something appended.
    with pytest.raises(UnsafeQuery, match="one statement"):
        check("SELECT 1; DROP TABLE farm_daily")


def test_a_trailing_semicolon_is_not_a_second_statement():
    assert check("SELECT 1;") == "SELECT 1"


def test_a_semicolon_inside_a_string_is_not_a_separator():
    # A naive split(";") refuses this valid query. The data really does contain
    # semicolons in station names and messages.
    sql = "SELECT * FROM quality WHERE message = 'a; b'"
    assert check(sql) == sql
    assert len(statements(sql)) == 1


def test_an_escaped_quote_inside_a_string_is_handled():
    sql = "SELECT * FROM farm_daily WHERE station_name = 'O''Brien; Farm'"
    assert len(statements(sql)) == 1


def test_a_comment_cannot_hide_the_real_statement():
    with pytest.raises(UnsafeQuery):
        check("/* SELECT 1 */ DROP TABLE farm_daily")


def test_a_statement_that_is_only_a_comment_is_refused():
    with pytest.raises(UnsafeQuery, match="only a comment"):
        check("-- nothing here")


def test_an_empty_reply_is_refused():
    with pytest.raises(UnsafeQuery, match="returned nothing"):
        check("   ")


# --- the sandbox ----------------------------------------------------------


def test_the_sandbox_can_read_the_tables_it_loaded(gold):
    result = run("SELECT count(*) AS n FROM farm_daily", gold)
    assert result.rows == [(1,)]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM read_parquet('/etc/passwd')",
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT * FROM read_parquet('../secrets.parquet')",
    ],
)
def test_the_sandbox_refuses_the_filesystem(gold, sql):
    # The second defence: even a SELECT that passes the allowlist cannot reach
    # a file. This is why the allowlist is not the only check.
    con = sandbox(gold)
    try:
        with pytest.raises(duckdb.Error):
            con.execute(sql)
    finally:
        con.close()


def test_external_access_cannot_be_turned_back_on(gold):
    con = sandbox(gold)
    try:
        with pytest.raises(duckdb.Error):
            con.execute("SET enable_external_access=true")
    finally:
        con.close()


def test_a_sibling_file_outside_gold_is_unreachable(gold):
    # The fixture writes secrets.parquet next to the gold directory on purpose.
    assert (gold.parent / "secrets.parquet").exists()
    con = sandbox(gold)
    try:
        with pytest.raises(duckdb.Error):
            con.execute(f"SELECT * FROM read_parquet('{gold.parent / 'secrets.parquet'}')")
    finally:
        con.close()


def test_each_question_gets_a_fresh_sandbox(gold):
    # Nothing a query does in memory can reach the next question.
    run("SELECT 1", gold)
    assert run("SELECT count(*) AS n FROM farm_daily", gold).rows == [(1,)]


def test_missing_gold_is_a_clear_error(tmp_path):
    with pytest.raises(UnsafeQuery, match="no gold tables"):
        run("SELECT 1", tmp_path / "absent")


def test_only_the_declared_tables_are_loaded(gold):
    con = sandbox(gold)
    try:
        present = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    finally:
        con.close()
    assert present <= set(TABLES)


def test_results_are_capped(gold):
    result = run("SELECT * FROM range(0, 5000)", gold)
    assert result.truncated
    assert len(result.rows) == 200


# --- recordings, so the repo needs no key ---------------------------------


def test_a_recording_round_trips(tmp_path):
    key = fingerprint(PROVIDER, "sys", "question?")
    save_recording(key, PROVIDER, "question?", "SELECT 1", tmp_path)
    assert load_recording(key, tmp_path) == "SELECT 1"


def test_the_fingerprint_changes_with_the_model(tmp_path):
    other = Provider(PROVIDER.name, PROVIDER.base_url, "other-model", PROVIDER.env_var)
    # A recording made against one model is not evidence about another.
    assert fingerprint(PROVIDER, "sys", "q") != fingerprint(other, "sys", "q")


def test_the_fingerprint_changes_with_the_prompt(tmp_path):
    assert fingerprint(PROVIDER, "sys", "a") != fingerprint(PROVIDER, "sys", "b")
    assert fingerprint(PROVIDER, "sys-a", "q") != fingerprint(PROVIDER, "sys-b", "q")


def test_a_recorded_reply_is_served_without_a_key(tmp_path, monkeypatch):
    monkeypatch.delenv(PROVIDER.env_var, raising=False)
    key = fingerprint(PROVIDER, "sys", "q")
    save_recording(key, PROVIDER, "q", "SELECT 1", tmp_path)

    reply, source = ask("sys", "q", PROVIDER, directory=tmp_path)
    assert (reply, source) == ("SELECT 1", "recording")


def test_no_key_and_no_recording_says_both(tmp_path, monkeypatch):
    monkeypatch.delenv(PROVIDER.env_var, raising=False)
    with pytest.raises(ProviderError) as exc:
        ask("sys", "unrecorded", PROVIDER, directory=tmp_path)
    assert PROVIDER.env_var in str(exc.value)


def test_replay_never_calls_out(tmp_path, monkeypatch):
    # Even with a key set, --replay must not reach the network.
    monkeypatch.setenv(PROVIDER.env_var, "sk-not-a-real-key")
    with pytest.raises(ProviderError, match="no recording"):
        ask("sys", "unrecorded", PROVIDER, replay=True, directory=tmp_path)


# --- the eval -------------------------------------------------------------


def test_results_compare_by_value_not_by_sql_text(gold, tmp_path, monkeypatch):
    monkeypatch.delenv(PROVIDER.env_var, raising=False)
    question = {
        "id": "count",
        "question": "how many farms?",
        "reference_sql": "SELECT count(*) AS farms FROM farm_daily",
    }
    # Different text, different alias, same answer.
    model_sql = "SELECT COUNT(1) AS n FROM farm_daily WHERE 1=1"
    save_recording(
        fingerprint(PROVIDER, __import__("qa").system_prompt(), question["question"]),
        PROVIDER, question["question"], model_sql, tmp_path,
    )
    monkeypatch.setattr("eval.ask", lambda s, q, p, **kw: (model_sql, "recording"))

    assert score_one(question, PROVIDER, gold, False, True).status == "pass"


def test_a_wrong_answer_fails(gold, monkeypatch):
    question = {
        "id": "count",
        "question": "how many farms?",
        "reference_sql": "SELECT count(*) AS farms FROM farm_daily",
    }
    monkeypatch.setattr("eval.ask", lambda s, q, p, **kw: ("SELECT 999 AS n", "recording"))
    assert score_one(question, PROVIDER, gold, False, True).status == "fail"


def test_a_dangerous_query_is_scored_as_refused(gold, monkeypatch):
    question = {"id": "x", "question": "q", "reference_sql": "SELECT 1"}
    monkeypatch.setattr("eval.ask", lambda s, q, p, **kw: ("DROP TABLE farm_daily", "recording"))
    assert score_one(question, PROVIDER, gold, False, True).status == "refused"


def test_normalise_ignores_row_order_and_float_noise():
    assert normalise([(1.000000001,), (2.0,)]) == normalise([(2.0,), (1.0,)])


def test_every_reference_query_is_a_safe_select():
    # The eval's own SQL goes through the same guard as the model's.
    questions = json.loads(
        (Path(__file__).resolve().parent.parent / "assistant" / "evals" / "questions.json")
        .read_text()
    )
    assert len(questions) >= 20
    for question in questions:
        check(question["reference_sql"])


def test_question_ids_are_unique():
    questions = json.loads(
        (Path(__file__).resolve().parent.parent / "assistant" / "evals" / "questions.json")
        .read_text()
    )
    ids = [q["id"] for q in questions]
    assert len(ids) == len(set(ids))


# --- rendering ------------------------------------------------------------


def test_render_shows_a_table():
    result = Result("SELECT 1", ("region", "mw"), [("SA1", 1234.5)], False)
    out = render(result)
    assert "region" in out and "SA1" in out and "1,234.5" in out


def test_render_marks_truncation():
    result = Result("SELECT 1", ("n",), [(1,)], True)
    assert "truncated" in render(result)


def test_render_handles_no_rows():
    assert render(Result("SELECT 1", ("n",), [], False)) == "(no rows)"
