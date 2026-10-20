"""Tests for the generated data dictionary.

The dictionary exists because hand-maintained documentation goes stale without
anyone noticing — the README had claimed two feeds where there were seven. So
the thing worth testing is not the prose but the two properties that keep it
honest: that it is regenerable from the code, and that it does not embed
anything which changes when the data does.
"""

import re
from pathlib import Path

from data_dictionary import main, purpose, render

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_the_committed_dictionary_is_up_to_date():
    """The same check CI runs. If this fails, regenerate and commit."""
    assert main(["--check"]) == 0


def test_every_declared_bronze_table_is_documented():
    """A new ReportSpec documents itself; it cannot be forgotten here."""
    text = render()
    for key in ("scada", "price", "regionsum", "interconnector", "p5min",
                "predispatch_price", "predispatch_region"):
        assert f"### `{key}`" in text, f"bronze table {key} missing from the dictionary"


def test_every_gold_model_is_documented_with_its_own_purpose_line():
    text = render()
    for model in sorted((REPO_ROOT / "sql" / "gold").glob("*.sql")):
        assert f"### `{model.stem}`" in text
        # The purpose comes from the model, so the two must agree exactly.
        assert purpose(model) in text, f"{model.stem} purpose line not carried through"


def test_bronze_columns_name_the_aemo_column_they_came_from():
    """The mapping is the part a reader cannot derive from the Parquet alone."""
    text = render()
    assert "| `settlementdate` | TIMESTAMPTZ | `SETTLEMENTDATE` |" in text
    assert "| `scadavalue` | DOUBLE | `SCADAVALUE` |" in text
    # Optional columns are marked, because their absence is not an error.
    assert "(optional)" in text


def test_the_dictionary_holds_no_figure_that_moves_with_the_data():
    """Schema only.

    Row counts and date spans would change on every ingest, so `--check` would
    fail hourly and be ignored within a week. This asserts the document carries
    no such figure: no 'N rows', no 'N files', no bare date.
    """
    text = render()
    assert not re.search(r"\d[\d,]* rows", text)
    assert not re.search(r"\d+ files", text)
    assert not re.search(r"\d+ days", text)
    assert not re.search(r"20\d\d-\d\d-\d\d", text)


def test_a_model_without_its_parquet_is_reported_not_written_off(monkeypatch):
    """Gold skips a model whose inputs are missing; that is a state, not an error."""
    import data_dictionary as dd
    monkeypatch.setattr(dd, "DATA_DIR", REPO_ROOT / "does-not-exist")
    text = dd.render()
    assert "_Not built: its inputs are not present._" in text
    # The models are still listed, so the contract is documented either way.
    assert "### `cannibalisation`" in text


def test_purpose_falls_back_rather_than_crashing(tmp_path):
    missing = tmp_path / "nope.sql"
    assert purpose(missing) == "_no model file_"
    bare = tmp_path / "bare.sql"
    bare.write_text("SELECT 1\n")
    assert purpose(bare) == "_no purpose line_"
