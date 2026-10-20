"""Tests for the repo size report.

The report's job is to turn bytes into a decision, so what is worth asserting
is the arithmetic behind that decision: that a rate is per the days a source
has actually been observed rather than per calendar day, and that gold is
costed per rewrite rather than per day. Getting either wrong would misstate the
runway, which is the only number anyone acts on.
"""

from pathlib import Path

from repo_size import BUDGET_BYTES, GOLD_FULL_REWRITES_PER_YEAR, human, main, survey

KB = 1024


def _write(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0" * size)


def build(root: Path) -> None:
    """Two bronze sources seen over different spans, one silver table, gold.

    scada:  4 files x 10 KB over 2 days  -> 20 KB/day
    price:  2 files x 10 KB over 1 day   -> 20 KB/day, but half the total
    The differing spans are the point: a source ingested yesterday must not be
    averaged over the whole record.
    """
    for day in ("2026-10-01", "2026-10-02"):
        for hour in ("00", "01"):
            _write(root / "bronze" / "scada" / f"date={day}" / f"hour={hour}.parquet", 10 * KB)
    for hour in ("00", "01"):
        _write(root / "bronze" / "price" / "date=2026-10-02" / f"hour={hour}.parquet", 10 * KB)
    _write(root / "silver" / "wind_output" / "date=2026-10-01.parquet", 30 * KB)
    _write(root / "gold" / "farm_daily.parquet", 50 * KB)


def test_rate_is_per_observed_day_not_per_calendar_day(tmp_path):
    build(tmp_path)
    layers, _ = survey(tmp_path)
    by_name = {layer.name: layer for layer in layers}

    scada = by_name["bronze/scada"]
    assert scada.files == 4 and scada.bytes_ == 40 * KB and scada.days == 2
    assert scada.bytes_per_day == 20 * KB

    # Half the bytes, but seen for one day, so the same daily rate. Averaging
    # both over the full two-day record would halve this and understate growth.
    price = by_name["bronze/price"]
    assert price.bytes_ == 20 * KB and price.days == 1
    assert price.bytes_per_day == 20 * KB


def test_annual_projection_is_the_daily_rate_over_a_year(tmp_path):
    build(tmp_path)
    layers, _ = survey(tmp_path)
    scada = next(x for x in layers if x.name == "bronze/scada")
    assert scada.per_year == 20 * KB * 365


def test_gold_is_costed_per_rewrite_not_per_day(tmp_path):
    """Gold has no daily rate; it costs its size every time it changes."""
    build(tmp_path)
    _, gold = survey(tmp_path)
    assert gold is not None
    assert gold.bytes_ == 50 * KB
    assert gold.days == 0, "gold is not a rate, and a rate would be wrong"
    assert gold.bytes_per_day == 0.0
    assert gold.per_year == 0.0
    # The annual figure comes from the rewrite count instead.
    assert gold.bytes_ * GOLD_FULL_REWRITES_PER_YEAR == 50 * KB * 365


def test_silver_is_measured_per_table(tmp_path):
    build(tmp_path)
    layers, _ = survey(tmp_path)
    silver = next(x for x in layers if x.name == "silver/wind_output")
    assert silver.files == 1 and silver.bytes_ == 30 * KB and silver.days == 1


def test_empty_data_directory_reports_rather_than_crashing(tmp_path):
    layers, gold = survey(tmp_path)
    assert layers == [] and gold is None
    assert main(["--data-dir", str(tmp_path)]) == 0


def test_the_report_never_gates(tmp_path, capsys):
    """A size trend is a person's decision. A red hourly run would be ignored."""
    build(tmp_path)
    assert main(["--data-dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "runway" in out and "months at this rate" in out


def test_quiet_mode_is_one_line(tmp_path, capsys):
    build(tmp_path)
    assert main(["--data-dir", str(tmp_path), "--quiet"]) == 0
    out = capsys.readouterr().out.strip()
    assert len(out.splitlines()) == 1
    assert "months of a" in out


def test_human_reads_at_the_right_scale():
    assert human(512) == "512 B"
    assert human(2 * KB) == "2.0 KB"
    assert human(5 * KB * KB) == "5.0 MB"
    assert human(3 * KB ** 3) == "3.0 GB"


def test_the_budget_is_a_stated_number():
    """CLAUDE.md calls it a hard budget; an unnamed budget cannot be acted on."""
    assert BUDGET_BYTES == 1024 ** 3
