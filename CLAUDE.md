# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this project is

An end-to-end Australian wind generation data platform, built as a portfolio
piece. It ingests two AEMO 5-minute feeds — dispatch SCADA (MW per unit) and
dispatch price (RRP per region) — keeps only wind farm units from silver
onward, and refines them through bronze / silver / gold Parquet layers. Joining
output to price is what turns generation volume into revenue, capture price and
negative-price exposure. A static dashboard and a Claude-powered Q&A assistant
sit on top.

The author is a data engineer (ADF, Synapse, Databricks medallion) with a prior
career as a wind turbine maintenance engineer, moving into AI engineering. The
repo should read as production-minded work, not a notebook dump.

## Goals

1. Reliable, idempotent ingestion of AEMO DISPATCHSCADA and DISPATCHIS on a
   schedule.
2. A clean medallion architecture that someone can follow end to end.
3. Small repo: wind DUIDs only from silver onward, write-once bronze files.
4. A static dashboard on GitHub Pages, built from the gold layer.
5. A Q&A assistant that answers questions over gold tables via the Claude API.

## Stack and constraints

- Python 3.11. DuckDB for all SQL and Parquet I/O. Parquet + zstd on disk.
- Runtime is GitHub Actions free tier only. No Azure, no paid services, no
  always-on compute, no external database.
- Data is committed back into this repository. Treat repo size as a hard budget.
- Dependencies: keep the list short. The runtime pipeline is `duckdb` and
  `httpx` only. Spreadsheet parsing for the reference registry lives in the
  `reference` extra (`pandas`, `openpyxl`, `xlrd`, `python-calamine`), which the
  hourly ingest never installs. Ask before adding anything else.

## Layer contract

| Layer  | Content | Committed | Grain |
|--------|---------|-----------|-------|
| raw    | Downloaded `PUBLIC_*.zip` | No (gitignored) | per 5-min file |
| bronze | All DUIDs and regions, source columns unchanged | Yes | one file per closed NEM hour |
| silver | Wind only. Typed, deduplicated, validated, UTC + NEM local time | Yes | one file per day |
| gold   | Aggregates and dashboard-ready tables | Yes | whole-table rewrite |

Rules that follow from this:

- Bronze is a faithful copy of the source: every DUID, columns untouched, no
  business logic. The wind filter belongs at the silver boundary, so bronze
  stays replayable if the DUID registry changes.
- Bronze and silver files are immutable once written. Never rewrite one — a
  rewritten file is a new blob in git history forever, and a file rewritten
  every run makes the repository grow without bound. Gold is small enough
  (hundreds of KB) to rewrite in full each run.
- Bronze is written one file per closed NEM hour, never per five-minute
  interval. Measured on live cardinality (517 DUIDs), hour files cost 293 MB
  and 17,520 files a year against 702 MB and 210,240 — the Parquet footer and
  the DUID dictionary are paid once an hour instead of 288 times a day.
- An hour is closed only once AEMO has published an interval in a later hour,
  and an hour is always written in full. Never write a partial hour: the rest
  of it arriving would force a rewrite. Never consult the clock for this — the
  directory listing says how far AEMO has got.
- Do not add a compaction step. Merging small files into large ones leaves both
  in git history permanently, so compacting after the fact always costs more
  than writing at the right grain to begin with.
- A silver day is written once, when bronze holds all 288 intervals for it, or
  when it is sealed: bronze has observed intervals more than two days past the
  day's end, so AEMO's two-day retention means the missing files are gone. A
  sealed day is written as it stands, with `intervals_present` and
  `is_complete` on every row. Judge this from the data's own latest interval,
  never the wall clock — a clock seals days during an ingestion outage, writing
  gaps that were still fillable.
- Every write is idempotent and keyed off a watermark: each source keeps its
  own at `data/bronze/<source>/_ingested.txt`, one file name per line, appended
  only after a successful Parquet write. Re-running a job must not duplicate
  rows or change existing files.
- Sources are independent. `scada` failing must never block `price`, and vice
  versa — AEMO retains only about two days, so a blocked source loses data
  permanently.

## Conventions

- Type hints on every function signature, including return types. Prefer
  `pathlib.Path` over `str` for paths, and `datetime` with explicit tzinfo —
  never a naive datetime.
- Comments are concise and explain *why*. No comment that restates the code, no
  decorative banners. Docstrings on public functions, one line unless the
  behaviour is genuinely subtle.
- Business logic that is naturally SQL lives in `sql/`, loaded and executed by
  thin Python wrappers. Do not hand-roll in pandas what DuckDB can express.
- Tests use `pytest`. Unit tests run against small fixtures checked into
  `tests/fixtures/`, never against the network. Mark anything that needs
  network `@pytest.mark.network` and keep it out of the default run.
- Formatting and linting with `ruff`. Line length 100.
- Module layout: one responsibility per module, no cross-layer imports
  (`gold.py` must not import `bronze.py`). Adding a NEMWeb report means
  declaring a `ReportSpec` in its own `scripts/ingest_*.py`, never editing
  `scripts/nemweb.py` with report-specific branching.
- Log with `logging`, not `print`, in anything that runs in a workflow.

## Data quality

- Checks live in `sql/quality/`, one model per check, each returning one row
  per finding as (trading_date, subject, observed, message). `check_quality.py`
  decides severity; the SQL never does.
- Hard checks fail the run with a non-zero exit: the data contradicts itself and
  anything built on it would be wrong. Soft checks warn and continue: the data
  is surprising but plausibly real.
- Put a check in the soft tier unless a finding means something downstream is
  definitely incorrect. The NEM is full of genuinely strange but real values —
  negative prices, farms over nameplate, units offline for days — and a
  pipeline that fails on all of them gets ignored.
- Results are rewritten in full to `data/gold/quality.parquet` each run,
  including a row for every check that found nothing, so a dashboard can tell
  "ran and passed" from "did not run".

## Workflows

- `ingest.yml` runs the whole pipeline hourly: the `ingest` job writes bronze,
  then the `transform` job runs silver, quality and gold. `transform` depends on
  `ingest` with `if: always()`, because the two are independent — a download
  that failed for one source must not stop silver being rebuilt from the bronze
  already committed.
- Quality gates gold, not silver. The check runs with `continue-on-error` so
  silver is still committed, gold is skipped on a hard failure, and a final step
  ends the run red so the failure cannot pass unnoticed.
- Concurrency is declared at workflow level only. A job asking for the same
  group as its own workflow queues behind its own run until it times out.
- Data commits carry `[skip ci]`, which suppresses every workflow for that push.
  Anything that must run after a data commit chains off `workflow_run`, not off
  the push — that is why `pages.yml` watches the ingest workflow finishing.

## Security

- Never commit secrets, API keys, or tokens. The only secret in this project is
  `ANTHROPIC_API_KEY`, which lives in GitHub Actions secrets and is read from
  the environment at runtime.
- Never add a credential to a workflow file, a config file, a test fixture, or a
  commit message. If something is needed locally, it goes in `.env`, which is
  gitignored.
- AEMO data is public and unauthenticated. Nothing in this pipeline should ever
  need an outbound credential except the Claude API call.

## Working style

- Keep changes scoped to what was asked. No speculative abstraction.
- Before adding a dependency, a workflow, or a new data layer, say what it buys
  and what it costs in repo size or runtime minutes.
- When touching ingestion, state the effect on git history growth.
