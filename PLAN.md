# Plan

Repository layout, design decisions, and build order. No implementation yet.

## 1. Repository structure

```
.
├── .github/
│   └── workflows/
│       ├── ingest.yml              # hourly: fetch → bronze → silver → gold → commit
│       ├── compact.yml             # daily: roll closed hour files into day files
│       ├── registry.yml            # weekly: refresh wind DUID registry, open PR on change
│       ├── tests.yml               # CI: ruff + pytest on every push and PR
│       └── pages.yml               # build and deploy the dashboard
│
├── src/aemo_wind/
│   ├── __init__.py
│   ├── config.py                   # paths, timezones, schedule constants, size budgets
│   ├── sources/
│   │   └── nemweb.py               # list directory, download zip, unpack to CSV
│   ├── state.py                    # ingestion manifest: what has been processed
│   ├── bronze.py                   # CSV → wind-filtered Parquet, one file per UTC hour
│   ├── silver.py                   # typed, deduplicated, validated, day-partitioned
│   ├── gold.py                     # run the gold SQL, write aggregate tables
│   ├── compact.py                  # merge closed hour files into a day file
│   ├── quality.py                  # data quality checks and the quality report table
│   ├── registry.py                 # wind DUID registry: load, refresh, diff
│   └── cli.py                      # `python -m aemo_wind <command>`
│
├── sql/
│   ├── bronze/select_wind.sql
│   ├── silver/normalise.sql
│   ├── silver/quality_checks.sql
│   └── gold/
│       ├── generation_5min.sql     # wide, dashboard-ready interval table
│       ├── farm_daily.sql          # MWh, capacity factor, peak per farm per day
│       ├── state_hourly.sql        # MW by state and NEM region
│       └── fleet_monthly.sql       # fleet totals and records
│
├── data/
│   ├── reference/
│   │   ├── wind_duids.csv          # DUID → farm name, state, region, capacity MW
│   │   └── wind_duids.meta.json    # source, retrieval date, row count
│   ├── bronze/date=YYYY-MM-DD/     # hour=HH.parquet (current day), day.parquet (closed)
│   ├── silver/date=YYYY-MM-DD.parquet
│   ├── gold/*.parquet
│   └── _state/
│       ├── manifest.parquet        # one row per ingested source file
│       └── runs.jsonl              # append-only run log: start, files, rows, duration
│
├── docs/                           # GitHub Pages site (static, built from gold)
│   ├── index.html
│   ├── assets/
│   └── data/                       # small JSON extracts the page fetches
│
├── assistant/
│   ├── schema.md                   # table and column descriptions given to Claude
│   ├── prompts/                    # system prompt and few-shot examples
│   └── qa.py                       # NL question → DuckDB SQL → answer
│
├── tests/
│   ├── fixtures/                   # a few real zips and CSVs, trimmed
│   ├── test_nemweb.py
│   ├── test_bronze.py
│   ├── test_silver.py
│   ├── test_gold.py
│   └── test_state.py
│
├── CLAUDE.md
├── README.md
├── PLAN.md
├── pyproject.toml                  # deps, ruff and pytest config
├── .gitignore
└── LICENSE
```

## 2. Key decisions

**DuckDB instead of Spark or pandas.** The whole dataset is a few hundred MB.
DuckDB reads and writes Parquet natively, runs the aggregations in SQL, and
needs no cluster — which matters because the only compute available is a
GitHub Actions runner. It also keeps the transformation logic as portable SQL
rather than framework-specific code.

**Raw zips are never committed.** They are downloaded into the runner's
workspace, parsed, and discarded. AEMO's `Current/` directory only retains about
two days of files, so the manifest plus the bronze layer is the durable record.

**Wind filtering happens at the silver boundary, not in bronze.** Bronze keeps
every DUID exactly as published, so it stays replayable when the wind DUID
registry changes — a new farm can be backfilled from existing bronze files
instead of re-downloading from a directory that only retains two days. The cost
is repo size: all ~500 DUIDs is roughly 150k rows per day against 23k for wind
alone, or very roughly 150 MB a year of committed Parquet rather than 35 MB.
That is affordable, and daily compaction plus zstd keeps it in check. If history
growth becomes the binding constraint, moving the filter into the bronze write is
the lever to pull.

**Immutable files, because git history is append-only.** The expensive mistake
here would be rewriting the current day's Parquet file on every run — 288
rewrites a day, each one a permanent blob. Instead bronze writes one file per
closed UTC hour and never touches it again, so history grows at roughly the rate
of the data itself. Gold tables are the exception: they are small enough
(hundreds of KB) that a full rewrite each run is cheaper than the complexity of
incremental merging.

**Hourly schedule, not every 5 minutes.** One run per hour picks up 12 source
files, produces one bronze file, and makes one commit. That is 24 commits a day
instead of 288, well inside the free tier, and the data is at most an hour
behind. Latency is not what this project is demonstrating.

**Daily compaction.** 24 hour-files a day is 8,760 files a year, which makes
checkouts slow. Once a day has closed, its hour files are merged into a single
`day.parquet` and deleted. That is one rewrite per day rather than per run, and
holds the file count to about 365 a year per layer.

**Idempotency through a manifest.** `data/_state/manifest.parquet` records every
source file that has been ingested, with its size and row count. Each run lists
the AEMO directory, subtracts what the manifest already has, and processes the
difference. This makes re-runs safe and automatically backfills gaps from a
failed or skipped run — which will happen, because GitHub's scheduled runs are
not guaranteed to fire on time.

**The DUID registry is checked in, not scraped at runtime.** Wind farm metadata
(name, state, region, registered capacity) comes from AEMO's NEM Registration
and Exemption List, which is an Excel file published irregularly. Scraping it on
every run would be fragile and slow. Instead it lives in
`data/reference/wind_duids.csv`, and a weekly workflow re-reads the source and
opens a pull request if the DUID set has changed — so new wind farms get a human
review rather than silently entering the pipeline.

**Capacity factor needs registered capacity**, which is why the registry is a
dependency of the gold layer and not just a filter list.

**Timestamps are stored twice.** AEMO publishes `SETTLEMENTDATE` in NEM time
(AEST, no daylight saving, UTC+10). Silver stores an explicit UTC timestamp for
correctness and the original NEM local timestamp for anything user-facing.
Nothing in the codebase handles a naive datetime.

**The dashboard is static.** GitHub Pages cannot run server-side code, so the
Pages workflow exports small JSON slices from gold into `docs/data/` and the
page renders them client-side. No API, nothing to keep running.

**The assistant generates SQL, it does not read the data.** Claude is given the
gold schema and the question, returns a read-only DuckDB query, and the query
runs locally against the Parquet files. This keeps token usage flat regardless
of dataset size, and makes every answer traceable to a query that can be
inspected.

## 3. Build steps

**Phase 0 — Foundations**
1. `pyproject.toml` with Python 3.11, `duckdb`, `httpx`; ruff and pytest config.
2. `.gitignore` (raw data, `data/bronze` scratch, `.env`, `__pycache__`, `.duckdb`).
3. `config.py`: paths, NEM timezone, layer schemas, size budget constants.
4. `tests.yml` running ruff and pytest, so CI is green before any logic exists.

**Phase 1 — Reference data**
5. Build `data/reference/wind_duids.csv` from the AEMO registration list.
6. `registry.py` to load it with validation, plus the refresh-and-diff workflow.

**Phase 2 — Ingestion and bronze** — *done, see `scripts/ingest_scada.py`*
7. List the AEMO directory, parse file names to interval timestamps, download
   and unpack with a timeout. ✓
8. Watermark in `data/bronze/_ingested.txt`, one source file name per line, so
   re-runs skip what is already ingested. ✓
9. Parse the nested AEMO CSV (`C`/`I`/`D` row markers, headers tracked per
   table) and write one Parquet file per source file, partitioned by NEM date. ✓
10. Parser and bronze tests against CSV fixtures, including a multi-table
    report and a corrupt file. ✓

**Phase 3 — Silver and quality**
11. `silver.py` + `sql/silver/`: cast types, deduplicate on
    (settlement time, DUID), add UTC and NEM timestamps, join farm metadata,
    flag negative and above-capacity values rather than dropping them.
12. `quality.py`: expected-interval coverage, missing DUIDs per interval, value
    range checks; write a quality table that the dashboard surfaces.

**Phase 4 — Gold**
13. The four gold SQL models, run by `gold.py` as a full rewrite.
14. Tests asserting known totals against a fixed fixture day.

**Phase 5 — Automation**
15. `ingest.yml`: hourly cron, run the CLI, commit data changes with `[skip ci]`,
    concurrency group so runs cannot overlap.
16. `compact.yml`: daily compaction of closed days.
17. A one-off backfill run to seed history from whatever `Current/` holds, plus a
    note on extending it from AEMO's archive.

**Phase 6 — Dashboard**
18. Export JSON slices from gold; build the page (fleet output, per-farm
    capacity factors, state mix, data freshness and quality panel).
19. `pages.yml` to deploy on every gold change.

**Phase 7 — Assistant**
20. `assistant/schema.md` and the system prompt describing the gold tables.
21. `qa.py`: question → SQL via the Claude API → execute read-only → format the
    answer with the query shown.
22. An eval set of ~20 questions with expected answers, run in CI.

**Phase 8 — Polish**
23. Architecture diagram and a data dictionary in the README.
24. Repo size report in the run log, and a documented pruning strategy for when
    history growth becomes a problem.
