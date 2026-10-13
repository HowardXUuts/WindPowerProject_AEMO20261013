# Plan

Repository layout, design decisions, and build order. Sections 1 and 2 describe
the repository as built; section 3 marks what is done and what remains.

## 1. Repository structure

```
.
├── .github/
│   └── workflows/
│       ├── ingest.yml              # hourly: fetch both feeds → bronze → commit
│       ├── reference.yml           # manual: refresh the wind DUID registry
│       ├── tests.yml               # CI: ruff + pytest on every push and PR
│       └── pages.yml               # build and deploy the dashboard
│
├── scripts/
│   ├── nemweb.py                   # shared: list, download, parse, watermark, write
│   ├── cli.py                      # shared argparse entry point
│   ├── ingest_scada.py             # ReportSpec: Dispatch_SCADA → bronze/scada
│   ├── ingest_price.py             # ReportSpec: DispatchIS   → bronze/price
│   ├── refresh_reference.py        # wind DUID registry from the AEMO list
│   ├── build_silver.py             # wind filter, typing, dedup, validation
│   ├── check_quality.py            # quality checks, hard fail / soft warn
│   └── build_gold.py               # run the gold SQL, write aggregate tables
│
├── sql/
│   ├── silver/wind_output.sql       # wind filter, timestamps, flags
│   ├── silver/price.sql             # regional price, pricing run only
│   ├── quality/*.sql                # one model per quality check
│   └── gold/
│       ├── generation_5min.sql     # wide, dashboard-ready interval table
│       ├── farm_daily.sql          # MWh, capacity factor, peak per farm per day
│       ├── farm_revenue_daily.sql  # MWh x RRP: revenue and realised price
│       ├── capture_price.sql       # capture price vs time-weighted average price
│       ├── negative_price.sql      # intervals generating into a negative price
│       ├── state_hourly.sql        # MW by state and NEM region
│       └── fleet_monthly.sql       # fleet totals and records
│
├── data/
│   ├── reference/
│   │   ├── wind_duids.csv          # DUID → station, region, fuel, capacity, in_scada
│   │   └── wind_duids.meta.json    # source, retrieval date, counts, exclusions
│   ├── bronze/
│   │   ├── scada/_ingested.txt     # watermark, one source file name per line
│   │   ├── scada/date=YYYY-MM-DD/  # hour=HH.parquet, write-once
│   │   ├── price/_ingested.txt
│   │   └── price/date=YYYY-MM-DD/  # hour=HH.parquet
│   ├── silver/date=YYYY-MM-DD.parquet
│   └── gold/*.parquet
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
│   ├── fixtures/                   # trimmed AEMO CSVs, committed
│   ├── test_parser.py              # C/I/D nested-CSV parsing
│   ├── test_bronze.py              # hour selection, writes, watermark
│   ├── test_listing.py             # directory listing and file names
│   ├── test_price.py               # DISPATCH_PRICE specifics
│   └── test_reference.py           # wind DUID registry
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
two days of files, so the watermark plus the bronze layer is the durable record.

**Wind filtering happens at the silver boundary, not in bronze.** Bronze keeps
every DUID exactly as published, so it stays replayable when the wind DUID
registry changes — a new farm can be backfilled from existing bronze files
instead of re-downloading from a directory that only retains two days. The cost
is repo size: all ~500 DUIDs is roughly 150k rows per day against 23k for wind
alone, or very roughly 150 MB a year of committed Parquet rather than 35 MB.
That is affordable at the hour grain below, and if history growth ever becomes
the binding constraint, moving the filter into the bronze write is the lever to
pull.

**Spot price is ingested alongside generation, not instead of it.** Generation
alone measures volume. Joined to the regional reference price it measures
economics: revenue per farm, capture price against the time-weighted average,
and how often a farm is generating into a negative price — which is the
commercially interesting question for Australian wind, and the one a former
turbine engineer is well placed to ask. Both feeds are the same shape, so the
second source cost one `ReportSpec`, not a second pipeline.

**Bronze keeps intervention runs.** When AEMO intervenes it publishes both a
physical and a pricing run for the same interval. Discarding one is a business
decision, so bronze keeps both and silver resolves it.

**FCAS price columns are not ingested.** They are eight separate ancillary
service markets, irrelevant to wind energy revenue, and would quadruple the
price table's width. This is the one place bronze is not perfectly faithful,
and it is a one-way door: history written before the columns are added will not
carry them.

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

**One Parquet file per closed hour, and no compaction.** Measured on live
cardinality (517 DUIDs, 5 regions), the three candidate grains cost:

| Grain | Size/year | Files/year | Rewrites |
|-------|-----------|------------|----------|
| per 5-min interval | 702 MB | 210,240 | none |
| **per hour** | **293 MB** | **17,520** | **none** |
| per day | 227 MB | 730 | one per run |

Hour files cost 58% less than interval files because the Parquet footer and the
DUID dictionary are paid once an hour instead of 288 times a day. Daily files
are smaller still, but only reachable by rewriting the day file on every run,
which costs about 2.5 GB a year of superseded blobs — worse than either
alternative — or by running once a day and risking permanent loss against
AEMO's two-day retention.

Compaction was the original plan and is now explicitly rejected: merging small
files into large ones leaves *both* permanently in git history, so it always
costs more than writing at the right grain in the first place. The lever is
choosing the grain, not cleaning up afterwards.

**An hour is written only once it is closed, and only in full.** AEMO having
published an interval in a later hour is what proves no further file can land
in this one; the clock is never consulted, because a late or early run would
then corrupt the boundary. A partial hour is never written, since the rest of
it arriving would force exactly the rewrite this design exists to avoid. The
`limit` input therefore rounds down to whole hours.

**Idempotency through a watermark.** `data/bronze/<source>/_ingested.txt` holds
one source file name per line, appended only after the Parquet write succeeds.
Each run lists the AEMO directory, subtracts what the watermark already has, and
processes the difference. This makes re-runs safe and automatically backfills gaps from a
failed or skipped run — which will happen, because GitHub's scheduled runs are
not guaranteed to fire on time.

**The DUID registry is checked in, refreshed by hand.** Wind farm metadata
comes from AEMO's NEM Registration and Exemption List, an Excel file published
irregularly. Scraping it on every run would be fragile and slow, so it lives in
`data/reference/wind_duids.csv` and `reference.yml` refreshes it on manual
dispatch — a new DUID entering the pipeline gets a human look at the diff.

Fuel source alone cannot identify a wind farm: AEMO files Hornsdale Power
Reserve, a battery, under fuel source "Wind" because it sits beside the wind
farm. The technology and dispatch type columns are what separate a turbine from
a battery, and excluded units are named in the meta sidecar so the judgement is
reviewable.

The registry also carries `in_scada`, true when the DUID appears in committed
bronze SCADA. Twelve registered wind farms are non-scheduled and never appear in
DISPATCHSCADA at all; without the flag, quality checks cannot tell a
non-scheduled unit from a genuine ingestion gap.

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

**Phase 1 — Reference data** — *script and workflow done, CSV pending a run*
5. `scripts/refresh_reference.py`: download the NEM Registration and Exemption
   List, locate the generator sheet and header row by name, filter to wind, and
   write `wind_duids.csv` plus a provenance sidecar. ✓
6. `reference.yml`: manual dispatch, since the list changes only on
   registration or retirement and a new DUID deserves a human look. ✓
7. Load-with-validation helper for the silver join.

**Phase 2 — Ingestion and bronze** — *done, see `scripts/`*
7. List the AEMO directory, parse file names to interval timestamps, download
   and unpack with a timeout. ✓
8. Watermark in `data/bronze/_ingested.txt`, one source file name per line, so
   re-runs skip what is already ingested. ✓
9. Parse the nested AEMO CSV (`C`/`I`/`D` row markers, headers tracked per
   table) and write one Parquet file per source file, partitioned by NEM date. ✓
10. Parser and bronze tests against CSV fixtures, including a multi-table
    report and a corrupt file. ✓
11. Second source: `ingest_price.py` for DispatchIS `DISPATCH_PRICE`, sharing
    the same core through a `ReportSpec`. ✓

**Phase 3 — Silver and quality** — *silver done, quality outstanding*
11. `scripts/build_silver.py` + `sql/silver/`: restate AEMO's interval-ending
    timestamps as start and end in UTC and NEM local, date by interval start,
    deduplicate on (interval_start, duid), join the registry, and flag negative
    and above-capacity values rather than dropping them. ✓
12. Price silver on the same timestamps, keeping the pricing run where an
    interval carries both. ✓
13. Day completeness and sealing, so a permanently incomplete day cannot block
    silver forever. ✓
14. `scripts/check_quality.py` + `sql/quality/`: six checks over silver,
    hard ones failing the run and soft ones warning, published to
    `data/gold/quality.parquet` for the dashboard. ✓

**Phase 4 — Gold**
13. The four gold SQL models, run by `gold.py` as a full rewrite.
14. Tests asserting known totals against a fixed fixture day.

**Phase 5 — Automation** — *ingest.yml and tests.yml done*
15. `ingest.yml`: hourly cron at :07, both ingesters, commit with `[skip ci]`,
    concurrency group so runs cannot overlap, rebase-and-retry on a push race. ✓
16. `tests.yml`: ruff and pytest on Python 3.11 for every push and PR. ✓
17. A one-off backfill run to seed history from whatever `Current/` holds
    (~576 intervals, about 48 hour files per source), plus a note on extending
    it from AEMO's archive.

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
