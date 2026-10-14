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
│       └── pages.yml               # deploy docs/ when gold changes
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

**Phase 4 — Gold** — *done*
15. Five gold models run by `scripts/build_gold.py` as a full rewrite:
    `farm_daily`, `farm_revenue_daily`, `capture_price`, `region_hourly` and
    `latest_snapshot`. ✓
16. `latest_snapshot` reads silver plus the newest bronze hours, filtered to
    wind in memory, so the dashboard is current even before a silver day
    closes. ✓
17. Tests asserting exact totals against a fixed fixture day. ✓

**Phase 5 — Automation** — *ingest.yml and tests.yml done*
15. `ingest.yml`: hourly cron at :07, both ingesters, commit with `[skip ci]`,
    concurrency group so runs cannot overlap, rebase-and-retry on a push race. ✓
16. `tests.yml`: ruff and pytest on Python 3.11 for every push and PR. ✓
17. A one-off backfill run to seed history from whatever `Current/` holds
    (~576 intervals, about 48 hour files per source), plus a note on extending
    it from AEMO's archive. ✓
18. `transform` job in `ingest.yml`: silver → quality → gold after every
    ingest, gated so a hard quality failure stops gold without losing silver.
    `pages.yml` chains off the ingest workflow finishing, because the data
    commits carry `[skip ci]` and would otherwise suppress it. ✓

**Phase 6 — Dashboard** — *done*
18. `scripts/export_dashboard.py` + `sql/dashboard/`: seven JSON slices from
    gold into `docs/data/`, about 18 KB in total. ✓
19. `docs/index.html`: plain HTML and Chart.js from a CDN, no build step.
    Fleet tiles, regional output, capacity factor trend, best and worst farms,
    revenue and negative-price exposure, capture detail, quality table. ✓
20. `pages.yml` deploys `docs/` whenever gold changes, regenerating the JSON
    rather than committing it. ✓

**Phase 7 — Assistant**
20. `assistant/schema.md` and the system prompt describing the gold tables.
21. `qa.py`: question → SQL via the Claude API → execute read-only → format the
    answer with the query shown.
22. An eval set of ~20 questions with expected answers, run in CI.

**Phase 8 — Polish**
23. Architecture diagram and a data dictionary in the README.
24. Repo size report in the run log, and a documented pruning strategy for when
    history growth becomes a problem.

**Phase 9 — Forecasting and battery arbitrage** — *see section 4*
25. Extend the registry from wind-only to a typed unit list, so batteries are
    classified rather than discarded, and solar and other fuels can be rolled
    up to regional totals.
26. New bronze source: `P5MIN`, AEMO's own five-minute forecast, as the
    baseline to beat and as a feature.
27. `scripts/build_features.py` + `sql/ml/`: one row per region per interval,
    derived and not committed.
28. `scripts/train_forecast.py`: walk-forward training for four targets, with
    the purge and embargo described in section 4.
29. `scripts/backtest_battery.py`: arbitrage simulation driven by the forecast,
    measured against perfect foresight and a no-forecast threshold rule.
30. Dashboard panels for forecast error against the baselines, spike
    reliability, and backtest revenue.

## 4. Forecasting and battery arbitrage

Plan only. Nothing in this section is built.

### 4.1 Scope

The project studies **wind farms and batteries**. Solar, coal, gas, hydro and
everything else enter only as **regional totals used as features** — never
modelled, never broken out per unit. That keeps the subject matter narrow
enough to say something real about, while still letting the model see the
supply stack that actually sets the price.

Everything the model sees comes from **silver** (section 4.3), which is the
last layer at five-minute grain.

This forces one change upstream. The registry today answers "is this a wind
farm?" and discards everything else; Hornsdale Power Reserve is excluded by
name. It must instead answer "what kind of unit is this?" — `wind`, `battery`,
or `other` — because a battery is now a subject and the rest is now a feature.
The existing exclusion logic becomes a classification, and `in_scada` stays as
it is.

### 4.2 Targets

Price is regional, so the unit of prediction is **(region, interval)**. Wind
and battery output are features, not targets. **SA1 first**: the most
wind-exposed region, the most volatile price, and the one where a battery has
the clearest case. Then the other four, as separate models rather than one
pooled model, until there is evidence pooling helps.

For a forecast made at interval `t`, over a window of the next `h` minutes
(`h` in {60, 120}, i.e. the next 12 and 24 dispatch intervals):

| Target | Definition | Type |
|--------|-----------|------|
| `y_mean_60` | mean RRP over `t+5 … t+60` | regression |
| `y_mean_120` | mean RRP over `t+5 … t+120` | regression |
| `y_spike_60` | any interval in `t+5 … t+60` above $300/MWh | binary |
| `y_spike_120` | any interval in `t+5 … t+120` above $300/MWh | binary |

Mean and spike are deliberately separate problems. Price is heavy-tailed
enough that a model tuned on mean absolute error learns to ignore spikes, and
a spike is exactly what a battery is paid for. One model optimising MAE would
be excellent at the 95% of intervals that do not matter commercially.

### 4.3 Features

**Features are built from silver, never from gold.**

Gold's finest grain is hourly (`region_hourly`); everything else in it is
daily. The targets in 4.2 are defined on five-minute dispatch intervals, and
most of the features below are five-minute lags, ramps and spike counts.
Those cannot be recovered from an hourly mean — the averaging has already
removed the spike, which is the single thing a battery is paid for. Silver is
the last layer that still holds one row per five-minute interval:

| Layer | Finest grain | Usable as a feature source |
|-------|--------------|---------------------------|
| `silver/wind_output`, `silver/price` | 5 minutes | **yes** |
| `gold/region_hourly` | 1 hour | no — spikes already averaged away |
| `gold/farm_daily`, `farm_revenue_daily`, `capture_price` | 1 day | no |

This is not a flaw in gold. Gold aggregates deliberately, for human reading;
it is simply the wrong input for a model predicting five-minute prices.

Gold keeps two roles in this phase: as context when interpreting results
(`capture_price` says what wind actually earned, which is the commercial
backdrop to a forecast), and as the dashboard's source, which is where the
model's own outputs are published alongside it.

Every feature is **a function of the (region, interval) grid at or before `t`**.

**Price history** — lags at 5, 10, 15, 30, 60, 120 minutes; rolling mean,
standard deviation, min and max over 30, 60 and 240 minutes; count of intervals
above $300 in the last 60 and 240 minutes; time since the last spike.

**Wind** — regional wind MW; wind as a share of registered wind capacity;
5, 30 and 60-minute ramp; the same for the region's neighbours, since an
interconnector makes a neighbour's wind a local price signal.

**Battery** — regional battery MW, signed, so charging and discharging are
distinguishable; battery share of registered battery capacity, a crude proxy
for available state of charge, which AEMO does not publish directly.

**Other supply, as regional totals only** — solar MW, and non-renewable MW
as a single figure. The point is the residual: demand minus wind minus solar
is what the dispatchable stack has to cover, and it predicts price far better
than any of its parts.

**Demand and system** — regional demand, its ramp, and net interconnector flow.

**Calendar** — interval of day and day of week, each as sine/cosine pairs so
midnight is adjacent to 23:55; public holiday flag; month.

**AEMO's own forecast** — the `P5MIN` run issued at or before `t`: forecast
RRP for each interval in the window, and forecast regional demand. This is both
the strongest feature and the baseline to beat.

**Weather (optional, later)** — forecast wind speed at hub height for the
region's main wind corridors. Deferred, not because it would not help but
because it introduces a second source with a genuinely harder leakage problem
(section 4.4) and the model should first be shown to work without it.

### 4.4 Leakage rules

These are the rules the whole phase stands on. A leaked feature produces a
model that looks superb and earns nothing.

1. **A feature at `t` may use only data whose interval ends at or before
   `t`.** The grid is interval-ending, so the interval labelled `t` is already
   complete and is admissible.

2. **Allow one interval of publication lag.** A dispatch interval ending at
   `t` is not on NEMWeb at `t`. Features use data up to `t − 5 min` unless a
   measured publication delay proves otherwise. Costing one interval of
   information is cheaper than discovering the whole backtest was optimistic.

3. **Forecasts are keyed by issue time, never by valid time.** Every
   forecast row carries `issued_at` and `valid_for`, and a feature may use a
   row only where `issued_at <= t`. Joining on `valid_for` alone is the
   classic way to hand a model tomorrow's answer. This applies to `P5MIN`,
   pre-dispatch and any weather forecast.

4. **Weather is admissible only from a forecast issued before `t`.** Observed
   weather at `t` is not available at `t` in practice, and analysis-grade
   reanalysis is not available at all in real time. Store the issue time or do
   not store it.

5. **The registry is a slowly changing dimension.** Using today's capacities
   to build 2024 features tells the model which farms were later expanded.
   Either snapshot the registry with effective dates, or restrict training to
   the period since the current registry was valid, and say which.

6. **Fit every transformation inside the fold.** Scalers, imputation values,
   target encodings and feature selection are fitted on training rows only.

7. **Purge and embargo** (section 4.5), because a target window straddling
   the fold boundary leaks the test period into training.

8. **No feature derived from the target window**, including "number of
   intervals since the last spike" computed with hindsight.

A test asserts rule 1 mechanically: for a sample of rows, rebuild each feature
using only data up to `t` and assert equality with the stored value. A feature
that fails cannot be explained away.

### 4.5 Walk-forward validation

Random k-fold on a time series is meaningless here, and ordinary
train/test splitting is not enough either, because the targets overlap.

- **Rolling origin.** Fold `k` trains on everything up to `cutoff_k` and tests
  on the following 14 days. Origins advance 14 days at a time.
- **Purge.** Drop training rows whose target window extends past `cutoff_k`.
  With a 120-minute horizon that is the last 24 training intervals.
- **Embargo.** Skip the first 120 minutes of the test period, so no test
  feature overlaps a training target.
- **Expanding, not sliding, window** to start. The NEM changes structurally —
  new batteries, new transmission — so a sliding window may prove better; that
  is an experiment, not an assumption.
- **Report per fold, never only pooled.** A single pooled number hides a model
  that worked until a market change and then stopped.

### 4.6 Baselines

The model must beat all three, reported side by side at every horizon:

1. **Persistence** — the forecast mean is the current RRP; spike probability is
   1 if the current interval is above $300. Trivially cheap and surprisingly
   hard to beat over 60 minutes.
2. **Same time yesterday** — the mean over the same clock window 24 hours
   earlier. Captures the daily shape that persistence misses.
3. **AEMO `P5MIN`** — the market operator's own forecast for the same window.
   This is the honest bar. If the model cannot beat it, the correct conclusion
   is that AEMO's forecast is good, and the write-up says so.

For spike probability, a fourth: **climatology**, the historical spike rate for
that interval-of-day, which is what any calibrated model must beat to be worth
its complexity.

**Metrics.** Regression: MAE and RMSE, reported both overall and split by
whether the window contained a spike, because an overall MAE is dominated by
the rare intervals. Classification: Brier score, precision-recall AUC (not
ROC AUC — spikes are rare enough that ROC flatters everything), and a
reliability curve, since a probability that is not calibrated is useless to
the dispatch rule in 4.7. The decisive metric is section 4.7's revenue.

### 4.7 Battery arbitrage backtest

A forecast is only worth what a decision made from it earns.

**The battery.** A price-taking unit, parameterised: power MW, energy MWh,
round-trip efficiency (0.85 to start), state-of-charge limits. Default to a
realistic SA1 unit. Price-taking is an assumption worth stating: a large
battery moves the price it trades into, and this backtest will not model that,
so results overstate what a large unit would earn.

**The policy.** Charge when the forecast mean over the next window is in the
bottom quantile of recent prices and state of charge allows; discharge when
the forecast mean is in the top quantile or forecast spike probability exceeds
a threshold. Thresholds tuned **inside the training fold only** — tuning them
on the test period is leakage wearing a different hat.

**Settlement.** Every trade settles at the *actual* RRP of the interval it
occurs in, never the forecast. Charging pays, discharging earns, and
round-trip losses are taken on the charge leg.

**Comparators.**

| Strategy | What it establishes |
|----------|--------------------|
| Perfect foresight | the ceiling: all the money a perfect forecast could earn |
| Forecast-driven policy | what this model is worth |
| Fixed price threshold, no forecast | how much of that is the forecast rather than the rule |
| Do nothing | zero |

Reported as $/MW/day and as a share of the perfect-foresight ceiling, per fold.
The second number is the one that matters: "captured 34% of the theoretical
maximum" says more than a dollar figure that depends on how volatile the test
period happened to be.

### 4.8 Repo size

Measured on a worst case of 1,440 region-intervals a day and 50 float columns,
using random values, which do not compress — real lag and rolling features
correlate heavily and would do better:

| Artefact | Per day | Per year | Committed? |
|----------|---------|----------|-----------|
| Features (50 cols) | 551 KB | **201 MB** | **No — derived** |
| Predictions (4 targets) | 46 KB | **17 MB** | Yes |
| Model files | — | ~60 MB if monthly | **No** |
| `P5MIN` bronze | — | ~15 MB | Yes |

Committing features would cost more than the entire existing pipeline and push
the repository past GitHub's 1 GB recommendation inside two years. They are
reproducible from silver by definition, so they are rebuilt in the workflow
and gitignored — the same decision already taken for `docs/data`.

Model files are build artefacts, not data: kept as workflow artefacts, with
only metrics and predictions committed. A model worth keeping gets attached to
a release.

**ML therefore adds about 32 MB a year**, taking the project to roughly
330 MB a year. The budget decision stands.

Model outputs get their own tree, because they do not all behave the same way:

```
data/ml/
  predictions/date=YYYY-MM-DD.parquet   write-once, like silver
  metrics.parquet                        full rewrite, like gold
  backtest.parquet                       full rewrite, like gold
```

Predictions are **write-once**. A forecast issued at 14:05 yesterday is a
historical fact, and rewriting it would destroy the only honest record of what
the model actually said at the time — the difference between a backtest and a
retrofit. Metrics and backtest results are recomputed across all history on
every run, so they are rewritten in full, exactly as gold is.

Features sit outside this tree entirely: derived, gitignored, rebuilt in the
workflow.

The flow, end to end:

```
silver ──► features (derived, gitignored) ──► model ──► data/ml/predictions
                                                   └──► data/ml/metrics
  gold ──────────────────────────────────────────────► dashboard ◄──────┘
```

### 4.9 Dependencies

A new `ml` extra, installed by the ML workflow and by nothing else:

```toml
ml = ["scikit-learn>=1.5", "lightgbm>=4.5", "pandas>=2.2", "numpy>=2.0"]
```

Gradient boosting because the problem is tabular, non-linear, and small enough
to train on a free runner in minutes; a regularised linear model is the first
thing to try and the thing to fall back to if boosting does not clearly beat it.
These are large wheels — scikit-learn alone is tens of megabytes — which is
precisely why the hourly ingest must keep installing plain `.` and never see
them.
