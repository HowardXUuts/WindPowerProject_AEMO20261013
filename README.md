# Australian Wind Power Data Platform

A serverless data platform for Australian wind generation, built on AEMO's
public market data.

Every five minutes AEMO publishes the metered output of every generating unit in
the National Electricity Market, and the spot price each region is settled at.
This project collects both feeds, keeps the wind farms, and refines them into a
queryable history of Australian wind generation *and what it earned* — then puts
a dashboard and a natural-language Q&A assistant on top of it.

The whole thing runs on GitHub Actions and stores its data in this repository.
No cloud account, no database server, no paid services.

## Architecture

```mermaid
flowchart LR
  subgraph nemweb["NEMWeb · Reports/Current · ~2 days retention"]
    direction TB
    SC["Dispatch_SCADA"]
    DI["DispatchIS_Reports"]
    P5["P5_Reports"]
    PD["PredispatchIS_Reports"]
  end

  subgraph bronze["bronze · write-once, one file per closed NEM hour"]
    direction TB
    B1["scada"]
    B2["price"]
    B3["regionsum"]
    B4["interconnector"]
    B5["p5min"]
    B6["predispatch_price<br/>predispatch_region"]
  end

  subgraph silver["silver · wind only, immutable per trading date"]
    direction TB
    S1["wind_output"]
    S2["price"]
  end

  GOLD["gold · 13 tables<br/>full rewrite each run<br/>revenue · capture price<br/>cannibalisation · concentration<br/>negative-price runs · correlation<br/>demand shape · quality"]

  DASH["dashboard<br/>GitHub Pages"]
  QA["Q&A assistant<br/>question to read-only SQL"]
  ML["ML features<br/>derived, never committed"]

  SC --> B1
  DI --> B2
  DI --> B3
  DI --> B4
  P5 --> B5
  PD --> B6

  B1 --> S1
  B2 --> S2
  S1 --> GOLD
  S2 --> GOLD
  B3 --> GOLD

  GOLD --> DASH
  GOLD --> QA
  S1 --> ML
  S2 --> ML
  B5 --> ML
```

One download can feed several bronze tables: `DispatchIS` carries price,
regional demand and interconnector flows in one zip, so it is fetched once and
parsed three ways, each with its own watermark. Forecast sources keep the run
time AEMO issued them at, because the leakage rule depends on it.

| Layer | What it holds | Committed |
|-------|---------------|-----------|
| **raw** | The downloaded `PUBLIC_*.zip` | No — transient |
| **bronze** | Every DUID and region as published, columns untouched. One Parquet per closed NEM hour, never rewritten. | Yes |
| **silver** | Wind only. Interval start and end in UTC and NEM local, deduplicated, quality-flagged. One immutable file per trading date. | Yes |
| **gold** | Aggregates and dashboard-ready tables, rewritten in full each run. | Yes |
| **features** | The ML feature table, rebuilt from silver on demand. | No — gitignored |

Every column of every table is listed in
[`DATA_DICTIONARY.md`](DATA_DICTIONARY.md), which is generated from the models
and the Parquet rather than written by hand.

## Stack

Python 3.11 · DuckDB · Parquet · GitHub Actions · GitHub Pages

No new dependencies for the assistant: any OpenAI-compatible endpoint over the
`httpx` the pipeline already uses. No API key is required to clone, test or
run CI.

## Data source

Two public, unauthenticated NEMWeb feeds, both published every 5 minutes:

- [Dispatch SCADA](https://nemweb.com.au/Reports/Current/Dispatch_SCADA/) —
  `PUBLIC_DISPATCHSCADA_*.zip`, MW output per DUID (dispatchable unit identifier).
- [DispatchIS](https://nemweb.com.au/Reports/Current/DispatchIS_Reports/) —
  `PUBLIC_DISPATCHIS_*.zip`, whose `DISPATCH_PRICE` table holds the regional
  reference price (RRP, $/MWh) that generators are settled against.

AEMO keeps only about two days in `Current/`, so ingestion runs hourly and
backfills from a watermark rather than assuming every scheduled run fires.

AEMO data is used under the terms of its
[copyright permissions](https://aemo.com.au/privacy-and-legal-notices/copyright-permissions).
This project is not affiliated with AEMO.

## Status

Bronze is live: both feeds ingest hourly on GitHub Actions and commit Parquet
back to this repository, one file per closed NEM market hour. The wind DUID
registry is built from AEMO's registration list and refreshed on demand. Silver
joins the two into a wind-only, quality-flagged table, one immutable file per
trading date, and gold turns that into revenue, capture price and a live fleet
snapshot. A static dashboard on GitHub Pages reads JSON slices exported from
gold: a fleet overview, and a per-farm page that pairs each farm's output shape
with its region's price shape. The assistant turns a question into read-only
SQL over gold and prints the answer with the query that produced it.

See [`PLAN.md`](PLAN.md) for the repository layout, design decisions, and build
steps.

## Running it yourself

The pipeline needs two repository settings that cannot be set from a workflow:

- **Settings → Actions → General**: allow all actions, and give workflows
  **read and write** permissions, so the hourly ingest can commit data back.
- **Settings → Pages → Build and deployment → Source: GitHub Actions**, before
  the first dashboard deploy. The automatic token can deploy to a Pages site but
  cannot create one, so this is a one-time manual step.

Locally:

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install '.[dev,reference]'
pytest -q                                    # full suite, no network needed
python scripts/export_dashboard.py           # gold + silver -> docs/data/
python -m http.server 8000 --directory docs  # then open localhost:8000

python assistant/eval.py                     # scores the assistant; no key needed
python scripts/data_dictionary.py --check     # dictionary matches the models
python assistant/qa.py "which farms earned the most?"
```

The assistant needs a model API key only to answer *new* questions. Put one in
`.env` (gitignored):

```bash
DEEPSEEK_API_KEY=sk-...        # or ASSISTANT_PROVIDER=openai + OPENAI_API_KEY
```

## Background

Built by a data engineer and former wind turbine maintenance engineer, as a
portfolio project bridging data engineering and AI engineering.
