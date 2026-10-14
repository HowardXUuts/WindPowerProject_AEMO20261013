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

```
 Dispatch_SCADA  ─┐                                                   dashboard
  MW per DUID    │                                                     (Pages)
                 ├─▶ raw zips ──▶ bronze ──▶ silver ──▶ gold ──▶      │
 DispatchIS      │   transient     write-once   wind only   joined        ▼
  $/MWh per      ┘   (not kept)    per file     typed &     volume   Q&A assistant
  region                                         validated   x price  (Claude API)
```

| Layer | What it holds |
|-------|---------------|
| **bronze** | Both feeds as published, every DUID and region. One Parquet file per closed NEM hour, never rewritten. |
| **silver** | Wind only. Interval start and end in UTC and NEM local, deduplicated, quality-flagged. One immutable file per trading date. |
| **gold** | Aggregates, rewritten in full each run: `farm_daily`, `farm_revenue_daily`, `capture_price`, `region_hourly`, `latest_snapshot`, `quality`. |

## Stack

Python 3.11 · DuckDB · Parquet · GitHub Actions · GitHub Pages · Claude API

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
with its region's price shape. The Q&A assistant is next.

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
pytest -q                                    # 191 tests, no network needed
python scripts/export_dashboard.py           # gold + silver -> docs/data/
python -m http.server 8000 --directory docs  # then open localhost:8000
```

## Background

Built by a data engineer and former wind turbine maintenance engineer, as a
portfolio project bridging data engineering and AI engineering.
