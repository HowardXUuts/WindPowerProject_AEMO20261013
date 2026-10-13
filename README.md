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
| **silver** | Wind only. Typed, deduplicated, validated. UTC and NEM local time. One file per day. |
| **gold** | Aggregates: generation by farm and state, capacity factors, revenue, capture price, negative-price exposure. |

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
is next.

See [`PLAN.md`](PLAN.md) for the repository layout, design decisions, and build
steps.

## Background

Built by a data engineer and former wind turbine maintenance engineer, as a
portfolio project bridging data engineering and AI engineering.
