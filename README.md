# Australian Wind Power Data Platform

A serverless data platform for Australian wind generation, built on AEMO's
public market data.

Every five minutes AEMO publishes the metered output of every generating unit in
the National Electricity Market. This project collects that feed, keeps the wind
farms, and refines it into a queryable history of Australian wind generation —
then puts a dashboard and a natural-language Q&A assistant on top of it.

The whole thing runs on GitHub Actions and stores its data in this repository.
No cloud account, no database server, no paid services.

## Architecture

```
nemweb.com.au  ──▶  raw zips  ──▶  bronze  ──▶  silver  ──▶  gold  ──▶  dashboard
 DISPATCH_SCADA     transient      wind only    typed &      aggregates   (Pages)
 every 5 min        (not kept)     per hour     validated    & metrics        │
                                                                              ▼
                                                                     Q&A assistant
                                                                      (Claude API)
```

| Layer | What it holds |
|-------|---------------|
| **bronze** | Wind DUIDs only, source columns untouched. One Parquet file per UTC hour. |
| **silver** | Typed, deduplicated, validated. UTC and NEM local time. One file per day. |
| **gold** | Aggregates: generation by farm, by state, capacity factors, daily and monthly rollups. |

## Stack

Python 3.11 · DuckDB · Parquet · GitHub Actions · GitHub Pages · Claude API

## Data source

[AEMO NEMWeb Dispatch SCADA](https://nemweb.com.au/Reports/Current/Dispatch_SCADA/)
— `PUBLIC_DISPATCHSCADA_*.zip`, published every 5 minutes, containing MW output
per DUID (dispatchable unit identifier). Public and unauthenticated.

AEMO data is used under the terms of its
[copyright permissions](https://aemo.com.au/privacy-and-legal-notices/copyright-permissions).
This project is not affiliated with AEMO.

## Status

Bronze layer built and tested. See [`PLAN.md`](PLAN.md) for the repository layout,
design decisions, and build steps.

## Background

Built by a data engineer and former wind turbine maintenance engineer, as a
portfolio project bridging data engineering and AI engineering.
