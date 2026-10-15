# Gold schema

The six tables the assistant may query. Every one is a DuckDB table loaded from
a Parquet file in `data/gold/`; query them by the bare names below.

Units are in the column names: `_mw` is megawatts (an instantaneous rate),
`_mwh` is megawatt hours (energy), `_aud` is Australian dollars.

The five NEM regions are `NSW1`, `QLD1`, `SA1`, `TAS1`, `VIC1` — note the
trailing `1` on every one. Only wind farms are present; no solar, coal, gas,
hydro or battery units appear anywhere in these tables.

A **trading date** is the NEM date (AEST, UTC+10, no daylight saving). An
interval is five minutes and is stamped by its **start**.

---

## farm_daily — one row per wind farm per trading date

| Column | Type | Meaning |
|---|---|---|
| `trading_date` | DATE | NEM date |
| `duid` | VARCHAR | AEMO's unit identifier, e.g. `ARWF1`. Primary key with `trading_date` |
| `station_name` | VARCHAR | Human name, e.g. `Ararat Wind Farm` |
| `region` | VARCHAR | NEM region |
| `capacity_mw` | DOUBLE | Registered (nameplate) capacity |
| `energy_mwh` | DOUBLE | Energy generated that day |
| `capacity_factor` | DOUBLE | `energy_mwh / (capacity_mw * 24)`. A fraction, not a percentage: 0.35 means 35% |
| `peak_mw` | DOUBLE | Highest five-minute reading |
| `min_mw` | DOUBLE | Lowest five-minute reading. Can be slightly negative — a farm drawing house load |
| `intervals` | BIGINT | Five-minute intervals present. 288 is a full day |
| `is_complete` | BOOLEAN | False when `intervals < 288`. **A short day has a low capacity factor for a pipeline reason, not a wind reason** — filter on this before ranking or comparing days |

## farm_revenue_daily — the same grain, with what the energy earned

| Column | Type | Meaning |
|---|---|---|
| `trading_date`, `duid`, `station_name`, `region`, `capacity_mw`, `energy_mwh` | | as above |
| `revenue_aud` | DOUBLE | Energy settled interval by interval at the regional price. Can be negative |
| `realised_price_aud_mwh` | DOUBLE | `revenue_aud / energy_mwh` — the **capture price**, what the farm actually earned per MWh. Almost never equals the day's average price |
| `negative_price_mwh` | DOUBLE | Energy generated while the regional price was below zero — volume the farm paid to produce |
| `negative_price_revenue_aud` | DOUBLE | The (negative) revenue from those intervals |
| `intervals` | BIGINT | Intervals that joined to a price |

## capture_price — one row per region per trading date

| Column | Type | Meaning |
|---|---|---|
| `trading_date`, `region` | | |
| `capture_price_aud_mwh` | DOUBLE | Output-weighted price: what the region's wind earned per MWh |
| `twap_aud_mwh` | DOUBLE | Time-weighted average price: what a generator producing evenly all day would have earned |
| `capture_ratio` | DOUBLE | `capture_price / twap`. **Below 1.0 is the cannibalisation discount** — wind generates when other wind generates, and the price falls when it does |
| `wind_energy_mwh` | DOUBLE | Regional wind energy that day |
| `intervals`, `price_intervals` | BIGINT | Coverage counts |

## region_hourly — one row per region per NEM hour

| Column | Type | Meaning |
|---|---|---|
| `trading_date`, `region` | | |
| `hour_nem` | TIMESTAMP | Hour start, NEM local time (no zone attached) |
| `hour_start_utc` | TIMESTAMPTZ | The same instant in UTC |
| `avg_mw`, `peak_mw`, `min_mw` | DOUBLE | Over the hour |
| `energy_mwh` | DOUBLE | Energy in the hour |
| `intervals` | BIGINT | 12 is a full hour |

## latest_snapshot — where the fleet is right now, one row per farm

| Column | Type | Meaning |
|---|---|---|
| `interval_start_utc` | TIMESTAMPTZ | The newest interval available |
| `interval_start_nem` | TIMESTAMP | The same, NEM local |
| `duid`, `station_name`, `region`, `capacity_mw` | | |
| `mw` | DOUBLE | Output in that interval |
| `pct_of_capacity` | DOUBLE | `mw / capacity_mw`, a fraction |
| `source_layer` | VARCHAR | `silver` or `bronze` — bronze means fresher than the last completed day |

This table holds **one interval only**. It answers "now", never "over time".

## quality — the result of the last data-quality run

| Column | Type | Meaning |
|---|---|---|
| `checked_at` | TIMESTAMPTZ | When the check ran |
| `check_name` | VARCHAR | e.g. `price_out_of_band` |
| `severity` | VARCHAR | `hard` fails the pipeline, `soft` warns |
| `status` | VARCHAR | `pass`, `warn`, `fail` or `skip` |
| `trading_date` | DATE | The day a finding concerns; NULL when none |
| `subject` | VARCHAR | The farm, region or interval at fault |
| `observed` | DOUBLE | The offending value |
| `message` | VARCHAR | One line of explanation |

Every check writes a row even when it finds nothing, so "ran and passed" is
distinguishable from "did not run".

---

## Things that are easy to get wrong

- **Capacity factor and `pct_of_capacity` are fractions.** Multiply by 100 to
  present a percentage.
- **Rank on `revenue_aud` or `realised_price_aud_mwh`, not `capacity_factor`,**
  when the question is about money. The live data has farms with the fleet's
  best capacity factors earning negative revenue.
- **Exclude incomplete days** (`is_complete = false`) when comparing or ranking
  days, unless the question is specifically about data coverage.
- **A regional total means summing farms first**, then aggregating — `region_hourly`
  and `capture_price` already do this, so prefer them over summing `farm_daily`
  when the question is regional.
- **"The latest day" is `max(trading_date)`**, which may be incomplete; "right
  now" is `latest_snapshot`.
- There is no table of five-minute per-farm output in gold. If a question needs
  one, say so rather than inventing a table.
