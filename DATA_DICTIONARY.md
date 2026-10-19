# Data dictionary

**Generated — do not edit.** Run `python scripts/data_dictionary.py` to refresh;
`--check` fails if it is out of date. Purpose lines come from each model's own
header comment, bronze columns from the `TableSpec`s the ingesters declare, and
gold and silver columns from the Parquet on disk. Schema only: row counts and
date spans change on every ingest and would make `--check` fail hourly.

See [`CLAUDE.md`](CLAUDE.md) for the layer contract these tables obey:
bronze is a faithful copy and immutable, silver is wind-only and immutable per
day, gold is rewritten in full every run.

## Bronze

Every DUID and region as AEMO published it, one Parquet per closed NEM hour.
`source_file` and `ingested_at` are added to every table by the writer.

### `scada`

Parsed from `Dispatch_SCADA`.

| Column | Type | AEMO source |
|---|---|---|
| `settlementdate` | TIMESTAMPTZ | `SETTLEMENTDATE` |
| `duid` | VARCHAR | `DUID` |
| `scadavalue` | DOUBLE | `SCADAVALUE` |

### `price`

Parsed from `DispatchIS_Reports`.

| Column | Type | AEMO source |
|---|---|---|
| `settlementdate` | TIMESTAMPTZ | `SETTLEMENTDATE` |
| `regionid` | VARCHAR | `REGIONID` |
| `intervention` | INTEGER | `INTERVENTION` |
| `runno` | INTEGER | `RUNNO` |
| `dispatchinterval` | VARCHAR | `DISPATCHINTERVAL` |
| `rrp` | DOUBLE | `RRP` |
| `eep` | DOUBLE | `EEP` |
| `rop` | DOUBLE | `ROP` |
| `apcflag` | INTEGER | `APCFLAG` |
| `marketsuspendedflag` | INTEGER | `MARKETSUSPENDEDFLAG` |
| `lastchanged` | TIMESTAMPTZ | `LASTCHANGED` |

### `regionsum`

Parsed from `DispatchIS_Reports`.

| Column | Type | AEMO source |
|---|---|---|
| `settlementdate` | TIMESTAMPTZ | `SETTLEMENTDATE` |
| `regionid` | VARCHAR | `REGIONID` |
| `intervention` | INTEGER | `INTERVENTION` |
| `totaldemand` | DOUBLE | `TOTALDEMAND` |
| `availablegeneration` | DOUBLE | `AVAILABLEGENERATION` |
| `netinterchange` | DOUBLE | `NETINTERCHANGE` |
| `ss_wind_uigf` | DOUBLE | `SS_WIND_UIGF (optional)` |
| `ss_solar_uigf` | DOUBLE | `SS_SOLAR_UIGF (optional)` |
| `bdu_energy_storage` | DOUBLE | `BDU_ENERGY_STORAGE (optional)` |
| `bdu_clearedmw_gen` | DOUBLE | `BDU_CLEAREDMW_GEN (optional)` |
| `bdu_clearedmw_load` | DOUBLE | `BDU_CLEAREDMW_LOAD (optional)` |
| `lastchanged` | TIMESTAMPTZ | `LASTCHANGED` |

### `interconnector`

Parsed from `DispatchIS_Reports`.

| Column | Type | AEMO source |
|---|---|---|
| `settlementdate` | TIMESTAMPTZ | `SETTLEMENTDATE` |
| `interconnectorid` | VARCHAR | `INTERCONNECTORID` |
| `intervention` | INTEGER | `INTERVENTION` |
| `mwflow` | DOUBLE | `MWFLOW` |
| `exportlimit` | DOUBLE | `EXPORTLIMIT` |
| `importlimit` | DOUBLE | `IMPORTLIMIT` |
| `lastchanged` | TIMESTAMPTZ | `LASTCHANGED` |

### `p5min`

Parsed from `P5_Reports`.

| Column | Type | AEMO source |
|---|---|---|
| `run_datetime` | TIMESTAMPTZ | `RUN_DATETIME` |
| `lastchanged` | TIMESTAMPTZ | `LASTCHANGED` |
| `interval_datetime` | TIMESTAMPTZ | `INTERVAL_DATETIME` |
| `regionid` | VARCHAR | `REGIONID` |
| `intervention` | INTEGER | `INTERVENTION` |
| `rrp` | DOUBLE | `RRP` |
| `totaldemand` | DOUBLE | `TOTALDEMAND` |
| `ss_wind_uigf` | DOUBLE | `SS_WIND_UIGF` |
| `ss_solar_uigf` | DOUBLE | `SS_SOLAR_UIGF` |

### `predispatch_price`

Parsed from `PredispatchIS_Reports`.

| Column | Type | AEMO source |
|---|---|---|
| `predispatchseqno` | VARCHAR | `PREDISPATCHSEQNO` |
| `lastchanged` | TIMESTAMPTZ | `LASTCHANGED` |
| `runno` | INTEGER | `RUNNO` |
| `datetime` | TIMESTAMPTZ | `DATETIME` |
| `periodid` | VARCHAR | `PERIODID` |
| `regionid` | VARCHAR | `REGIONID` |
| `intervention` | INTEGER | `INTERVENTION` |
| `rrp` | DOUBLE | `RRP` |

### `predispatch_region`

Parsed from `PredispatchIS_Reports`.

| Column | Type | AEMO source |
|---|---|---|
| `predispatchseqno` | VARCHAR | `PREDISPATCHSEQNO` |
| `lastchanged` | TIMESTAMPTZ | `LASTCHANGED` |
| `runno` | INTEGER | `RUNNO` |
| `datetime` | TIMESTAMPTZ | `DATETIME` |
| `periodid` | VARCHAR | `PERIODID` |
| `regionid` | VARCHAR | `REGIONID` |
| `intervention` | INTEGER | `INTERVENTION` |
| `totaldemand` | DOUBLE | `TOTALDEMAND` |
| `availablegeneration` | DOUBLE | `AVAILABLEGENERATION` |
| `netinterchange` | DOUBLE | `NETINTERCHANGE` |
| `ss_wind_uigf` | DOUBLE | `SS_WIND_UIGF` |
| `ss_solar_uigf` | DOUBLE | `SS_SOLAR_UIGF` |

## Silver

Wind only, typed and deduplicated, UTC and NEM local time. One immutable file per trading date.

### `price`

regional dispatch price for one trading date.

| Column | Type |
|---|---|
| `trading_date` | DATE |
| `interval_start_utc` | TIMESTAMP WITH TIME ZONE |
| `interval_start_nem` | TIMESTAMP |
| `interval_end_utc` | TIMESTAMP WITH TIME ZONE |
| `interval_end_nem` | TIMESTAMP |
| `region` | VARCHAR |
| `rrp` | DOUBLE |
| `is_negative` | BOOLEAN |
| `intervention` | INTEGER |
| `apcflag` | INTEGER |
| `marketsuspendedflag` | INTEGER |
| `source_file` | VARCHAR |
| `intervals_present` | SMALLINT |
| `is_complete` | BOOLEAN |

### `wind_output`

wind farm output for one trading date.

| Column | Type |
|---|---|
| `trading_date` | DATE |
| `interval_start_utc` | TIMESTAMP WITH TIME ZONE |
| `interval_start_nem` | TIMESTAMP |
| `interval_end_utc` | TIMESTAMP WITH TIME ZONE |
| `interval_end_nem` | TIMESTAMP |
| `duid` | VARCHAR |
| `station_name` | VARCHAR |
| `region` | VARCHAR |
| `capacity_mw` | DOUBLE |
| `mw` | DOUBLE |
| `is_negative` | BOOLEAN |
| `is_above_capacity` | BOOLEAN |
| `source_file` | VARCHAR |
| `intervals_present` | SMALLINT |
| `is_complete` | BOOLEAN |

## Gold

Aggregates and dashboard-ready tables, rewritten in full each run.

### `cannibalisation`

what wind earns as a function of how much wind is blowing.

| Column | Type |
|---|---|
| `region` | VARCHAR |
| `bucket` | INTEGER |
| `bucket_label` | VARCHAR |
| `intervals` | BIGINT |
| `median_penetration_pct` | DOUBLE |
| `median_solar_penetration_pct` | DOUBLE |
| `median_vre_penetration_pct` | DOUBLE |
| `avg_rrp_aud_mwh` | DOUBLE |
| `capture_price_aud_mwh` | DOUBLE |
| `wind_energy_mwh` | DOUBLE |
| `pct_of_wind_energy` | DOUBLE |
| `pct_intervals_negative` | DOUBLE |
| `first_date` | DATE |
| `last_date` | DATE |

### `capture_price`

wind capture price against the time-weighted average, per region per day.

| Column | Type |
|---|---|
| `trading_date` | DATE |
| `region` | VARCHAR |
| `capture_price_aud_mwh` | DOUBLE |
| `twap_aud_mwh` | DOUBLE |
| `capture_ratio` | DOUBLE |
| `wind_energy_mwh` | DOUBLE |
| `intervals` | BIGINT |
| `price_intervals` | BIGINT |

### `demand_shape`

when in the day does wind arrive, relative to the demand it is sold into?

| Column | Type |
|---|---|
| `region` | VARCHAR |
| `hour_nem` | BIGINT |
| `mean_wind_mw` | DOUBLE |
| `mean_demand_mw` | DOUBLE |
| `penetration_pct` | DOUBLE |
| `days` | BIGINT |
| `intervals` | BIGINT |

### `farm_correlation`

each farm's closest and most independent counterparts.

| Column | Type |
|---|---|
| `duid` | VARCHAR |
| `station_name` | VARCHAR |
| `region` | VARCHAR |
| `relation` | VARCHAR |
| `rank` | BIGINT |
| `other_duid` | VARCHAR |
| `other_name` | VARCHAR |
| `other_region` | VARCHAR |
| `same_region` | BOOLEAN |
| `r` | DOUBLE |
| `intervals` | BIGINT |

### `farm_daily`

one row per wind farm per trading day.

| Column | Type |
|---|---|
| `trading_date` | DATE |
| `duid` | VARCHAR |
| `station_name` | VARCHAR |
| `region` | VARCHAR |
| `capacity_mw` | DOUBLE |
| `energy_mwh` | DOUBLE |
| `capacity_factor` | DOUBLE |
| `peak_mw` | DOUBLE |
| `min_mw` | DOUBLE |
| `intervals` | BIGINT |
| `is_complete` | BOOLEAN |

### `farm_revenue_daily`

what each farm's output was worth, per trading day.

| Column | Type |
|---|---|
| `trading_date` | DATE |
| `duid` | VARCHAR |
| `station_name` | VARCHAR |
| `region` | VARCHAR |
| `capacity_mw` | DOUBLE |
| `energy_mwh` | DOUBLE |
| `revenue_aud` | DOUBLE |
| `realised_price_aud_mwh` | DOUBLE |
| `negative_price_mwh` | DOUBLE |
| `negative_price_revenue_aud` | DOUBLE |
| `intervals` | BIGINT |

### `latest_snapshot`

where the fleet is right now, one row per wind farm.

| Column | Type |
|---|---|
| `interval_start_utc` | TIMESTAMP WITH TIME ZONE |
| `interval_start_nem` | TIMESTAMP |
| `duid` | VARCHAR |
| `station_name` | VARCHAR |
| `region` | VARCHAR |
| `capacity_mw` | DOUBLE |
| `mw` | DOUBLE |
| `pct_of_capacity` | DOUBLE |
| `source_layer` | VARCHAR |

### `negative_price_runs`

how long negative prices last, not just how often they happen.

| Column | Type |
|---|---|
| `region` | VARCHAR |
| `bucket` | INTEGER |
| `bucket_label` | VARCHAR |
| `runs` | BIGINT |
| `intervals` | DOUBLE |
| `pct_of_negative_intervals` | DOUBLE |
| `pct_of_runs` | DOUBLE |
| `median_run_minutes` | DOUBLE |
| `longest_run_minutes` | BIGINT |
| `deepest_rrp` | DOUBLE |
| `wind_energy_mwh` | DOUBLE |
| `pct_of_negative_energy` | DOUBLE |
| `revenue_aud` | DOUBLE |
| `first_date` | DATE |
| `last_date` | DATE |

### `negative_price_worst`

the longest negative-price events, named and dated.

| Column | Type |
|---|---|
| `region` | VARCHAR |
| `rank` | BIGINT |
| `started_utc` | TIMESTAMP WITH TIME ZONE |
| `started_nem` | TIMESTAMP |
| `ended_nem` | TIMESTAMP |
| `intervals` | BIGINT |
| `duration_hours` | DOUBLE |
| `mean_rrp` | DOUBLE |
| `deepest_rrp` | DOUBLE |
| `wind_energy_mwh` | DOUBLE |
| `revenue_aud` | DOUBLE |
| `is_open` | BOOLEAN |

### `output_correlation`

do two wind farms generate at the same time?

| Column | Type |
|---|---|
| `region_a` | VARCHAR |
| `region_b` | VARCHAR |
| `is_same_region` | BOOLEAN |
| `farm_pairs` | BIGINT |
| `mean_r` | DOUBLE |
| `median_r` | DOUBLE |
| `min_r` | DOUBLE |
| `max_r` | DOUBLE |
| `portfolio_r` | DOUBLE |
| `min_intervals` | BIGINT |
| `farms_excluded` | BIGINT |
| `first_date` | DATE |
| `last_date` | DATE |

### `region_hourly`

regional wind output by hour, for the dashboard's shape-of-the-day.

| Column | Type |
|---|---|
| `trading_date` | DATE |
| `region` | VARCHAR |
| `hour_nem` | TIMESTAMP |
| `hour_start_utc` | TIMESTAMP WITH TIME ZONE |
| `avg_mw` | DOUBLE |
| `peak_mw` | DOUBLE |
| `min_mw` | DOUBLE |
| `energy_mwh` | DOUBLE |
| `intervals` | BIGINT |

### `revenue_concentration`

how much of wind revenue arrives in how few intervals.

| Column | Type |
|---|---|
| `region` | VARCHAR |
| `intervals` | BIGINT |
| `revenue_aud` | DOUBLE |
| `wind_energy_mwh` | DOUBLE |
| `top_1pct_intervals` | BIGINT |
| `pct_from_top_1pct` | DOUBLE |
| `pct_from_top_5pct` | DOUBLE |
| `pct_from_top_10pct` | DOUBLE |
| `negative_revenue_aud` | DOUBLE |
| `pct_intervals_negative` | DOUBLE |
| `first_date` | DATE |
| `last_date` | DATE |

### `quality`

One row per data-quality finding, plus a row for every check that found
nothing, so a reader can tell "ran and passed" from "did not run".
Written by `scripts/check_quality.py` from `sql/quality/`, not from a
`sql/gold/` model.

| Column | Type |
|---|---|
| `checked_at` | TIMESTAMP WITH TIME ZONE |
| `check_name` | VARCHAR |
| `severity` | VARCHAR |
| `status` | VARCHAR |
| `trading_date` | DATE |
| `subject` | VARCHAR |
| `observed` | DOUBLE |
| `message` | VARCHAR |
