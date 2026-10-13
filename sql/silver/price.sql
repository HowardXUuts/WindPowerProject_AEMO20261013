-- Silver: regional dispatch price for one trading date.
--
-- Same timestamp treatment as wind output, so the two join cleanly on
-- (interval_start_utc, region) once gold needs revenue.
--
-- Where AEMO intervenes in the market it publishes two runs for the same
-- interval: a physical run describing what the system actually did, and a
-- pricing run describing what generators are settled against. Revenue follows
-- the pricing run, so that is the one kept here. Bronze holds both, so the
-- choice stays reversible.
--
-- Parameters: $price_glob, $trading_date, $intervals_present, $is_complete

WITH bronze AS (
    SELECT
        settlementdate AS interval_end_utc,
        settlementdate - INTERVAL 5 MINUTE AS interval_start_utc,
        regionid,
        rrp,
        intervention,
        apcflag,
        marketsuspendedflag,
        source_file,
        ingested_at
    FROM read_parquet($price_glob)
),

dated AS (
    SELECT
        *,
        interval_start_utc AT TIME ZONE 'Australia/Brisbane' AS interval_start_nem,
        interval_end_utc AT TIME ZONE 'Australia/Brisbane' AS interval_end_nem,
        CAST(interval_start_utc AT TIME ZONE 'Australia/Brisbane' AS DATE) AS trading_date
    FROM bronze
),

deduped AS (
    SELECT *
    FROM dated
    WHERE trading_date = $trading_date
    -- intervention = 0 is the pricing run, so it sorts first and wins whenever
    -- an interval carries both. Ingestion time breaks any remaining tie.
    QUALIFY row_number() OVER (
        PARTITION BY interval_start_utc, regionid
        ORDER BY intervention ASC, ingested_at DESC, source_file DESC
    ) = 1
)

SELECT
    trading_date,
    interval_start_utc,
    interval_start_nem,
    interval_end_utc,
    interval_end_nem,
    regionid AS region,
    rrp,
    rrp < 0 AS is_negative,
    -- Kept so a reader can see the row came from an intervention interval even
    -- though the physical run was discarded.
    intervention,
    apcflag,
    marketsuspendedflag,
    source_file,
    CAST($intervals_present AS SMALLINT) AS intervals_present,
    CAST($is_complete AS BOOLEAN) AS is_complete
FROM deduped
ORDER BY interval_start_utc, region
