-- Silver: wind farm output for one trading date.
--
-- Bronze holds every DUID in the NEM as published. This keeps the wind farms,
-- attaches their station, region and registered capacity, and restates AEMO's
-- interval-ending timestamps as an explicit start and end in both UTC and NEM
-- local time.
--
-- Nothing is dropped for being surprising: negative output and output above
-- registered capacity are flagged, not filtered. A negative reading is a farm
-- drawing auxiliary power or being curtailed, and an above-capacity reading is
-- usually a genuine over-rating rather than an error. Dropping either would
-- quietly bias every downstream total.
--
-- Parameters: $scada_glob, $registry, $trading_date, $intervals_present,
--             $is_complete

WITH registry AS (
    SELECT duid, station_name, region, capacity_mw
    FROM read_csv($registry, header = true, auto_detect = true)
    -- Non-scheduled farms are registered but never appear in DISPATCHSCADA.
    -- Excluding them here keeps a structurally absent unit from looking like
    -- an ingestion gap in the quality checks.
    WHERE lower(CAST(in_scada AS VARCHAR)) = 'true'
),

bronze AS (
    SELECT
        -- AEMO stamps a dispatch interval with the time it ENDS, so the five
        -- minutes of generation this row measures began five minutes earlier.
        settlementdate AS interval_end_utc,
        settlementdate - INTERVAL 5 MINUTE AS interval_start_utc,
        duid,
        scadavalue,
        source_file,
        ingested_at
    FROM read_parquet($scada_glob)
),

dated AS (
    SELECT
        *,
        -- NEM market time is AEST, UTC+10 all year. Brisbane is the one
        -- Australian zone that never observes daylight saving, so it tracks
        -- market time exactly; Sydney and Melbourne do not.
        interval_start_utc AT TIME ZONE 'Australia/Brisbane' AS interval_start_nem,
        interval_end_utc AT TIME ZONE 'Australia/Brisbane' AS interval_end_nem,
        -- Dated by when the interval STARTED. The interval ending 00:00 began
        -- at 23:55 and therefore belongs to the previous day.
        CAST(interval_start_utc AT TIME ZONE 'Australia/Brisbane' AS DATE) AS trading_date
    FROM bronze
),

deduped AS (
    SELECT *
    FROM dated
    WHERE trading_date = $trading_date
    -- AEMO occasionally republishes an interval. Keep the most recently
    -- ingested copy, breaking ties on file name so the result is deterministic.
    QUALIFY row_number() OVER (
        PARTITION BY interval_start_utc, duid
        ORDER BY ingested_at DESC, source_file DESC
    ) = 1
)

SELECT
    d.trading_date,
    d.interval_start_utc,
    d.interval_start_nem,
    d.interval_end_utc,
    d.interval_end_nem,
    d.duid,
    r.station_name,
    r.region,
    r.capacity_mw,
    d.scadavalue AS mw,
    d.scadavalue < 0 AS is_negative,
    -- 2% tolerance: registered capacity is a nameplate figure, and a farm
    -- running slightly over it on a cold windy night is normal. Anything
    -- beyond that is worth a look.
    (r.capacity_mw IS NOT NULL AND d.scadavalue > 1.02 * r.capacity_mw) AS is_above_capacity,
    d.source_file,
    -- Day-level completeness, carried on every row so a reader never has to
    -- consult a sidecar to know whether the day is whole. Dictionary encoding
    -- makes the repetition free.
    CAST($intervals_present AS SMALLINT) AS intervals_present,
    CAST($is_complete AS BOOLEAN) AS is_complete
FROM deduped d
JOIN registry r USING (duid)
ORDER BY d.interval_start_utc, d.duid
