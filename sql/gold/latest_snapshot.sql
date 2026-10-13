-- Gold: where the fleet is right now, one row per wind farm.
--
-- Silver only exists for days bronze has finished, so a snapshot built from
-- silver alone would be up to two days stale — useless for the thing a
-- dashboard most wants to show. This reads silver AND the raw bronze hours,
-- filtering bronze to wind in memory using the registry. The filtered bronze
-- is never written anywhere; only this snapshot is.
--
-- Bronze and silver overlap for days silver has already covered, so the union
-- is deduplicated on (interval_start, duid), preferring silver because it has
-- already been through the dedup and typing the silver model applies.
--
-- Parameters: $wind_glob, $bronze_glob, $registry

WITH scheduled AS (
    SELECT duid, station_name, region, capacity_mw
    FROM read_csv($registry, header = true, auto_detect = true)
    WHERE lower(CAST(in_scada AS VARCHAR)) = 'true'
),

from_silver AS (
    SELECT interval_start_utc, interval_start_nem, duid, mw, 0 AS preference
    FROM read_parquet($wind_glob)
),

from_bronze AS (
    SELECT
        settlementdate - INTERVAL 5 MINUTE AS interval_start_utc,
        (settlementdate - INTERVAL 5 MINUTE)
            AT TIME ZONE 'Australia/Brisbane' AS interval_start_nem,
        duid,
        scadavalue AS mw,
        1 AS preference
    FROM read_parquet($bronze_glob)
    WHERE duid IN (SELECT duid FROM scheduled)
),

combined AS (
    SELECT * FROM from_silver
    UNION ALL
    SELECT * FROM from_bronze
    QUALIFY row_number() OVER (
        PARTITION BY interval_start_utc, duid ORDER BY preference
    ) = 1
),

newest AS (
    SELECT max(interval_start_utc) AS interval_start_utc FROM combined
)

SELECT
    c.interval_start_utc,
    c.interval_start_nem,
    s.duid,
    s.station_name,
    s.region,
    s.capacity_mw,
    c.mw,
    CASE
        WHEN s.capacity_mw > 0 THEN c.mw / s.capacity_mw
    END AS pct_of_capacity,
    -- Says which layer the reading came from, so a stale snapshot is visible
    -- rather than merely suspected.
    CASE WHEN c.preference = 0 THEN 'silver' ELSE 'bronze' END AS source_layer
FROM combined c
JOIN scheduled s USING (duid)
WHERE c.interval_start_utc = (SELECT interval_start_utc FROM newest)
ORDER BY c.mw DESC NULLS LAST, s.duid
