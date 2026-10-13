-- SOFT: readings above registered capacity plus tolerance.
--
-- Silver flags these rather than dropping them, because a farm running over
-- nameplate on a cold windy night is real. Sustained over-capacity usually
-- means the registry's capacity is stale rather than the reading being wrong,
-- so this warns and points at the farm to check.
--
-- Parameters: $wind_glob

SELECT
    trading_date,
    duid AS subject,
    CAST(count(*) AS DOUBLE) AS observed,
    any_value(station_name) || ': ' || count(*) || ' readings above 1.02x capacity, peak '
        || round(max(mw), 1) || ' MW against ' || any_value(capacity_mw) || ' MW registered'
        AS message
FROM read_parquet($wind_glob)
WHERE is_above_capacity
GROUP BY trading_date, duid
ORDER BY trading_date, observed DESC
