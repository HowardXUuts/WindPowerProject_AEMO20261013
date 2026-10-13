-- HARD: the silver grain must be unique.
--
-- Silver deduplicates on (interval_start, duid), so a duplicate here means the
-- QUALIFY in the silver model stopped working. Every downstream sum would be
-- inflated by exactly the duplicated rows, invisibly.
--
-- Parameters: $wind_glob

SELECT
    trading_date,
    duid AS subject,
    CAST(count(*) AS DOUBLE) AS observed,
    'duplicate grain: ' || count(*) || ' rows for ' || duid
        || ' at ' || strftime(interval_start_nem, '%Y-%m-%d %H:%M') AS message
FROM read_parquet($wind_glob)
GROUP BY trading_date, interval_start_utc, interval_start_nem, duid
HAVING count(*) > 1
ORDER BY trading_date, duid
