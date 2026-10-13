-- HARD: a day claiming completeness must actually hold 288 intervals.
--
-- is_complete is what downstream consumers trust when deciding whether a daily
-- total is comparable to other days. A day short of 288 but flagged complete
-- would make a partial day look like a full one, understating it silently.
-- Sealed days are the legitimate short days, and they carry is_complete = false.
--
-- Parameters: $wind_glob, $intervals_per_day

WITH per_day AS (
    SELECT
        trading_date,
        count(DISTINCT interval_start_utc) AS intervals_found,
        any_value(is_complete) AS claimed_complete,
        any_value(intervals_present) AS claimed_intervals
    FROM read_parquet($wind_glob)
    GROUP BY trading_date
)

SELECT
    trading_date,
    NULL AS subject,
    CAST(intervals_found AS DOUBLE) AS observed,
    'day flagged complete with ' || intervals_found || '/' || $intervals_per_day
        || ' intervals (file claims ' || claimed_intervals || ')' AS message
FROM per_day
WHERE claimed_complete AND intervals_found < $intervals_per_day
ORDER BY trading_date
