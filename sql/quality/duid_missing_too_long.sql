-- SOFT: a scheduled farm absent from more than two hours of a day.
--
-- Only DUIDs flagged in_scada are considered, so a non-scheduled farm that
-- never appears in DISPATCHSCADA does not look like a gap. A real absence is
-- usually an outage or a commissioning window rather than a pipeline fault,
-- which is why this warns rather than fails — but a farm silently missing for
-- hours quietly drags down every fleet total it belongs to.
--
-- Measured against the intervals the day actually holds, not a flat 288, so a
-- sealed part-day does not report every farm as missing.
--
-- Parameters: $wind_glob, $registry, $max_missing_intervals

WITH per_day AS (
    SELECT trading_date, count(DISTINCT interval_start_utc) AS expected
    FROM read_parquet($wind_glob)
    GROUP BY trading_date
),

scheduled AS (
    SELECT duid, station_name
    FROM read_csv($registry, header = true, auto_detect = true)
    WHERE lower(CAST(in_scada AS VARCHAR)) = 'true'
),

present AS (
    SELECT trading_date, duid, count(DISTINCT interval_start_utc) AS found
    FROM read_parquet($wind_glob)
    GROUP BY trading_date, duid
)

SELECT
    d.trading_date,
    s.duid AS subject,
    CAST(d.expected - coalesce(p.found, 0) AS DOUBLE) AS observed,
    s.station_name || ' missing ' || (d.expected - coalesce(p.found, 0))
        || ' of ' || d.expected || ' intervals ('
        || round((d.expected - coalesce(p.found, 0)) * 5.0 / 60.0, 1) || ' hours)' AS message
FROM per_day d
CROSS JOIN scheduled s
LEFT JOIN present p ON p.trading_date = d.trading_date AND p.duid = s.duid
WHERE d.expected - coalesce(p.found, 0) > $max_missing_intervals
ORDER BY d.trading_date, observed DESC
