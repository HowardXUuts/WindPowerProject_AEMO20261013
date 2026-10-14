-- Dashboard: five-minute output and the price it was settled at, for every
-- farm, on the most recent complete trading day.
--
-- Read from silver rather than gold on purpose. Gold's finest grain is hourly,
-- and an hourly mean hides exactly what this panel is for: the ramps, the
-- notches where a farm was constrained off, and the single intervals where the
-- price went negative. Gold is also committed and rewritten every run, so a
-- table of 96 farms x 288 intervals would add megabytes to git history hourly;
-- docs/data is gitignored and rebuilt at deploy time, so this costs nothing.
--
-- Every farm is returned in one query and split per farm by the exporter: 96
-- separate queries would each rescan the whole of silver.
--
-- Parameters: $wind_glob, $price_glob
WITH latest AS (
    SELECT max(trading_date) AS trading_date
    FROM read_parquet($wind_glob)
    WHERE is_complete
)

SELECT
    w.duid,
    CAST(w.trading_date AS VARCHAR) AS trading_date,
    strftime(w.interval_start_nem, '%H:%M') AS t,
    round(w.mw, 1) AS mw,
    round(p.rrp, 2) AS rrp
FROM read_parquet($wind_glob) w
JOIN latest l USING (trading_date)
LEFT JOIN read_parquet($price_glob) p
    ON p.interval_start_utc = w.interval_start_utc
   AND p.region = w.region
ORDER BY w.duid, w.interval_start_utc
