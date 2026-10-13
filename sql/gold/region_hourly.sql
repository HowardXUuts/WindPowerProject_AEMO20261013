-- Gold: regional wind output by hour, for the dashboard's shape-of-the-day.
--
-- Farms are summed to a regional figure per interval before averaging, so the
-- hourly average is the average of twelve regional totals rather than the
-- average across farm-intervals, which would weight an hour by how many farms
-- happened to report in it.
--
-- Hours are NEM local, because the question being asked is about the shape of
-- an Australian day.
--
-- Parameters: $wind_glob

WITH region_interval AS (
    SELECT
        trading_date,
        region,
        interval_start_utc,
        interval_start_nem,
        sum(mw) AS region_mw
    FROM read_parquet($wind_glob)
    GROUP BY trading_date, region, interval_start_utc, interval_start_nem
)

SELECT
    trading_date,
    region,
    date_trunc('hour', interval_start_nem) AS hour_nem,
    min(interval_start_utc) AS hour_start_utc,
    avg(region_mw) AS avg_mw,
    max(region_mw) AS peak_mw,
    min(region_mw) AS min_mw,
    sum(region_mw) * 5.0 / 60.0 AS energy_mwh,
    count(*) AS intervals
FROM region_interval
GROUP BY trading_date, region, date_trunc('hour', interval_start_nem)
ORDER BY trading_date, region, hour_nem
