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
--
-- Float aggregates are rounded so the committed bytes do not move when the data
-- has not. DuckDB sums in parallel, so the same inputs can differ in the last
-- bit; gold is rewritten and committed every hour, so that drift alone was
-- adding a Parquet blob to git history on every run. Precision is matched to
-- each column's magnitude, because the drift is relative.
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
    round(avg(region_mw), 4) AS avg_mw,
    round(max(region_mw), 4) AS peak_mw,
    round(min(region_mw), 4) AS min_mw,
    round(sum(region_mw) * 5.0 / 60.0, 3) AS energy_mwh,
    count(*) AS intervals
FROM region_interval
GROUP BY trading_date, region, date_trunc('hour', interval_start_nem)
ORDER BY trading_date, region, hour_nem
