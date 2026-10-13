-- Gold: one row per wind farm per trading day.
--
-- Energy is the five-minute MW readings turned into MWh: a reading held for
-- five minutes is MW x 5/60.
--
-- Capacity factor divides that by a full 24 hours at registered capacity, as
-- the conventional definition does. A sealed part-day therefore reads low, by
-- design rather than by accident, which is why is_complete and intervals
-- travel with every row: a reader comparing days must be able to see that one
-- of them is short. (The quality check scales by intervals present instead,
-- so an incomplete day does not raise a false alarm there.)
--
-- Parameters: $wind_glob

SELECT
    trading_date,
    duid,
    any_value(station_name) AS station_name,
    any_value(region) AS region,
    any_value(capacity_mw) AS capacity_mw,
    sum(mw) * 5.0 / 60.0 AS energy_mwh,
    CASE
        WHEN any_value(capacity_mw) > 0
        THEN (sum(mw) * 5.0 / 60.0) / (any_value(capacity_mw) * 24.0)
    END AS capacity_factor,
    max(mw) AS peak_mw,
    min(mw) AS min_mw,
    count(DISTINCT interval_start_utc) AS intervals,
    any_value(is_complete) AS is_complete
FROM read_parquet($wind_glob)
GROUP BY trading_date, duid
ORDER BY trading_date, duid
