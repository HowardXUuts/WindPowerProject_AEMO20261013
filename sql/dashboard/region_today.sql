-- Dashboard: regional wind output by hour, for the most recent trading date.
-- Parameters: $region_hourly
SELECT
    CAST(trading_date AS VARCHAR) AS trading_date,
    region,
    strftime(hour_nem, '%H:%M') AS hour,
    round(avg_mw, 1) AS avg_mw,
    round(energy_mwh, 1) AS energy_mwh
FROM read_parquet($region_hourly)
WHERE trading_date = (SELECT max(trading_date) FROM read_parquet($region_hourly))
ORDER BY region, hour_nem
