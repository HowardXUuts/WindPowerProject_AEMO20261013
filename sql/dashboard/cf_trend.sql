-- Dashboard: daily capacity factor by region.
--
-- Capacity-weighted, not an average of farm capacity factors: a 500 MW farm and
-- a 20 MW farm are not equal votes on how the region performed.
--
-- Parameters: $farm_daily
SELECT
    CAST(trading_date AS VARCHAR) AS trading_date,
    region,
    round(sum(energy_mwh) / nullif(sum(capacity_mw) * 24.0, 0), 4) AS capacity_factor,
    round(sum(energy_mwh), 1) AS energy_mwh,
    bool_and(is_complete) AS is_complete,
    count(*) AS farms
FROM read_parquet($farm_daily)
GROUP BY trading_date, region
ORDER BY trading_date, region
