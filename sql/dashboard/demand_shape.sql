-- Dashboard: wind as a share of operational demand, by hour of the NEM day.
-- Parameters: $demand_shape
SELECT
    region,
    hour_nem,
    mean_wind_mw,
    mean_demand_mw,
    penetration_pct,
    days
FROM read_parquet($demand_shape)
ORDER BY region, hour_nem
