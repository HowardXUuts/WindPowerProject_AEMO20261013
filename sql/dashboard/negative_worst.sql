-- Dashboard: the longest negative-price events, longest first across all regions.
-- Parameters: $negative_price_worst
SELECT
    region,
    rank,
    strftime(started_nem, '%Y-%m-%d %H:%M') AS started_nem,
    strftime(ended_nem, '%H:%M') AS ended_nem,
    duration_hours,
    intervals,
    round(mean_rrp, 2) AS mean_rrp,
    round(deepest_rrp, 2) AS deepest_rrp,
    round(wind_energy_mwh) AS wind_energy_mwh,
    round(revenue_aud) AS revenue_aud,
    is_open
FROM read_parquet($negative_price_worst)
ORDER BY duration_hours DESC, region
