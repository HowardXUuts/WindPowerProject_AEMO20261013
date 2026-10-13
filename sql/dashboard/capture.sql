-- Dashboard: capture price against the time-weighted average, by region.
-- Parameters: $capture
SELECT
    CAST(trading_date AS VARCHAR) AS trading_date,
    region,
    round(capture_price_aud_mwh, 2) AS capture_price_aud_mwh,
    round(twap_aud_mwh, 2) AS twap_aud_mwh,
    round(capture_ratio, 3) AS capture_ratio,
    round(wind_energy_mwh, 1) AS wind_energy_mwh
FROM read_parquet($capture)
ORDER BY trading_date, region
