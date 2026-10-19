-- Dashboard: price received against wind penetration, with the solar control.
-- Parameters: $cannibalisation
SELECT
    region,
    bucket,
    bucket_label,
    intervals,
    round(median_penetration_pct, 1) AS median_penetration_pct,
    round(median_solar_penetration_pct, 1) AS median_solar_penetration_pct,
    round(avg_rrp_aud_mwh, 2) AS avg_rrp_aud_mwh,
    round(capture_price_aud_mwh, 2) AS capture_price_aud_mwh,
    round(pct_of_wind_energy, 1) AS pct_of_wind_energy,
    round(pct_intervals_negative, 1) AS pct_intervals_negative
FROM read_parquet($cannibalisation)
ORDER BY region, bucket
