-- Dashboard: negative-price run-length distribution by region.
-- Parameters: $negative_price_runs
SELECT
    region,
    bucket,
    bucket_label,
    runs,
    intervals,
    pct_of_runs,
    pct_of_negative_intervals,
    pct_of_negative_energy,
    median_run_minutes,
    longest_run_minutes,
    deepest_rrp,
    round(revenue_aud) AS revenue_aud,
    sum(intervals) OVER (PARTITION BY region) AS region_negative_intervals
FROM read_parquet($negative_price_runs)
ORDER BY region, bucket
