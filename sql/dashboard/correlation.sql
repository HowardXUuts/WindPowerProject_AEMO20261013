-- Dashboard: the regional wind-output correlation matrix.
-- Parameters: $output_correlation
SELECT
    region_a,
    region_b,
    is_same_region,
    farm_pairs,
    mean_r,
    median_r,
    min_r,
    max_r,
    portfolio_r,
    min_intervals,
    farms_excluded,
    CAST(first_date AS VARCHAR) AS first_date,
    CAST(last_date AS VARCHAR) AS last_date
FROM read_parquet($output_correlation)
ORDER BY region_a, region_b
