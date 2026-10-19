-- Dashboard: share of wind revenue earned in the best intervals, by region.
-- Parameters: $revenue_concentration
SELECT
    region,
    intervals,
    top_1pct_intervals,
    round(revenue_aud) AS revenue_aud,
    round(pct_from_top_1pct, 1) AS pct_from_top_1pct,
    round(pct_from_top_5pct, 1) AS pct_from_top_5pct,
    round(pct_from_top_10pct, 1) AS pct_from_top_10pct,
    round(negative_revenue_aud) AS negative_revenue_aud,
    round(pct_intervals_negative, 1) AS pct_intervals_negative,
    CAST(first_date AS VARCHAR) AS first_date,
    CAST(last_date AS VARCHAR) AS last_date
FROM read_parquet($revenue_concentration)
ORDER BY region
