-- SOFT: regional price outside the band normal trading stays within.
--
-- The NEM's administered floor is -$1,000/MWh and the cap is far above
-- $5,000, so neither bound here is impossible: both are reached during real
-- scarcity and real oversupply. They warn because a genuine extreme and a
-- parsing fault look identical in a single number, and the difference matters
-- enormously to revenue figures.
--
-- Parameters: $price_glob, $price_max, $price_min

SELECT
    trading_date,
    region AS subject,
    CASE WHEN max(rrp) > $price_max THEN max(rrp) ELSE min(rrp) END AS observed,
    region || ': ' || count(*) || ' intervals outside the band, range '
        || round(min(rrp), 2) || ' to ' || round(max(rrp), 2) || ' $/MWh' AS message
FROM read_parquet($price_glob)
WHERE rrp > $price_max OR rrp < $price_min
GROUP BY trading_date, region
ORDER BY trading_date, region
