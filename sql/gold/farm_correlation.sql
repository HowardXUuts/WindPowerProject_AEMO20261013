-- Gold: each farm's closest and most independent counterparts.
--
-- The per-asset form of output_correlation.sql, and the one a developer uses
-- directly. For a farm being bought, built or offtaken, the two useful lists
-- are: which existing assets it duplicates (concentration — its output arrives
-- when theirs does, into the price they are already depressing), and which it
-- offsets (diversification — the pairing that makes a firmed product cheaper).
--
-- Five rows each way per farm rather than the whole matrix. The full 96 farms
-- give 4,560 unordered pairs, which is both more than any page displays and a
-- Parquet blob entering git history every time silver gains a day. The tails
-- are what is read; the middle of a correlation ranking is not.
--
-- Correlation is symmetric, so rows are emitted in both directions: a page
-- showing one farm should not have to know which way the pair was stored.
--
-- Definitions and exclusions follow output_correlation.sql exactly — capacity
-- factor at five-minute grain, farms needing a day of intervals and non-zero
-- variation — so the two tables can be read against each other.
--
-- Float aggregates are rounded, and build_gold runs single-threaded, for the
-- byte-stability reason set out in capture_price.sql.
--
-- Parameters: $wind_glob

WITH cf AS (
    SELECT
        duid,
        station_name,
        region,
        interval_start_utc,
        mw / capacity_mw AS cf
    FROM read_parquet($wind_glob)
    WHERE capacity_mw > 0
),

usable AS (
    SELECT duid
    FROM cf
    GROUP BY duid
    HAVING count(*) >= 288
       AND stddev_pop(cf) > 1e-9
),

eligible AS (
    SELECT cf.* FROM cf JOIN usable USING (duid)
),

pairs AS (
    SELECT
        a.duid AS duid_a,
        any_value(a.station_name) AS name_a,
        any_value(a.region) AS region_a,
        b.duid AS duid_b,
        any_value(b.station_name) AS name_b,
        any_value(b.region) AS region_b,
        corr(a.cf, b.cf) AS r,
        count(*) AS intervals
    FROM eligible a
    JOIN eligible b
        ON b.interval_start_utc = a.interval_start_utc
       AND b.duid > a.duid
    GROUP BY a.duid, b.duid
    HAVING count(*) >= 288
),

both_ways AS (
    SELECT duid_a AS duid, name_a AS station_name, region_a AS region,
           duid_b AS other_duid, name_b AS other_name, region_b AS other_region,
           r, intervals
    FROM pairs
    UNION ALL
    SELECT duid_b, name_b, region_b, duid_a, name_a, region_a, r, intervals
    FROM pairs
),

ranked AS (
    SELECT
        *,
        -- duid breaks ties, so the chosen five do not depend on scan order.
        row_number() OVER (PARTITION BY duid ORDER BY r DESC, other_duid) AS most_rank,
        row_number() OVER (PARTITION BY duid ORDER BY r ASC, other_duid) AS least_rank
    FROM both_ways
)

SELECT
    duid,
    station_name,
    region,
    'most' AS relation,
    most_rank AS rank,
    other_duid,
    other_name,
    other_region,
    other_region = region AS same_region,
    round(r, 4) AS r,
    intervals
FROM ranked
WHERE most_rank <= 5
UNION ALL
SELECT
    duid, station_name, region, 'least', least_rank,
    other_duid, other_name, other_region, other_region = region,
    round(r, 4), intervals
FROM ranked
WHERE least_rank <= 5
ORDER BY duid, relation, rank
