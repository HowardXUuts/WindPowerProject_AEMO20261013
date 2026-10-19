-- Gold: do two wind farms generate at the same time?
--
-- The portfolio question, and the one a capture price cannot answer. Two farms
-- with identical capacity factors are worth very different amounts depending on
-- whether they produce together: output that arrives at the same moment as
-- everyone else's is output sold into the price its own arrival depressed. The
-- cannibalisation curve says wind earns less when wind blows; this says *which*
-- wind blows together, which is the half a developer can act on — by siting
-- elsewhere, by pairing an offtake, or by firming.
--
-- Measured on this data the answer is blunt: the mean correlation between two
-- farms in the same region is 0.342, and between two farms in different regions
-- 0.114. Crossing a regional boundary removes about two thirds of the
-- co-movement, which is the quantitative case for a geographically spread
-- portfolio rather than a cheaper second farm next door.
--
-- **Capacity factor, not MW.** A 500 MW farm and a 50 MW farm correlate on
-- shape, not on scale, and normalising by registered capacity is what makes the
-- figure comparable across the fleet.
--
-- **Five-minute grain, and the choice does not drive the result.** This is the
-- interval output is settled in, so it needs no further defence, but the
-- obvious objection — that five-minute data is turbulence rather than weather —
-- was tested rather than argued: correlating hourly means instead shifts the
-- mean by 0.008 across regions and 0.020 within one. The co-movement is
-- genuinely in the weather, not in the sampling.
--
-- Every cell of this table is the same statistic — the mean correlation of a
-- farm pair — so the diagonal reads as "two farms inside this region" and the
-- off-diagonal as "one farm in each". `portfolio_r` is the different and also
-- useful figure of the two regions' aggregate output correlating; it is
-- trivially 1 on the diagonal and is reported beside, never mixed in.
--
-- Float aggregates are rounded, and build_gold runs single-threaded, for the
-- byte-stability reason set out in capture_price.sql.
--
-- Parameters: $wind_glob

WITH cf AS (
    SELECT
        duid,
        region,
        interval_start_utc,
        trading_date,
        mw / capacity_mw AS cf,
        mw,
        capacity_mw
    FROM read_parquet($wind_glob)
    WHERE capacity_mw > 0
),

-- A farm that never moved has no correlation with anything, and a farm seen for
-- an afternoon has one that means nothing. Both are excluded rather than
-- reported as null, and the count of exclusions rides on every row so a reader
-- can see the fleet was filtered.
usable AS (
    SELECT duid
    FROM cf
    GROUP BY duid
    HAVING count(*) >= 288          -- at least one full day of intervals
       AND stddev_pop(cf) > 1e-9    -- and it actually varied
),

eligible AS (
    SELECT cf.* FROM cf JOIN usable USING (duid)
),

pairs AS (
    SELECT
        a.region AS region_a,
        b.region AS region_b,
        corr(a.cf, b.cf) AS r,
        count(*) AS intervals,
        min(a.trading_date) AS first_date,
        max(a.trading_date) AS last_date
    FROM eligible a
    JOIN eligible b
        ON b.interval_start_utc = a.interval_start_utc
       AND b.duid > a.duid          -- each unordered pair once
    GROUP BY a.duid, b.duid, a.region, b.region
    HAVING count(*) >= 288
),

-- Order the two regions so a pair appears under one key, then mirror it below.
keyed AS (
    SELECT
        least(region_a, region_b) AS region_low,
        greatest(region_a, region_b) AS region_high,
        r, intervals, first_date, last_date
    FROM pairs
),

-- Regional aggregate output, for the portfolio-level figure.
regional AS (
    SELECT
        region,
        interval_start_utc,
        sum(mw) / nullif(sum(capacity_mw), 0) AS cf
    FROM eligible
    GROUP BY region, interval_start_utc
),

portfolio AS (
    -- One orientation per region pair, not both. Joining every pair in both
    -- directions and folding them together with least/greatest looks harmless
    -- because correlation is symmetric, but it is not: the group then holds
    -- (x, y) and (y, x) interleaved, so the statistic becomes the correlation
    -- of [x;y] against [y;x], which is cov(x,y) over the *mean* of the two
    -- variances rather than the geometric mean. On the test fixture that
    -- returned -2/3 where the real figure is -1/sqrt(2); the two agree only
    -- when both series happen to have the same variance, which is why the
    -- mistake survives casual inspection.
    SELECT
        a.region AS region_low,
        b.region AS region_high,
        corr(a.cf, b.cf) AS portfolio_r
    FROM regional a
    JOIN regional b
        ON b.interval_start_utc = a.interval_start_utc
       AND b.region >= a.region
    GROUP BY a.region, b.region
),

summarised AS (
    SELECT
        k.region_low,
        k.region_high,
        k.region_low = k.region_high AS is_same_region,
        count(*) AS farm_pairs,
        round(avg(k.r), 4) AS mean_r,
        round(median(k.r), 4) AS median_r,
        round(min(k.r), 4) AS min_r,
        round(max(k.r), 4) AS max_r,
        min(k.intervals) AS min_intervals,
        min(k.first_date) AS first_date,
        max(k.last_date) AS last_date
    FROM keyed k
    GROUP BY k.region_low, k.region_high
)

-- Emitted in both directions so a consumer can index the matrix without
-- knowing which way the key was sorted. The diagonal appears once.
SELECT
    s.region_low AS region_a,
    s.region_high AS region_b,
    s.is_same_region,
    s.farm_pairs,
    s.mean_r,
    s.median_r,
    s.min_r,
    s.max_r,
    round(p.portfolio_r, 4) AS portfolio_r,
    s.min_intervals,
    (SELECT count(DISTINCT duid) FROM cf) - (SELECT count(*) FROM usable) AS farms_excluded,
    s.first_date,
    s.last_date
FROM summarised s
LEFT JOIN portfolio p USING (region_low, region_high)
UNION ALL
SELECT
    s.region_high, s.region_low, s.is_same_region, s.farm_pairs,
    s.mean_r, s.median_r, s.min_r, s.max_r, round(p.portfolio_r, 4),
    s.min_intervals,
    (SELECT count(DISTINCT duid) FROM cf) - (SELECT count(*) FROM usable),
    s.first_date, s.last_date
FROM summarised s
LEFT JOIN portfolio p USING (region_low, region_high)
WHERE NOT s.is_same_region
ORDER BY region_a, region_b
