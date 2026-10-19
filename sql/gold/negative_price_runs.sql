-- Gold: how long negative prices last, not just how often they happen.
--
-- The dashboard already reports the share of output sold below zero, and that
-- share is the wrong unit for a decision. Eight per cent spread thinly across a
-- month is a rounding error on a revenue model; eight per cent arriving as
-- four-hour blocks is a curtailment policy, a battery charging window, and a
-- different PPA structure. Share cannot tell those apart. Duration can.
--
-- Measured on this data the distinction is not subtle. Victoria's *median*
-- negative run is two intervals — ten minutes, trivial — yet 85.7% of its
-- negative intervals arrive in runs longer than four hours, and those runs are
-- 18.8% of the count. South Australia's longest single run is 134 intervals,
-- eleven hours and ten minutes. Reading the median alone, or the share alone,
-- would miss the only part that matters operationally.
--
-- Runs are built with gaps-and-islands over a five-minute sequence number
-- rather than over row order, so a missing interval breaks a run instead of
-- silently welding together the two either side of it. A run genuinely broken
-- by absent data is reported as two, which understates duration; inventing
-- continuity across a gap would overstate it, and of the two the honest
-- direction is the conservative one.
--
-- A run still in progress at the end of the data is truncated and therefore
-- understated. The effect is bounded at one run per region, it always shortens
-- rather than lengthens, and `negative_price_worst` flags the affected run.
--
-- Float aggregates are rounded, and build_gold runs single-threaded, for the
-- byte-stability reason set out in capture_price.sql.
--
-- Parameters: $wind_glob, $price_glob

WITH marked AS (
    SELECT
        region,
        trading_date,
        interval_start_utc,
        rrp,
        -- An explicit interval index, so "contiguous" means contiguous in time
        -- and not merely adjacent in the result set.
        CAST(epoch(interval_start_utc) / 300 AS BIGINT) AS seq
    FROM read_parquet($price_glob)
),

islanded AS (
    SELECT
        *,
        seq - row_number() OVER (PARTITION BY region, rrp < 0 ORDER BY seq) AS island
    FROM marked
),

-- Wind output in the same interval, so a run can be priced as well as timed.
-- Left-joined: a negative run is a fact about the region's price whether or not
-- any wind farm was generating into it.
priced AS (
    SELECT
        i.region,
        i.island,
        i.trading_date,
        i.rrp,
        coalesce(w.wind_mw, 0) AS wind_mw
    FROM islanded i
    LEFT JOIN (
        SELECT region, interval_start_utc, sum(mw) AS wind_mw
        FROM read_parquet($wind_glob)
        GROUP BY region, interval_start_utc
    ) w
        ON w.region = i.region
       AND w.interval_start_utc = i.interval_start_utc
    WHERE i.rrp < 0
),

runs AS (
    SELECT
        region,
        island,
        count(*) AS intervals,
        min(rrp) AS deepest_rrp,
        sum(wind_mw) * 5.0 / 60.0 AS wind_energy_mwh,
        sum(wind_mw * rrp) * 5.0 / 60.0 AS revenue_aud,
        min(trading_date) AS first_date,
        max(trading_date) AS last_date
    FROM priced
    GROUP BY region, island
),

bucketed AS (
    SELECT
        *,
        -- Edges chosen against the observed distribution: it is bimodal, with a
        -- mass of single intervals and a mass beyond four hours and almost
        -- nothing between, so the buckets have to resolve both ends.
        CASE
            WHEN intervals = 1 THEN 1
            WHEN intervals <= 3 THEN 2
            WHEN intervals <= 6 THEN 3
            WHEN intervals <= 12 THEN 4
            WHEN intervals <= 24 THEN 5
            WHEN intervals <= 48 THEN 6
            WHEN intervals <= 96 THEN 7
            ELSE 8
        END AS bucket
    FROM runs
),

totals AS (
    SELECT
        region,
        sum(intervals) AS region_intervals,
        sum(wind_energy_mwh) AS region_energy_mwh
    FROM bucketed
    GROUP BY region
)

SELECT
    b.region,
    b.bucket,
    CASE b.bucket
        WHEN 1 THEN '5 min'
        WHEN 2 THEN '10-15 min'
        WHEN 3 THEN '20-30 min'
        WHEN 4 THEN '35-60 min'
        WHEN 5 THEN '1-2 h'
        WHEN 6 THEN '2-4 h'
        WHEN 7 THEN '4-8 h'
        ELSE '>8 h'
    END AS bucket_label,
    count(*) AS runs,
    sum(b.intervals) AS intervals,
    -- The column the metric exists for: what share of the region's time below
    -- zero was spent inside runs of this length.
    round(100.0 * sum(b.intervals) / nullif(t.region_intervals, 0), 4)
        AS pct_of_negative_intervals,
    round(100.0 * count(*) / nullif(
        (SELECT count(*) FROM bucketed WHERE region = b.region), 0), 4) AS pct_of_runs,
    round(median(b.intervals) * 5.0, 1) AS median_run_minutes,
    max(b.intervals) * 5 AS longest_run_minutes,
    round(min(b.deepest_rrp), 4) AS deepest_rrp,
    round(sum(b.wind_energy_mwh), 3) AS wind_energy_mwh,
    round(100.0 * sum(b.wind_energy_mwh) / nullif(t.region_energy_mwh, 0), 4)
        AS pct_of_negative_energy,
    round(sum(b.revenue_aud), 2) AS revenue_aud,
    min(b.first_date) AS first_date,
    max(b.last_date) AS last_date
FROM bucketed b
JOIN totals t USING (region)
GROUP BY b.region, b.bucket, t.region_intervals, t.region_energy_mwh
ORDER BY b.region, b.bucket
