-- Gold: when in the day does wind arrive, relative to the demand it is sold into?
--
-- The question a shape chart of megawatts cannot answer on its own. A region
-- can generate most of its wind energy at 3am and look healthy on a capacity
-- factor, and that output is sold into the cheapest hours of the day. What a
-- developer wants is the ratio: wind output as a share of the operational
-- demand it arrives alongside, hour by hour.
--
-- Wind MW and demand MW share a unit, so plotting both would need no second
-- axis — but five regions times two series is ten lines on one chart. The
-- ratio carries the comparison in one series per region and answers the
-- question more directly than two shapes a reader has to difference by eye.
-- The two means are kept beside it so the ratio can be taken apart.
--
-- Averaged over every day held rather than the latest one. A single day's
-- diurnal shape is weather; a mean over the record is the beginning of a
-- pattern. `days` rides on every row because at eight days it is the
-- beginning and not much more, and a reader should see that.
--
-- Hour of the NEM day, not UTC: the question is about the local daily cycle,
-- and NEM time is AEST year round with no daylight saving to straddle.
--
-- Float aggregates are rounded, and build_gold runs single-threaded, for the
-- byte-stability reason set out in capture_price.sql.
--
-- Parameters: $wind_glob, $regionsum_glob

WITH wind AS (
    SELECT
        region,
        interval_start_utc,
        trading_date,
        hour(interval_start_nem) AS hour_nem,
        sum(mw) AS wind_mw
    FROM read_parquet($wind_glob)
    GROUP BY region, interval_start_utc, interval_start_nem, trading_date
),

demand AS (
    -- settlementdate is interval-ENDING, shifted to a start so it keys like
    -- every other join in this project. intervention = 0 is the pricing run.
    SELECT
        settlementdate - INTERVAL 5 MINUTE AS interval_start_utc,
        regionid AS region,
        totaldemand
    FROM read_parquet($regionsum_glob)
    WHERE totaldemand > 0
    QUALIFY row_number() OVER (
        PARTITION BY settlementdate, regionid
        ORDER BY intervention ASC, ingested_at DESC, source_file DESC
    ) = 1
),

joined AS (
    SELECT
        w.region,
        w.hour_nem,
        w.trading_date,
        w.wind_mw,
        d.totaldemand
    FROM wind w
    JOIN demand d
        ON d.region = w.region
       AND d.interval_start_utc = w.interval_start_utc
)

SELECT
    region,
    hour_nem,
    round(avg(wind_mw), 3) AS mean_wind_mw,
    round(avg(totaldemand), 3) AS mean_demand_mw,
    -- The ratio of the means, not the mean of the ratios: this is the share of
    -- the hour's energy that wind supplied, which is what a reader takes it
    -- for. A mean of per-interval ratios would let a single low-demand
    -- interval dominate the hour, the same trap the cannibalisation model
    -- avoids with a median.
    round(100.0 * avg(wind_mw) / nullif(avg(totaldemand), 0), 4) AS penetration_pct,
    count(DISTINCT trading_date) AS days,
    count(*) AS intervals
FROM joined
GROUP BY region, hour_nem
ORDER BY region, hour_nem
