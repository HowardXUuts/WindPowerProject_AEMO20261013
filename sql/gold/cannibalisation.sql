-- Gold: what wind earns as a function of how much wind is blowing.
--
-- The question behind every merchant wind investment and every PPA price:
-- does the asset earn less precisely when it generates most? Capture price
-- says that it does; this says how steeply, which is what a revenue model
-- needs.
--
-- Intervals are bucketed by instantaneous wind penetration — regional wind
-- output as a share of operational demand — and each bucket reports what the
-- region paid and what wind actually earned there. Bands extend past 100%
-- because South Australia routinely generates more wind than its own
-- operational demand and exports the surplus. A region with a flat curve
-- is one where more wind can be built; a region whose top bucket is negative
-- is one where the next farm is buying into a saturated market.
--
-- Two properties of the denominator are worth stating, because both move the
-- curve and neither is an error:
--
--   * `totaldemand` is operational demand, so it is net of rooftop solar.
--     Penetration here is therefore the share of the demand the market
--     actually dispatches for, which is the share that sets the price.
--   * The numerator is this project's registry wind, not every wind MW in the
--     region. Where the registry is incomplete, penetration is understated and
--     the curve shifts left — it never changes shape.
--
-- **The curve is not monotonic, and solar is why.** Measured on this data,
-- Victoria's lowest wind bucket is its *worst* priced one: below 10% wind
-- penetration the average price is negative and every interval is, because low
-- wind in the middle of the day means high solar — that bucket averages 43%
-- solar penetration against 6% in the top wind bucket. Read the wind buckets
-- without that column and the chart says something false. Read bucket 3
-- upward, where solar is comparable, and the wind effect is the one the
-- literature predicts: $52, $50, $33 as wind penetration climbs.
--
-- So `avg_solar_penetration_pct` is not decoration. It is the control variable
-- that makes the wind buckets comparable, and the honest version of this chart
-- shows both.
--
-- `pct_of_wind_energy` is the column that turns the curve into a risk
-- statement. A steep curve matters only to the extent output lands in the
-- cannibalised buckets, and that share is how much of the fleet's energy does.
--
-- Grain is (region, bucket) over all available history, not per day: a single
-- day holds too few intervals per bucket to average, and the curve is a
-- structural property of a region rather than a daily event.
--
--
-- Every float below is rounded. Gold is committed and rewritten on every run,
-- and DuckDB sums in parallel, so the same inputs can produce results differing
-- in the last bit — enough for a new Parquet blob to enter git history every
-- hour for no change in the data.
--
-- The precision is matched to each column's magnitude, because the drift is
-- *relative*: at 1e-14 of a term, an energy figure near 80,000 MWh moves by
-- about 1e-9, which is close enough to a six-decimal boundary to flip it — as
-- it did. Energy is rounded at three decimals, prices at four and percentages
-- at four, each leaving at least six orders of magnitude of margin, and all of
-- them finer than these figures can meaningfully be read to.
--
-- Parameters: $wind_glob, $price_glob, $regionsum_glob

WITH demand AS (
    -- Intervention runs publish a second row per interval; the pricing run
    -- (intervention = 0) is the one the market settles against, exactly as
    -- sql/silver/price.sql chooses it.
    SELECT
        -- settlementdate is interval-ENDING, as AEMO sends it. Shifted to a
        -- start here so every join in this project keys the same way; joining
        -- it unshifted silently pairs each demand figure with the interval
        -- before it.
        settlementdate - INTERVAL 5 MINUTE AS interval_start_utc,
        regionid AS region,
        totaldemand,
        -- AEMO's own availability forecast for semi-scheduled solar, which is
        -- the best available proxy for how much solar was pushing on price.
        -- It excludes rooftop PV, already netted out of totaldemand.
        ss_solar_uigf
    FROM read_parquet($regionsum_glob)
    WHERE totaldemand > 0
    QUALIFY row_number() OVER (
        PARTITION BY settlementdate, regionid
        ORDER BY intervention ASC, ingested_at DESC, source_file DESC
    ) = 1
),

region_interval AS (
    -- Farms collapse to one regional figure before the join, so an interval is
    -- never weighted by how many farms happened to be reporting.
    SELECT
        region,
        interval_start_utc,
        sum(mw) AS wind_mw
    FROM read_parquet($wind_glob)
    GROUP BY region, interval_start_utc
),

joined AS (
    SELECT
        w.region,
        w.wind_mw,
        p.rrp,
        p.trading_date,
        100.0 * w.wind_mw / d.totaldemand AS penetration_pct,
        100.0 * coalesce(d.ss_solar_uigf, 0) / d.totaldemand AS solar_penetration_pct
    FROM region_interval w
    JOIN demand d
        ON d.region = w.region
       AND d.interval_start_utc = w.interval_start_utc
    JOIN read_parquet($price_glob) p
        ON p.region = w.region
       AND p.interval_start_utc = w.interval_start_utc
),

bucketed AS (
    SELECT
        *,
        -- Fixed edges rather than quantiles, so a bucket means the same thing
        -- in every region and does not move as history accumulates.
        -- Bands run past 100% because South Australia does. Measured on this
        -- data, SA wind averages 128% of operational demand in its top band and
        -- 94.5% of its energy lands there — a band topping out at 45% would
        -- collapse the region's entire curve into one point.
        CASE
            WHEN penetration_pct < 10 THEN 1
            WHEN penetration_pct < 20 THEN 2
            WHEN penetration_pct < 30 THEN 3
            WHEN penetration_pct < 45 THEN 4
            WHEN penetration_pct < 60 THEN 5
            WHEN penetration_pct < 80 THEN 6
            WHEN penetration_pct < 100 THEN 7
            ELSE 8
        END AS bucket
    FROM joined
),

totals AS (
    SELECT region, sum(wind_mw) AS region_wind_mw
    FROM bucketed
    GROUP BY region
)

SELECT
    b.region,
    b.bucket,
    CASE b.bucket
        WHEN 1 THEN '<10%'
        WHEN 2 THEN '10-20%'
        WHEN 3 THEN '20-30%'
        WHEN 4 THEN '30-45%'
        WHEN 5 THEN '45-60%'
        WHEN 6 THEN '60-80%'
        WHEN 7 THEN '80-100%'
        ELSE '>100%'
    END AS bucket_label,
    count(*) AS intervals,
    -- Median, not mean. These are ratios whose denominator is operational
    -- demand, which collapses towards zero on a sunny low-load afternoon; a
    -- handful of such intervals drags a mean far above anything that actually
    -- happened. SA's top band reads 128% as a mean and the median says what a
    -- typical interval in it looked like.
    round(median(b.penetration_pct), 4) AS median_penetration_pct,
    round(median(b.solar_penetration_pct), 4) AS median_solar_penetration_pct,
    round(median(b.penetration_pct + b.solar_penetration_pct), 4) AS median_vre_penetration_pct,
    -- Each interval counted once: the price the region set in this bucket.
    round(avg(b.rrp), 4) AS avg_rrp_aud_mwh,
    -- Output-weighted: what wind was actually paid in this bucket.
    round(sum(b.wind_mw * b.rrp) / nullif(sum(b.wind_mw), 0), 4) AS capture_price_aud_mwh,
    round(sum(b.wind_mw) * 5.0 / 60.0, 3) AS wind_energy_mwh,
    round(100.0 * sum(b.wind_mw) / nullif(t.region_wind_mw, 0), 4) AS pct_of_wind_energy,
    round(100.0 * count(*) FILTER (WHERE b.rrp < 0) / count(*), 4) AS pct_intervals_negative,
    min(b.trading_date) AS first_date,
    max(b.trading_date) AS last_date
FROM bucketed b
JOIN totals t USING (region)
GROUP BY b.region, b.bucket, t.region_wind_mw
ORDER BY b.region, b.bucket
