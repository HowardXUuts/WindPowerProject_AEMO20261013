-- Gold: how much of wind revenue arrives in how few intervals.
--
-- Two regions can show the same capture price and carry completely different
-- risk. If a third of the year's revenue arrives in 1% of intervals, the
-- business case rests on scarcity events: a quiet summer, a new interconnector
-- or a battery fleet clipping those peaks takes the revenue with it. If
-- revenue is spread evenly, the asset is a volume business and behaves like
-- one. Capture price cannot tell these apart, and this is the column a
-- developer stress-tests.
--
-- Intervals are ranked by the revenue wind earned in them, descending, and the
-- top slices are reported as a share of the regional total. Ranking by revenue
-- rather than by price is deliberate: a $15,000/MWh interval in which the
-- fleet generated almost nothing is not where the money came from.
--
-- **A share above 100% is a real result, not an error.** Net revenue is the
-- denominator, so where a region gives back a material amount at negative
-- prices the best intervals can account for more than all of it — the rest was
-- earned and then paid back. Reported as it falls out, because a figure capped
-- at 100% would hide exactly the regions worth worrying about.
--
-- Shares are cumulative and nested: the top 5% contains the top 1%. The
-- interval counts are published alongside so a reader can see how few
-- intervals a share rests on — at eight days of history the top 1% is about
-- twenty intervals, and a share computed from twenty intervals deserves to be
-- read with that in front of it.
--
--
-- Every float below is rounded. Gold is committed and rewritten on every run,
-- and DuckDB sums in parallel, so the same inputs can produce results differing
-- in the last bit — enough for a new Parquet blob to enter git history every
-- hour for no change in the data.
--
-- The precision is matched to each column's magnitude, because the drift is
-- *relative*: revenue runs to tens of millions, so it is rounded to whole
-- dollars rather than to a fraction of a cent that a parallel sum cannot
-- reproduce. Percentages keep four decimals.
--
-- Parameters: $wind_glob, $price_glob

WITH region_interval AS (
    SELECT
        w.region,
        w.trading_date,
        w.interval_start_utc,
        sum(w.mw) * 5.0 / 60.0 AS energy_mwh
    FROM read_parquet($wind_glob) w
    GROUP BY w.region, w.trading_date, w.interval_start_utc
),

settled AS (
    SELECT
        r.region,
        r.trading_date,
        r.energy_mwh,
        p.rrp,
        r.energy_mwh * p.rrp AS revenue_aud
    FROM region_interval r
    JOIN read_parquet($price_glob) p
        ON p.region = r.region
       AND p.interval_start_utc = r.interval_start_utc
),

ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY region ORDER BY revenue_aud DESC) AS rank,
        count(*) OVER (PARTITION BY region) AS n
    FROM settled
)

SELECT
    region,
    max(n) AS intervals,
    round(sum(revenue_aud), 2) AS revenue_aud,
    round(sum(energy_mwh), 3) AS wind_energy_mwh,
    -- ceil, so a slice is never empty: 1% of 2,000 intervals is 20, and 1% of
    -- 50 is one interval rather than none.
    count(*) FILTER (WHERE rank <= ceil(0.01 * n)) AS top_1pct_intervals,
    round(100.0 * sum(revenue_aud) FILTER (WHERE rank <= ceil(0.01 * n))
              / nullif(sum(revenue_aud), 0), 4) AS pct_from_top_1pct,
    round(100.0 * sum(revenue_aud) FILTER (WHERE rank <= ceil(0.05 * n))
              / nullif(sum(revenue_aud), 0), 4) AS pct_from_top_5pct,
    round(100.0 * sum(revenue_aud) FILTER (WHERE rank <= ceil(0.10 * n))
              / nullif(sum(revenue_aud), 0), 4) AS pct_from_top_10pct,
    -- The other tail, and the reason a share above 100% happens.
    round(sum(revenue_aud) FILTER (WHERE rrp < 0), 2) AS negative_revenue_aud,
    round(100.0 * count(*) FILTER (WHERE rrp < 0) / count(*), 4) AS pct_intervals_negative,
    min(trading_date) AS first_date,
    max(trading_date) AS last_date
FROM ranked
GROUP BY region
ORDER BY region
