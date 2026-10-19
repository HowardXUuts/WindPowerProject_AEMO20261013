-- Gold: the longest negative-price events, named and dated.
--
-- The distribution in negative_price_runs.sql says the mass sits in long runs.
-- This says which runs, so the claim can be checked against a date rather than
-- taken on trust, and so a reader can go and look at what the system was doing
-- that afternoon. Ten per region: enough to see the pattern, bounded so gold
-- stays small as history accumulates.
--
-- Times are NEM local, because "it started at 10:25 and ran until 21:35" is the
-- form in which this is useful. The UTC start is kept so the event can be
-- joined back to silver without a timezone conversion in the consumer.
--
-- `is_open` marks a run still in progress at the end of the data. Such a run is
-- truncated, so its duration is a floor rather than a measurement, and ranking
-- it against completed runs without saying so would be wrong.
--
-- Float aggregates are rounded, and build_gold runs single-threaded, for the
-- byte-stability reason set out in capture_price.sql.
--
-- Parameters: $wind_glob, $price_glob

WITH marked AS (
    SELECT
        region,
        interval_start_utc,
        interval_start_nem,
        rrp,
        CAST(epoch(interval_start_utc) / 300 AS BIGINT) AS seq
    FROM read_parquet($price_glob)
),

islanded AS (
    SELECT
        *,
        seq - row_number() OVER (PARTITION BY region, rrp < 0 ORDER BY seq) AS island
    FROM marked
),

last_interval AS (
    SELECT region, max(seq) AS final_seq FROM marked GROUP BY region
),

priced AS (
    SELECT
        i.region,
        i.island,
        i.interval_start_utc,
        i.interval_start_nem,
        i.seq,
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
        p.region,
        p.island,
        count(*) AS intervals,
        min(p.interval_start_utc) AS started_utc,
        min(p.interval_start_nem) AS started_nem,
        -- The run ends when its last interval ends, five minutes after that
        -- interval starts.
        max(p.interval_start_nem) + INTERVAL 5 MINUTE AS ended_nem,
        avg(p.rrp) AS mean_rrp,
        min(p.rrp) AS deepest_rrp,
        sum(p.wind_mw) * 5.0 / 60.0 AS wind_energy_mwh,
        sum(p.wind_mw * p.rrp) * 5.0 / 60.0 AS revenue_aud,
        max(p.seq) = max(l.final_seq) AS is_open
    FROM priced p
    JOIN last_interval l USING (region)
    GROUP BY p.region, p.island
)

SELECT
    region,
    row_number() OVER (PARTITION BY region ORDER BY intervals DESC, started_utc) AS rank,
    started_utc,
    started_nem,
    ended_nem,
    intervals,
    round(intervals * 5.0 / 60.0, 3) AS duration_hours,
    round(mean_rrp, 4) AS mean_rrp,
    round(deepest_rrp, 4) AS deepest_rrp,
    round(wind_energy_mwh, 3) AS wind_energy_mwh,
    round(revenue_aud, 2) AS revenue_aud,
    is_open
FROM runs
QUALIFY rank <= 10
ORDER BY region, rank
