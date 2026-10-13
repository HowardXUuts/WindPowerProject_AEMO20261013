-- Gold: wind capture price against the time-weighted average, per region per day.
--
-- Capture price is what wind actually earned per MWh: output-weighted price.
-- The time-weighted average is what a generator earning the same amount every
-- interval would have earned. The ratio between them is the single number that
-- says whether a region's wind is worth more or less than the average megawatt
-- hour, and for a mature wind region it is reliably below 1 — wind generates
-- when other wind generates, and the price falls when it does.
--
-- The two averages are computed over different populations on purpose: capture
-- is weighted by wind output, so it is taken from the joined rows, while the
-- time-weighted average must count each interval once regardless of how many
-- farms were generating, so it comes from the price table alone.
--
-- Parameters: $wind_glob, $price_glob

WITH region_interval AS (
    -- Collapse farms to a single regional wind figure per interval first, so
    -- the join below cannot weight an interval by farm count.
    SELECT
        trading_date,
        region,
        interval_start_utc,
        sum(mw) AS wind_mw
    FROM read_parquet($wind_glob)
    GROUP BY trading_date, region, interval_start_utc
),

settled AS (
    SELECT r.trading_date, r.region, r.wind_mw, p.rrp
    FROM region_interval r
    JOIN read_parquet($price_glob) p
        ON p.interval_start_utc = r.interval_start_utc
       AND p.region = r.region
),

captured AS (
    SELECT
        trading_date,
        region,
        sum(wind_mw * rrp) / nullif(sum(wind_mw), 0) AS capture_price_aud_mwh,
        sum(wind_mw) * 5.0 / 60.0 AS wind_energy_mwh,
        count(*) AS intervals
    FROM settled
    GROUP BY trading_date, region
),

time_weighted AS (
    SELECT
        trading_date,
        region,
        avg(rrp) AS twap_aud_mwh,
        count(*) AS price_intervals
    FROM read_parquet($price_glob)
    GROUP BY trading_date, region
)

SELECT
    c.trading_date,
    c.region,
    c.capture_price_aud_mwh,
    t.twap_aud_mwh,
    -- Below 1 means wind earns less than the average megawatt hour in that
    -- region; the cannibalisation effect, measured.
    CASE
        WHEN t.twap_aud_mwh <> 0
        THEN c.capture_price_aud_mwh / t.twap_aud_mwh
    END AS capture_ratio,
    c.wind_energy_mwh,
    c.intervals,
    t.price_intervals
FROM captured c
JOIN time_weighted t USING (trading_date, region)
ORDER BY c.trading_date, c.region
