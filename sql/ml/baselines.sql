-- ML: the forecasts a model has to beat, on the same grid as the features.
--
-- PLAN 4.6 names four. Three are computable from the price history alone and
-- are built here; AEMO's own P5MIN forecast is the fourth and comes from
-- bronze, joined on issue time rather than valid time (see baselines.py).
--
-- A baseline is only honest if it is subject to the same rule as the model:
-- every column below reads data at or before t.
--
-- Parameters: $price_glob, $spike_threshold

WITH price AS (
    SELECT region, interval_start_utc, interval_start_nem, trading_date, rrp
    FROM read_parquet($price_glob)
),

-- Climatology: the historical spike rate for this interval-of-day. PLAN 4.6
-- calls it the bar any calibrated spike model must clear to be worth its
-- complexity — and it is computed over the whole span on purpose, as a
-- reference point rather than a fold-safe feature. Using it inside a fold
-- means recomputing it on that fold's training rows only.
climatology AS (
    SELECT
        region,
        CAST(extract('hour' FROM interval_start_nem) * 12
             + extract('minute' FROM interval_start_nem) / 5 AS INTEGER) AS interval_of_day,
        avg(CASE WHEN rrp > $spike_threshold THEN 1.0 ELSE 0.0 END) AS clim_spike_rate,
        avg(rrp) AS clim_mean_rrp
    FROM price
    GROUP BY 1, 2
)

SELECT
    p.region,
    p.interval_start_utc,
    p.trading_date,

    -- 1. Persistence: tomorrow looks like now. Cheap, and over 60 minutes
    --    surprisingly hard to beat.
    p.rrp AS persist_mean_60,
    p.rrp AS persist_mean_120,
    CASE WHEN p.rrp > $spike_threshold THEN 1.0 ELSE 0.0 END AS persist_spike_60,
    CASE WHEN p.rrp > $spike_threshold THEN 1.0 ELSE 0.0 END AS persist_spike_120,

    -- 2. Same time yesterday: the daily shape persistence cannot see. The
    --    window is the same clock window 24 h earlier, so it is known at t.
    y.y_mean_60 AS yest_mean_60,
    y.y_mean_120 AS yest_mean_120,
    y.y_spike_60 AS yest_spike_60,
    y.y_spike_120 AS yest_spike_120,

    -- 3. Climatology, for the spike models.
    c.clim_spike_rate,
    c.clim_mean_rrp
FROM price p
LEFT JOIN climatology c
       ON c.region = p.region
      AND c.interval_of_day = CAST(extract('hour' FROM p.interval_start_nem) * 12
                                   + extract('minute' FROM p.interval_start_nem) / 5 AS INTEGER)
-- Yesterday's realised window, which is a fact by the time t arrives.
LEFT JOIN (
    SELECT
        region,
        interval_start_utc,
        avg(rrp) OVER w60 AS y_mean_60,
        avg(rrp) OVER w120 AS y_mean_120,
        max(CASE WHEN rrp > $spike_threshold THEN 1.0 ELSE 0.0 END) OVER w60 AS y_spike_60,
        max(CASE WHEN rrp > $spike_threshold THEN 1.0 ELSE 0.0 END) OVER w120 AS y_spike_120
    FROM price
    WINDOW
        w60 AS (PARTITION BY region ORDER BY interval_start_utc
                ROWS BETWEEN 1 FOLLOWING AND 12 FOLLOWING),
        w120 AS (PARTITION BY region ORDER BY interval_start_utc
                 ROWS BETWEEN 1 FOLLOWING AND 24 FOLLOWING)
) y ON y.region = p.region
   AND y.interval_start_utc = p.interval_start_utc - INTERVAL 24 HOUR
ORDER BY p.region, p.interval_start_utc
