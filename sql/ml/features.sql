-- ML: one row per (region, five-minute interval), with targets and features.
--
-- The grid is silver price: price is regional and is what we forecast, so an
-- interval exists here if and only if a price was published for it.
--
-- **Every feature is a function of data at or before t; every target is after
-- it.** That split is the whole file. A window that reads one row too far
-- forward produces a model that looks superb and earns nothing, so each window
-- below states its bound explicitly rather than relying on a default.
--
-- DuckDB window frames are inclusive of the current row. `ROWS BETWEEN 12
-- PRECEDING AND CURRENT ROW` therefore spans t-60min .. t inclusive, which is
-- what a feature may see. Targets use `1 FOLLOWING`, never `CURRENT ROW`.
--
-- Parameters: $price_glob, $wind_glob, $registry, $spike_threshold

WITH price AS (
    -- The pricing run only: an intervention run is AEMO re-solving the market,
    -- and silver has already chosen between them.
    SELECT region, interval_start_utc, interval_start_nem, trading_date, rrp
    FROM read_parquet($price_glob)
),

wind AS (
    -- Farms collapsed to a regional total first. Joining farm rows to price
    -- would weight an interval by how many farms happened to report.
    SELECT
        region,
        interval_start_utc,
        sum(mw) AS wind_mw,
        sum(capacity_mw) AS wind_capacity_mw
    FROM read_parquet($wind_glob)
    GROUP BY region, interval_start_utc
),

grid AS (
    SELECT
        p.region,
        p.interval_start_utc,
        p.interval_start_nem,
        p.trading_date,
        p.rrp,
        w.wind_mw,
        w.wind_capacity_mw,
        CASE WHEN w.wind_capacity_mw > 0 THEN w.wind_mw / w.wind_capacity_mw END AS wind_share
    FROM price p
    LEFT JOIN wind w USING (region, interval_start_utc)
),

-- ---------------------------------------------------------------------------
-- Targets. The only place in this file that looks forward.
--
-- A window of h minutes from t covers the intervals starting t+5 .. t+h, which
-- is h/5 rows beginning at 1 FOLLOWING. The interval starting at t is excluded:
-- its price is already known at t and predicting it is not forecasting.
-- ---------------------------------------------------------------------------
targeted AS (
    SELECT
        *,
        avg(rrp) OVER w60 AS y_mean_60,
        avg(rrp) OVER w120 AS y_mean_120,
        max(CASE WHEN rrp > $spike_threshold THEN 1 ELSE 0 END) OVER w60 AS y_spike_60,
        max(CASE WHEN rrp > $spike_threshold THEN 1 ELSE 0 END) OVER w120 AS y_spike_120,
        count(rrp) OVER w60 AS y_intervals_60,
        count(rrp) OVER w120 AS y_intervals_120
    FROM grid
    WINDOW
        w60 AS (PARTITION BY region ORDER BY interval_start_utc
                ROWS BETWEEN 1 FOLLOWING AND 12 FOLLOWING),
        w120 AS (PARTITION BY region ORDER BY interval_start_utc
                 ROWS BETWEEN 1 FOLLOWING AND 24 FOLLOWING)
),

-- ---------------------------------------------------------------------------
-- Features. Everything below reads backwards only.
-- ---------------------------------------------------------------------------
featured AS (
    SELECT
        *,
        -- Price history. The lag at 0 is the current interval's price, which
        -- is known at t: dispatch publishes a price at the start of the
        -- interval it governs, measured at 09:45:09 for the 09:50 stamp.
        lag(rrp, 1) OVER o AS rrp_lag_5,
        lag(rrp, 2) OVER o AS rrp_lag_10,
        lag(rrp, 3) OVER o AS rrp_lag_15,
        lag(rrp, 6) OVER o AS rrp_lag_30,
        lag(rrp, 12) OVER o AS rrp_lag_60,
        lag(rrp, 24) OVER o AS rrp_lag_120,

        avg(rrp) OVER back6 AS rrp_mean_30,
        avg(rrp) OVER back12 AS rrp_mean_60,
        avg(rrp) OVER back48 AS rrp_mean_240,
        stddev_samp(rrp) OVER back12 AS rrp_std_60,
        stddev_samp(rrp) OVER back48 AS rrp_std_240,
        min(rrp) OVER back12 AS rrp_min_60,
        max(rrp) OVER back12 AS rrp_max_60,
        min(rrp) OVER back48 AS rrp_min_240,
        max(rrp) OVER back48 AS rrp_max_240,

        -- How often the price has already spiked recently. A spike count is a
        -- better regime signal than the last price alone.
        sum(CASE WHEN rrp > $spike_threshold THEN 1 ELSE 0 END) OVER back12 AS spikes_60,
        sum(CASE WHEN rrp > $spike_threshold THEN 1 ELSE 0 END) OVER back48 AS spikes_240,
        sum(CASE WHEN rrp < 0 THEN 1 ELSE 0 END) OVER back12 AS negatives_60,

        -- Wind now, and how fast it is moving. Ramp is the commercial signal:
        -- a region losing 400 MW of wind in half an hour reprices.
        wind_mw,
        wind_share,
        wind_mw - lag(wind_mw, 1) OVER o AS wind_ramp_5,
        wind_mw - lag(wind_mw, 6) OVER o AS wind_ramp_30,
        wind_mw - lag(wind_mw, 12) OVER o AS wind_ramp_60,
        avg(wind_share) OVER back12 AS wind_share_mean_60,

        -- Calendar. Sine/cosine pairs so 23:55 is adjacent to 00:00 rather
        -- than 287 units away from it.
        extract('hour' FROM interval_start_nem) * 12
            + extract('minute' FROM interval_start_nem) / 5 AS interval_of_day,
        extract('dow' FROM interval_start_nem) AS day_of_week,
        extract('month' FROM interval_start_nem) AS month,
        CASE WHEN extract('dow' FROM interval_start_nem) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend
    FROM targeted
    WINDOW
        o AS (PARTITION BY region ORDER BY interval_start_utc),
        back6 AS (PARTITION BY region ORDER BY interval_start_utc
                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        back12 AS (PARTITION BY region ORDER BY interval_start_utc
                   ROWS BETWEEN 12 PRECEDING AND CURRENT ROW),
        back48 AS (PARTITION BY region ORDER BY interval_start_utc
                   ROWS BETWEEN 48 PRECEDING AND CURRENT ROW)
)

SELECT
    region,
    interval_start_utc,
    interval_start_nem,
    trading_date,
    rrp,

    y_mean_60, y_mean_120, y_spike_60, y_spike_120,
    -- A target window short of its full span sits at the end of the data or
    -- across a gap. Carried rather than dropped, so a caller decides.
    y_intervals_60, y_intervals_120,

    rrp_lag_5, rrp_lag_10, rrp_lag_15, rrp_lag_30, rrp_lag_60, rrp_lag_120,
    rrp_mean_30, rrp_mean_60, rrp_mean_240,
    rrp_std_60, rrp_std_240,
    rrp_min_60, rrp_max_60, rrp_min_240, rrp_max_240,
    spikes_60, spikes_240, negatives_60,

    wind_mw, wind_share, wind_ramp_5, wind_ramp_30, wind_ramp_60, wind_share_mean_60,

    CAST(interval_of_day AS INTEGER) AS interval_of_day,
    CAST(day_of_week AS INTEGER) AS day_of_week,
    CAST(month AS INTEGER) AS month,
    is_weekend,
    sin(2 * pi() * interval_of_day / 288.0) AS tod_sin,
    cos(2 * pi() * interval_of_day / 288.0) AS tod_cos,
    sin(2 * pi() * day_of_week / 7.0) AS dow_sin,
    cos(2 * pi() * day_of_week / 7.0) AS dow_cos
FROM featured
ORDER BY region, interval_start_utc
