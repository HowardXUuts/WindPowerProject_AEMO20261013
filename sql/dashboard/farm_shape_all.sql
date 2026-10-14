-- Dashboard: the merchant profile — when each farm generates, against when its
-- region pays.
--
-- Averaged by hour of day over the window, which is the view that answers the
-- question behind a wind project's revenue: a farm whose output peaks at 3am
-- earns the 3am price, however good its capacity factor looks. Pairing the two
-- shapes on a shared x axis is what makes the mismatch visible; they are two
-- charts rather than one with two y axes.
--
-- Windowed to 14 days so the shape reflects current behaviour and the per-farm
-- files stay a few kilobytes as history accumulates.
--
-- Parameters: $wind_glob, $price_glob
WITH window_start AS (
    SELECT max(trading_date) - INTERVAL 13 DAY AS first_date
    FROM read_parquet($wind_glob)
),

joined AS (
    SELECT
        w.duid,
        extract('hour' FROM w.interval_start_nem) AS hour_nem,
        w.mw,
        p.rrp
    FROM read_parquet($wind_glob) w
    CROSS JOIN window_start s
    LEFT JOIN read_parquet($price_glob) p
        ON p.interval_start_utc = w.interval_start_utc
       AND p.region = w.region
    WHERE w.trading_date >= s.first_date
)

SELECT
    duid,
    CAST(hour_nem AS INTEGER) AS hour_nem,
    printf('%02d:00', CAST(hour_nem AS INTEGER)) AS hour,
    round(avg(mw), 1) AS avg_mw,
    round(avg(rrp), 2) AS avg_rrp,
    count(*) AS intervals
FROM joined
GROUP BY duid, hour_nem
ORDER BY duid, hour_nem
