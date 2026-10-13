-- Gold: what each farm's output was worth, per trading day.
--
-- Energy is settled at the regional reference price of the interval it was
-- generated in, so revenue is summed interval by interval rather than taken
-- as daily energy times a daily average price. Those two differ whenever
-- output correlates with price, which for wind it always does — that
-- correlation is the whole subject.
--
-- negative_price_mwh is the energy generated while the price was below zero:
-- the volume the farm paid to produce.
--
-- Parameters: $wind_glob, $price_glob

WITH settled AS (
    SELECT
        w.trading_date,
        w.duid,
        w.station_name,
        w.region,
        w.capacity_mw,
        w.mw * 5.0 / 60.0 AS mwh,
        p.rrp
    FROM read_parquet($wind_glob) w
    JOIN read_parquet($price_glob) p
        ON p.interval_start_utc = w.interval_start_utc
       AND p.region = w.region
)

SELECT
    trading_date,
    duid,
    any_value(station_name) AS station_name,
    any_value(region) AS region,
    any_value(capacity_mw) AS capacity_mw,
    sum(mwh) AS energy_mwh,
    sum(mwh * rrp) AS revenue_aud,
    -- What the farm actually earned per MWh, which is the number that matters
    -- and is almost never the day's average price.
    CASE WHEN sum(mwh) <> 0 THEN sum(mwh * rrp) / sum(mwh) END AS realised_price_aud_mwh,
    coalesce(sum(mwh) FILTER (WHERE rrp < 0), 0) AS negative_price_mwh,
    coalesce(sum(mwh * rrp) FILTER (WHERE rrp < 0), 0) AS negative_price_revenue_aud,
    count(*) AS intervals
FROM settled
GROUP BY trading_date, duid
ORDER BY trading_date, duid
