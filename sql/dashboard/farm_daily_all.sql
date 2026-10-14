-- Dashboard: the daily record for every farm, split per farm by the exporter.
--
-- capture_ratio compares what the farm earned per MWh with the time-weighted
-- average price in its region that day. Below 1.0 is the cannibalisation
-- discount: the farm generated when its neighbours did and the price fell. It
-- is the number that separates two farms with identical capacity factors and
-- very different revenue.
--
-- Parameters: $farm_daily, $farm_revenue, $capture
SELECT
    d.duid,
    CAST(d.trading_date AS VARCHAR) AS trading_date,
    round(d.energy_mwh, 1) AS energy_mwh,
    round(d.capacity_factor, 4) AS capacity_factor,
    round(d.peak_mw, 1) AS peak_mw,
    d.intervals,
    d.is_complete,
    round(r.revenue_aud, 0) AS revenue_aud,
    round(r.realised_price_aud_mwh, 2) AS capture_price_aud_mwh,
    round(c.twap_aud_mwh, 2) AS twap_aud_mwh,
    round(r.realised_price_aud_mwh / nullif(c.twap_aud_mwh, 0), 3) AS capture_ratio,
    round(r.negative_price_mwh, 1) AS negative_mwh,
    round(r.revenue_aud / nullif(d.capacity_mw, 0), 0) AS revenue_per_mw_day
FROM read_parquet($farm_daily) d
LEFT JOIN read_parquet($farm_revenue) r USING (duid, trading_date)
LEFT JOIN read_parquet($capture) c
    ON c.trading_date = d.trading_date AND c.region = d.region
ORDER BY d.duid, d.trading_date
