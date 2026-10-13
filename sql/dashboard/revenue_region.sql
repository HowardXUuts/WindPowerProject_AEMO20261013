-- Dashboard: revenue and negative-price exposure by region.
--
-- negative_mwh is energy generated while the regional price was below zero —
-- volume the farms paid to produce — and negative_pct puts it against total
-- output so a large region does not look worse simply for being large.
--
-- Parameters: $farm_revenue
SELECT
    region,
    round(sum(energy_mwh), 1) AS energy_mwh,
    round(sum(revenue_aud), 0) AS revenue_aud,
    round(sum(revenue_aud) / nullif(sum(energy_mwh), 0), 2) AS realised_price_aud_mwh,
    round(sum(negative_price_mwh), 1) AS negative_mwh,
    round(100.0 * sum(negative_price_mwh) / nullif(sum(energy_mwh), 0), 2) AS negative_pct,
    round(sum(negative_price_revenue_aud), 0) AS negative_revenue_aud,
    count(DISTINCT trading_date) AS days
FROM read_parquet($farm_revenue)
GROUP BY region
ORDER BY revenue_aud DESC
