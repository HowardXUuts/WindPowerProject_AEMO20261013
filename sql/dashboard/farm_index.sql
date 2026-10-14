-- Dashboard: the farm picker, and the scorecard each farm is ranked by.
--
-- One row per farm, aggregated over every day held. The measures are the ones
-- a developer compares projects on, not the ones that are easiest to compute:
--
--   capacity_factor     yield against nameplate — the bankability number
--   capture_price       what the farm actually earned per MWh
--   revenue_per_mw_day  revenue normalised by installed capacity, so a 453 MW
--                       farm and a 20 MW farm are comparable at all
--   negative_pct        share of output sold below zero — merchant downside
--
-- Parameters: $farm_daily, $farm_revenue
WITH daily AS (
    SELECT
        duid,
        any_value(station_name) AS station_name,
        any_value(region) AS region,
        any_value(capacity_mw) AS capacity_mw,
        sum(energy_mwh) AS energy_mwh,
        max(peak_mw) AS peak_mw,
        count(*) AS days,
        sum(intervals) AS intervals,
        bool_and(is_complete) AS all_days_complete
    FROM read_parquet($farm_daily)
    GROUP BY duid
),

revenue AS (
    SELECT
        duid,
        sum(revenue_aud) AS revenue_aud,
        sum(energy_mwh) AS settled_mwh,
        sum(negative_price_mwh) AS negative_mwh
    FROM read_parquet($farm_revenue)
    GROUP BY duid
)

SELECT
    d.duid,
    d.station_name,
    d.region,
    round(d.capacity_mw, 1) AS capacity_mw,
    d.days,
    d.all_days_complete,
    round(d.energy_mwh, 1) AS energy_mwh,
    -- Capacity factor over the intervals actually present, not over 24 h x
    -- days: a sealed part-day would otherwise drag a farm down for a gap in
    -- the pipeline rather than a gap in the wind.
    round(d.energy_mwh / nullif(d.intervals * 5.0 / 60.0 * d.capacity_mw, 0), 4) AS capacity_factor,
    round(100.0 * d.peak_mw / nullif(d.capacity_mw, 0), 1) AS peak_pct_of_capacity,
    round(r.revenue_aud, 0) AS revenue_aud,
    round(r.revenue_aud / nullif(r.settled_mwh, 0), 2) AS capture_price_aud_mwh,
    round(r.revenue_aud / nullif(d.capacity_mw * d.days, 0), 0) AS revenue_per_mw_day,
    round(100.0 * r.negative_mwh / nullif(r.settled_mwh, 0), 2) AS negative_pct
FROM daily d
LEFT JOIN revenue r USING (duid)
ORDER BY d.station_name, d.duid
