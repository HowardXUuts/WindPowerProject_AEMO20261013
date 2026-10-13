-- Dashboard: how current the data is, and where the fleet sits right now.
--
-- source_layer says whether the newest reading came from silver or straight
-- from bronze, so a stale page is visible rather than merely suspected.
--
-- Parameters: $snapshot
SELECT
    strftime(any_value(interval_start_nem), '%Y-%m-%d %H:%M') AS latest_interval_nem,
    count(*) AS farms,
    round(sum(mw), 0) AS fleet_mw,
    round(sum(capacity_mw), 0) AS fleet_capacity_mw,
    round(100.0 * sum(mw) / nullif(sum(capacity_mw), 0), 1) AS fleet_pct,
    any_value(source_layer) AS source_layer
FROM read_parquet($snapshot)
