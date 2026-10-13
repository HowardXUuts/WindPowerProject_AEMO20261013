-- Dashboard: the ten best and ten worst farms by capacity factor this week.
--
-- Aggregated over whole days rather than averaged per day, so a farm present
-- for only part of the week is not flattered by its good days alone. days_seen
-- travels with each row so a short record is visible.
--
-- Parameters: $farm_daily
WITH recent AS (
    SELECT *
    FROM read_parquet($farm_daily)
    WHERE trading_date > (SELECT max(trading_date) FROM read_parquet($farm_daily))
                         - INTERVAL 7 DAY
),

per_farm AS (
    SELECT
        duid,
        any_value(station_name) AS station_name,
        any_value(region) AS region,
        any_value(capacity_mw) AS capacity_mw,
        round(sum(energy_mwh), 1) AS energy_mwh,
        round(sum(energy_mwh) / nullif(sum(capacity_mw * 24.0), 0), 4) AS capacity_factor,
        count(*) AS days_seen
    FROM recent
    GROUP BY duid
)

SELECT * FROM (SELECT *, 'top' AS band FROM per_farm ORDER BY capacity_factor DESC LIMIT 10)
UNION ALL
SELECT * FROM (SELECT *, 'bottom' AS band FROM per_farm ORDER BY capacity_factor ASC LIMIT 10)
