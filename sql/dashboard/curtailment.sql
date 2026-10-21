-- Dashboard: wind that was available and did not run, by region and day.
-- Parameters: $curtailment
SELECT
    region,
    CAST(trading_date AS VARCHAR) AS trading_date,
    available_mwh,
    actual_mwh,
    curtailed_mwh,
    pct_of_available,
    economic_mwh,
    network_mwh,
    curtailed_intervals,
    intervals,
    tolerance_pct
FROM read_parquet($curtailment)
ORDER BY region, trading_date
