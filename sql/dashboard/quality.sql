-- Dashboard: the quality table, most serious first.
-- Parameters: $quality
SELECT
    check_name,
    severity,
    status,
    CAST(trading_date AS VARCHAR) AS trading_date,
    subject,
    observed,
    message,
    strftime(checked_at AT TIME ZONE 'Australia/Brisbane', '%Y-%m-%d %H:%M') AS checked_at_nem
FROM read_parquet($quality)
ORDER BY
    CASE status WHEN 'fail' THEN 0 WHEN 'warn' THEN 1 WHEN 'skip' THEN 2 ELSE 3 END,
    check_name,
    trading_date
