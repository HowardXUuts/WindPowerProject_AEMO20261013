-- ML: AEMO's own five-minute forecast, as the baseline a model must beat.
--
-- PLAN 4.6 requires every model be reported against P5MIN, and says plainly
-- that failing to beat it is a finding rather than something to hide. This puts
-- P5MIN on the same grid and the same target window as `features.sql`, so the
-- comparison is like for like.
--
-- **Joined on issue time, never valid time.** The leakage rule exists because
-- joining a forecast on the interval it describes hands the model the answer.
-- Measured from the data rather than assumed: a run named R carries exactly 12
-- intervals with `interval_datetime` from R to R+55, and its `lastchanged`
-- averages 4.29 minutes *before* R — so the run named R is published at about
-- R - 4.3 min. A feature row keyed `t` may use anything known by the end of
-- interval `t`, which is `t + 5 min`, so the latest admissible run is the one
-- named `t + 5 min`. The run named `t + 10` is published at about `t + 5.7` and
-- is therefore inadmissible by about forty seconds. The ASOF join below encodes
-- exactly that cutoff.
--
-- **`interval_datetime` is interval-ending**, like every other AEMO timestamp
-- here, so interval_start = interval_datetime - 5 min. Verified by the run
-- shape above: the first interval of a run published mid-interval is the
-- interval then in progress.
--
-- **Sixty minutes only, and that is AEMO's limit, not a shortcut.** A run
-- reaches 55 minutes past its own name, so against the 60-minute target
-- (interval starts t+5 .. t+60) it covers t+5 .. t+55 — eleven of twelve
-- intervals. The count is published so the shortfall is visible. Against the
-- 120-minute target it covers eleven of twenty-four, which is not a forecast of
-- that window in any useful sense, so no P5MIN column is emitted for it. The
-- right comparator there is PREDISPATCH, which runs four half-hours ahead and
-- is already in bronze; that is a separate piece of work.
--
-- Parameters: $p5min_glob, $price_glob, $spike_threshold

WITH grid AS (
    SELECT region, interval_start_utc
    FROM read_parquet($price_glob)
),

runs AS (
    SELECT
        run_datetime,
        regionid AS region,
        interval_datetime,
        rrp
    FROM read_parquet($p5min_glob)
    WHERE intervention = 0
    QUALIFY row_number() OVER (
        PARTITION BY run_datetime, regionid, interval_datetime
        ORDER BY ingested_at DESC, source_file DESC
    ) = 1
),

-- The latest run each row is allowed to see. ASOF takes the greatest
-- run_datetime satisfying the cutoff, which is the rule stated above.
admissible AS (
    SELECT g.region, g.interval_start_utc, r.run_datetime
    FROM grid g
    ASOF JOIN (SELECT DISTINCT region, run_datetime FROM runs) r
        ON r.region = g.region
       AND r.run_datetime <= g.interval_start_utc + INTERVAL 5 MINUTE
)

SELECT
    a.region,
    a.interval_start_utc,
    a.run_datetime AS p5_run_datetime,
    avg(r.rrp) AS p5_mean_60,
    -- Does AEMO's own forecast call a spike anywhere in the window? The same
    -- question the spike classifier is asked, so the two are comparable.
    max(CASE WHEN r.rrp > $spike_threshold THEN 1.0 ELSE 0.0 END) AS p5_spike_60,
    count(r.rrp) AS p5_intervals_60
FROM admissible a
LEFT JOIN runs r
    ON r.region = a.region
   AND r.run_datetime = a.run_datetime
   -- Target window in interval-start terms is t+5 .. t+60, which is
   -- interval_datetime t+10 .. t+65.
   AND r.interval_datetime > a.interval_start_utc + INTERVAL 5 MINUTE
   AND r.interval_datetime <= a.interval_start_utc + INTERVAL 65 MINUTE
GROUP BY a.region, a.interval_start_utc, a.run_datetime
-- The full window or nothing. Where ingestion gapped, the ASOF join falls back
-- to an older run that overlaps the target only partially — a forecast of the
-- last six intervals is not a forecast of the window, and averaging it as one
-- put Queensland's P5MIN MAE at 122 against persistence's 9. About 110 rows of
-- 9,405 are affected; dropping them costs 1% of the sample and removes an
-- artefact that dominated the comparison. Eleven is twelve minus the interval
-- AEMO's own horizon cannot reach.
HAVING count(r.rrp) = 11
ORDER BY a.region, a.interval_start_utc
