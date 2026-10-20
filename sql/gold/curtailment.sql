-- Gold: wind that was available and did not run, split by why.
--
-- Three things look identical in SCADA output: the wind did not blow, the farm
-- backed off because the price had gone negative, and AEMO capped it for a
-- network or system constraint. A capacity factor cannot separate them. The
-- first is weather and nobody's problem; the second is a decision a battery or
-- a different offtake can change; the third is a risk that does not appear
-- anywhere in a yield figure.
--
-- Availability comes from AEMO's own `SS_WIND_UIGF`, the unconstrained
-- intermittent generation forecast for semi-scheduled wind. The gap between it
-- and metered output is the curtailment estimate.
--
-- **Only semi-scheduled units, and that is the whole reason this model took so
-- long to arrive.** `SS_WIND_UIGF` covers semi-scheduled units alone, while the
-- registry holds 27 wind DUIDs of 109 that are not. Compared unfiltered,
-- Tasmania's metered output read 175 MW against 109 MW of availability — above
-- its own forecast, which is impossible for the same units and was the proof
-- the populations differed. Filtered, Tasmania reads 107 MW against 109 MW and
-- the comparison becomes coherent. Nothing here was publishable before the
-- registry carried `schedule_type`; see PLAN.md section 5.
--
-- **This is a proxy, not AEMO's answer.** The definitive signal is the
-- `SEMIDISPATCHCAP` flag on each unit in `DISPATCHLOAD`, which says outright
-- that AEMO capped a semi-scheduled unit in that interval. This project does
-- not ingest `DISPATCHLOAD`, so the split below is inferred from the price
-- instead, and should be read as "consistent with" rather than "was".
--
-- The economic / network split is a heuristic with a known soft edge. A
-- negative price is an unambiguous reason to back off — a generator pays to
-- run. A positive price with output held below availability is more likely a
-- constraint. But a low positive price can also be below a farm's own
-- opportunity cost once certificates are counted, so some of what is counted
-- as network here is really economic. Stated rather than hidden, because the
-- fix is to ingest DISPATCHLOAD, not to tune the threshold.
--
-- A tolerance is required because UIGF is a *forecast*, not a measurement, and
-- a forecast that lands 2% high is ordinary. Intervals whose gap is inside
-- CURTAILMENT_TOLERANCE of availability are treated as forecast error and not
-- counted. The figure moves with that tolerance, so it is reported alongside.
--
-- One further caveat the data cannot settle: 82 DUIDs are registered
-- semi-scheduled and 81 appear in SCADA, so one registered unit contributes
-- availability it cannot meter. That understates actual and so overstates
-- curtailment, slightly and in one direction.
--
-- Float aggregates are rounded, and build_gold runs single-threaded, for the
-- byte-stability reason set out in capture_price.sql.
--
-- Parameters: $wind_glob, $price_glob, $regionsum_glob, $registry

WITH semi_scheduled AS (
    -- Trailing asterisks are AEMO footnote markers on the classification, not
    -- part of it: the live registry carries both "Non-Scheduled" and
    -- "Non-Scheduled*". Matched case-insensitively and trimmed so a future
    -- "Semi-Scheduled*" does not silently drop a farm out of the population.
    SELECT duid
    FROM read_csv($registry, header = true, auto_detect = true)
    WHERE lower(trim(trailing '*' FROM trim(schedule_type))) = 'semi-scheduled'
),

actual AS (
    SELECT
        w.region,
        w.trading_date,
        w.interval_start_utc,
        sum(w.mw) AS actual_mw
    FROM read_parquet($wind_glob) w
    JOIN semi_scheduled USING (duid)
    GROUP BY w.region, w.trading_date, w.interval_start_utc
),

available AS (
    SELECT
        settlementdate - INTERVAL 5 MINUTE AS interval_start_utc,
        regionid AS region,
        ss_wind_uigf AS available_mw
    FROM read_parquet($regionsum_glob)
    WHERE ss_wind_uigf IS NOT NULL
    QUALIFY row_number() OVER (
        PARTITION BY settlementdate, regionid
        ORDER BY intervention ASC, ingested_at DESC, source_file DESC
    ) = 1
),

joined AS (
    SELECT
        a.region,
        a.trading_date,
        a.actual_mw,
        v.available_mw,
        p.rrp,
        greatest(v.available_mw - a.actual_mw, 0) AS gap_mw,
        -- Inside the tolerance this is forecast error, not curtailment.
        greatest(v.available_mw - a.actual_mw, 0)
            > 0.05 * nullif(v.available_mw, 0) AS beyond_tolerance
    FROM actual a
    JOIN available v
        ON v.region = a.region
       AND v.interval_start_utc = a.interval_start_utc
    JOIN read_parquet($price_glob) p
        ON p.region = a.region
       AND p.interval_start_utc = a.interval_start_utc
)

SELECT
    region,
    trading_date,
    round(sum(available_mw) * 5.0 / 60.0, 3) AS available_mwh,
    round(sum(actual_mw) * 5.0 / 60.0, 3) AS actual_mwh,
    round(coalesce(sum(gap_mw) FILTER (WHERE beyond_tolerance), 0) * 5.0 / 60.0, 3)
        AS curtailed_mwh,
    round(
        100.0 * coalesce(sum(gap_mw) FILTER (WHERE beyond_tolerance), 0)
        / nullif(sum(available_mw), 0), 4
    ) AS pct_of_available,
    -- A negative price is an unambiguous reason to stop: the generator pays to
    -- run. Attributed to the operator's own decision.
    -- coalesced to zero, not left null: a region with curtailment but no
    -- negative-price interval curtailed genuinely zero MWh for that reason, and
    -- economic + network must equal curtailed for any consumer that adds them.
    -- (Contrast revenue_concentration, where a null means no such interval
    -- existed at all, which is a different fact.)
    round(coalesce(sum(gap_mw) FILTER (WHERE beyond_tolerance AND rrp < 0), 0) * 5.0 / 60.0, 3)
        AS economic_mwh,
    -- Held below availability while the price was positive. More likely a
    -- constraint, but see the heuristic's soft edge above.
    round(coalesce(sum(gap_mw) FILTER (WHERE beyond_tolerance AND rrp >= 0), 0) * 5.0 / 60.0, 3)
        AS network_mwh,
    count(*) AS intervals,
    count(*) FILTER (WHERE beyond_tolerance) AS curtailed_intervals,
    -- Published so the figure is never read without the assumption behind it.
    5.0 AS tolerance_pct
FROM joined
GROUP BY region, trading_date
ORDER BY region, trading_date
