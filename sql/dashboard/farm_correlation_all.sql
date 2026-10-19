-- Dashboard: each farm's most and least correlated counterparts, fanned out
-- per farm by export_dashboard.
-- Parameters: $farm_correlation
SELECT
    duid,
    relation,
    rank,
    other_duid,
    other_name,
    other_region,
    same_region,
    r
FROM read_parquet($farm_correlation)
ORDER BY duid, relation, rank
