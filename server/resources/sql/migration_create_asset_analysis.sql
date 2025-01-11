CREATE MATERIALIZED VIEW asset_analysis AS
WITH
    final AS (
        SELECT
            lp.id,
            lp."ownerId",
            lp.ts,

            /* add cluster info */
            lp.cluster_id,
            COALESCE(cs.cluster_cardinality, 0)                   AS cluster_cardinality,
            COALESCE(cs.cluster_cardinality_score_ge_0, 0)        AS cluster_cardinality_score_ge_0,
            COALESCE(cs.cluster_cardinality_score_ge_1, 0)        AS cluster_cardinality_score_ge_1,
            COALESCE(cs.cluster_start, lp.ts)                     AS cluster_start,
            COALESCE(cs.cluster_end, lp.ts)                       AS cluster_end,
            COALESCE(cs.cluster_duration, INTERVAL '0')           AS cluster_duration,
            lp.is_core,
            lp.is_noise,
            lp.is_border,
            CASE WHEN lp.is_noise THEN 'noise' ELSE 'cluster' END AS label,

            /* add location data from exif */
            e.city,
            e.state,
            e.country,

            /* add quality scores - both raw and normalized using window functions */
            q.score                                               AS quality_score,
            CASE
                WHEN q.score IS NOT NULL AND STDDEV(q.score) OVER (PARTITION BY lp."ownerId") != 0
                    THEN (q.score - AVG(q.score) OVER (PARTITION BY lp."ownerId")) / STDDEV(q.score) OVER (PARTITION BY lp."ownerId")
                END                                               AS normalized_quality_score,

            /* time-based density metrics */
            COUNT(*) FILTER (WHERE TRUE) OVER (
                PARTITION BY lp."ownerId"
                ORDER BY lp.ts
                RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND INTERVAL '5 minutes' FOLLOWING
                )                                                 AS neighbors_5m,

            COUNT(*) FILTER (WHERE TRUE) OVER (
                PARTITION BY lp."ownerId"
                ORDER BY lp.ts
                RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND INTERVAL '1 hour' FOLLOWING
                )                                                 AS neighbors_1h,

            COUNT(*) FILTER (WHERE TRUE) OVER (
                PARTITION BY lp."ownerId"
                ORDER BY lp.ts
                RANGE BETWEEN INTERVAL '1 day' PRECEDING AND INTERVAL '1 day' FOLLOWING
                )                                                 AS neighbors_1d,

            COUNT(*) FILTER (WHERE TRUE) OVER (
                PARTITION BY lp."ownerId"
                ORDER BY lp.ts
                RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '7 days' FOLLOWING
                )                                                 AS neighbors_7d,

            COUNT(*) FILTER (WHERE TRUE) OVER (
                PARTITION BY lp."ownerId"
                ORDER BY lp.ts
                RANGE BETWEEN INTERVAL '30 days' PRECEDING AND INTERVAL '30 days' FOLLOWING
                )                                                 AS neighbors_30d,

            COUNT(*) FILTER (WHERE TRUE) OVER (
                PARTITION BY lp."ownerId"
                ORDER BY lp.ts
                RANGE BETWEEN INTERVAL '180 days' PRECEDING AND INTERVAL '180 days' FOLLOWING
                )                                                 AS neighbors_180d

        FROM asset_dbscan lp
             LEFT JOIN asset_dbscan_clusters cs ON lp.cluster_id = cs.cluster_id
             LEFT JOIN exif e ON lp.id = e."assetId"
             LEFT JOIN quality_assessments q ON lp.id = q."assetId"
    )
SELECT *
FROM final;
