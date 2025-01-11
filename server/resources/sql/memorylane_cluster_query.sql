WITH
    CONSTANTS AS (
        SELECT
            $1::BIGINT            AS SEED,
            $2::INT               AS RESULT_LIMIT,
            $3::uuid[]            AS USER_IDS,
            INTERVAL '15 minutes' AS MIN_TIME_BETWEEN_PHOTOS
    ),

    cluster_data AS (
        SELECT
            c.cluster_id                           AS cluster_id,
            c.cluster_start                        AS cluster_start,
            c.cluster_end                          AS cluster_end,
            JSONB_BUILD_OBJECT(
                    'cities', c.cities,
                    'states', c.states,
                    'countries', c.countries
            )                                      AS cluster_location_distribution,
            SQRT(c.cluster_cardinality_score_ge_0) AS weight
        FROM asset_dbscan_clusters c
             CROSS JOIN CONSTANTS co
        WHERE
              c.cluster_cardinality_score_ge_0 >= co.RESULT_LIMIT
          AND c."ownerId" = ANY (co.USER_IDS)
    ),
    cluster_w AS (
        SELECT
            cd.cluster_id,
            cd.cluster_start,
            cd.cluster_end,
            cd.cluster_location_distribution,
            cd.weight,
            SUM(cd.weight) OVER ()                       AS total_weight,
            SUM(cd.weight) OVER (ORDER BY cd.cluster_id) AS right_cumulative
        FROM cluster_data cd
    ),
    cluster_w2 AS (
        SELECT
            cw.cluster_id,
            cw.cluster_start,
            cw.cluster_end,
            cw.cluster_location_distribution,
            cw.weight,
            cw.total_weight,
            cw.right_cumulative,
            COALESCE(
                            LAG(cw.right_cumulative) OVER (ORDER BY cw.cluster_id),
                            0
            ) AS left_cumulative
        FROM cluster_w cw
    ),
    chosen_cluster AS (
        SELECT
            cw2.cluster_id,
            cw2.cluster_start,
            cw2.cluster_end,
            cw2.cluster_location_distribution
        FROM cluster_w2 cw2
             CROSS JOIN CONSTANTS c
        WHERE
            (c.SEED % ROUND(1367 * cw2.total_weight)::BIGINT) BETWEEN 1367 * cw2.left_cumulative AND 1367 * cw2.right_cumulative
        LIMIT 1
    ),

    data AS (
        SELECT
            ad.id,
            ad.ts,
            1 + ad.normalized_quality_score AS weight
        FROM asset_analysis ad
             JOIN chosen_cluster cc ON ad.cluster_id = cc.cluster_id
        WHERE
            ad.normalized_quality_score >= 0
    ),
    w AS (
        SELECT
            d.id,
            d.ts,
            d.weight,
            SUM(d.weight) OVER ()              AS total_weight,
            SUM(d.weight) OVER (ORDER BY d.ts) AS right_cumulative
        FROM data d
    ),
    w2 AS (
        SELECT
            w.id,
            w.ts,
            w.weight,
            w.total_weight,
            w.right_cumulative,
            COALESCE(LAG(w.right_cumulative) OVER (ORDER BY w.ts), 0) AS left_cumulative
        FROM w
    ),
    candidates AS (
        SELECT
            i AS draw_number,
            w2.id,
            w2.ts
        FROM w2
             CROSS JOIN CONSTANTS c
             JOIN GENERATE_SERIES(0, 2 * c.RESULT_LIMIT) i
                  ON (((c.SEED # i)::BIGINT * 73244475::BIGINT) % 4294967296::BIGINT) %
                     ROUND(1367 * w2.total_weight)::BIGINT BETWEEN 1367 * w2.left_cumulative AND 1367 * w2.right_cumulative
        ORDER BY draw_number
    ),
    candidates_with_prev AS (
        SELECT
            a.draw_number,
            a.id,
            a.ts,
            LAG(a.ts) OVER (ORDER BY a.ts) AS prev_ts
        FROM candidates a
             CROSS JOIN CONSTANTS c
    ),
    filtered_candidates AS (
        SELECT
            sc.draw_number,
            sc.id,
            sc.ts,
            ROW_NUMBER() OVER (ORDER BY sc.draw_number) AS row_number
        FROM candidates_with_prev sc
             CROSS JOIN CONSTANTS c
        WHERE
             sc.prev_ts IS NULL
          OR sc.ts - sc.prev_ts >= c.MIN_TIME_BETWEEN_PHOTOS
        ORDER BY sc.draw_number
    )


SELECT
    fc.id,
    cc.cluster_id,
    cc.cluster_start,
    cc.cluster_end,
    cc.cluster_location_distribution
FROM filtered_candidates fc
     CROSS JOIN CONSTANTS c
     CROSS JOIN chosen_cluster cc
WHERE
    fc.row_number <= c.RESULT_LIMIT
ORDER BY fc.ts
