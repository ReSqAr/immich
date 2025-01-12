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
            c.cluster_id,
            c.cluster_start,
            c.cluster_end,
            c."ownerId" AS owner_id,
            JSONB_BUILD_OBJECT(
                    'cities', c.cities,
                    'states', c.states,
                    'countries', c.countries
            )           AS cluster_location_distribution,
            c.cluster_cardinality_score_ge_0
        FROM asset_dbscan_clusters c
             CROSS JOIN CONSTANTS co
        WHERE
              c.cluster_cardinality_score_ge_0 >= co.RESULT_LIMIT
          AND c."ownerId" = ANY (co.USER_IDS)
    ),

    asset_data AS (
        SELECT
            ad.id,
            ad.ts,
            ad.cluster_id,
            ad."ownerId" AS owner_id,
            ad.normalized_quality_score
        FROM asset_analysis ad
             CROSS JOIN CONSTANTS co
        WHERE
              ad.normalized_quality_score >= 0
          AND ad."ownerId" = ANY (co.USER_IDS)
    ),

    selected_cluster AS (
        WITH
            weighted_data AS (
                SELECT
                    d.cluster_id,
                    d.owner_id,
                    d.cluster_start,
                    d.cluster_end,
                    d.cluster_location_distribution,
                    SQRT(d.cluster_cardinality_score_ge_0) AS weight
                FROM cluster_data d
                     CROSS JOIN CONSTANTS c
                WHERE
                    d.cluster_cardinality_score_ge_0 >= c.RESULT_LIMIT
            ),

            weighted_data_running_sum AS (
                SELECT
                    wd.*,
                    SUM(wd.weight) OVER ()                       AS total_weight,
                    SUM(wd.weight) OVER (ORDER BY wd.cluster_id) AS right_cumulative
                FROM weighted_data wd
            ),

            weighted_data_bands AS (
                SELECT
                    wdrs.*,
                    COALESCE(LAG(wdrs.right_cumulative) OVER (ORDER BY wdrs.cluster_id), 0) AS left_cumulative
                FROM weighted_data_running_sum wdrs
            )

        SELECT
            wdb.cluster_id,
            wdb.owner_id,
            wdb.cluster_start,
            wdb.cluster_end,
            wdb.cluster_location_distribution
        FROM weighted_data_bands wdb
             CROSS JOIN CONSTANTS c
        WHERE
            (c.SEED % ROUND(1367 * wdb.total_weight)::BIGINT)
                BETWEEN 1367 * wdb.left_cumulative AND 1367 * wdb.right_cumulative
        LIMIT 1
    ),

    selected_assets AS (
        WITH
            weighted_data AS (
                SELECT
                    d.id,
                    d.ts,
                    1 + d.normalized_quality_score AS weight
                FROM asset_data d
                     JOIN selected_cluster sy USING (cluster_id, owner_id)
            ),

            weighted_data_running_sum AS (
                SELECT
                    wd.*,
                    SUM(wd.weight) OVER ()               AS total_weight,
                    SUM(wd.weight) OVER (ORDER BY wd.ts) AS right_cumulative
                FROM weighted_data wd
            ),

            weighted_data_bands AS (
                SELECT
                    wdrs.*,
                    COALESCE(LAG(wdrs.right_cumulative) OVER (ORDER BY wdrs.ts), 0) AS left_cumulative
                FROM weighted_data_running_sum wdrs
            ),

            candidates AS (
                SELECT
                    i AS draw_number,
                    wb.id,
                    wb.ts
                FROM weighted_data_bands wb
                     CROSS JOIN CONSTANTS c
                     JOIN GENERATE_SERIES(0, 2 * c.RESULT_LIMIT) i
                          ON (((c.SEED # i)::BIGINT * 73244475::BIGINT) % 4294967296::BIGINT) % ROUND(1367 * wb.total_weight)::BIGINT
                              BETWEEN 1367 * wb.left_cumulative AND 1367 * wb.right_cumulative
            ),

            candidates_with_lookback AS (
                SELECT
                    c.draw_number,
                    c.id,
                    c.ts,
                    LAG(c.ts) OVER (ORDER BY c.ts, c.draw_number) AS prev_ts
                FROM candidates c
            ),

            filtered_candidates AS (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY cwl.draw_number) AS draw_number,
                    cwl.id,
                    cwl.ts
                FROM candidates_with_lookback cwl
                     CROSS JOIN CONSTANTS c
                WHERE
                     cwl.prev_ts IS NULL
                  OR cwl.ts - cwl.prev_ts >= c.MIN_TIME_BETWEEN_PHOTOS
            )

        SELECT
            saf.draw_number,
            saf.id,
            saf.ts
        FROM filtered_candidates saf
             CROSS JOIN CONSTANTS c
        WHERE
            saf.draw_number <= c.RESULT_LIMIT
    )

SELECT
    sa.id,
    sc.cluster_id,
    sc.cluster_start,
    sc.cluster_end,
    sc.cluster_location_distribution
FROM selected_assets sa
     CROSS JOIN selected_cluster sc
ORDER BY sa.ts
