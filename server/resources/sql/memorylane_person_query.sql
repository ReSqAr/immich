WITH
    CONSTANTS AS (
        SELECT
            $1::BIGINT            AS SEED,
            $2::INT               AS RESULT_LIMIT,
            $3::uuid[]            AS USER_IDS,
            INTERVAL '15 minutes' AS MIN_TIME_BETWEEN_PHOTOS
    ),

    person_data AS (
        SELECT
            p.id                  AS person_id,
            p.name                AS person_name,
            p."ownerId"           AS owner_id,
            COUNT(DISTINCT af.id) AS cardinality_score_ge_0
        FROM person p
             JOIN asset_faces af ON p.id = af."personId"
             JOIN asset_analysis aa ON af."assetId" = aa.id
             CROSS JOIN CONSTANTS co
        WHERE
              p."ownerId" = ANY (co.USER_IDS)
          AND COALESCE(aa.normalized_quality_score, 0) >= 0
        GROUP BY p.id, p.name
    ),

    asset_data AS (
        SELECT
            ad.id,
            ad.ts,
            af."personId"                            AS person_id,
            ad."ownerId"                             AS owner_id,
            COALESCE(ad.normalized_quality_score, 0) AS normalized_quality_score
        FROM asset_analysis ad
             JOIN asset_faces af ON ad.id = af."assetId"
             CROSS JOIN CONSTANTS co
        WHERE
              COALESCE(ad.normalized_quality_score, 0) >= 0
          AND ad."ownerId" = ANY (co.USER_IDS)
    ),

    selected_person AS (
        WITH
            weighted_data AS (
                SELECT
                    d.person_id,
                    d.person_name,
                    d.owner_id,
                    SQRT(d.cardinality_score_ge_0) AS weight
                FROM person_data d
                     CROSS JOIN CONSTANTS c
                WHERE
                    d.cardinality_score_ge_0 >= c.RESULT_LIMIT
            ),

            weighted_data_running_sum AS (
                SELECT
                    wd.*,
                    SUM(wd.weight) OVER ()                      AS total_weight,
                    SUM(wd.weight) OVER (ORDER BY wd.person_id) AS right_cumulative
                FROM weighted_data wd
            ),

            weighted_data_bands AS (
                SELECT
                    wdrs.*,
                    COALESCE(LAG(wdrs.right_cumulative) OVER (ORDER BY wdrs.person_id), 0) AS left_cumulative
                FROM weighted_data_running_sum wdrs
            )

        SELECT
            wdb.person_id,
            wdb.person_name,
            wdb.owner_id
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
                     JOIN selected_person USING (person_id, owner_id)
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
    s.person_id,
    s.person_name
FROM selected_assets sa
     CROSS JOIN selected_person s
ORDER BY sa.draw_number
