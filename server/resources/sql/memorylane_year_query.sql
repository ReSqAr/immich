WITH
    CONSTANTS AS (
        SELECT
            $1::BIGINT            AS SEED,
            $2::INT               AS RESULT_LIMIT,
            $3::uuid[]            AS USER_IDS,
            INTERVAL '15 minutes' AS MIN_TIME_BETWEEN_PHOTOS
    ),

    data AS (
        SELECT
            aa.*,
            EXTRACT(YEAR FROM aa.ts) AS year
        FROM asset_analysis aa
             CROSS JOIN CONSTANTS c
        WHERE
              aa."ownerId" = ANY (c.USER_IDS)
          AND aa.normalized_quality_score >= 0
    ),

    selected_year AS (
        WITH
            weighted_data AS (
                SELECT
                    d.year,
                    SQRT(COUNT(*)) AS weight
                FROM data d
                     CROSS JOIN CONSTANTS c
                GROUP BY d.year, c.RESULT_LIMIT
                HAVING
                    COUNT(*) > c.RESULT_LIMIT
            ),

            weighted_data_running_sum AS (
                SELECT
                    wd.*,
                    SUM(wd.weight) OVER ()                 AS total_weight,
                    SUM(wd.weight) OVER (ORDER BY wd.year) AS right_cumulative
                FROM weighted_data wd
            ),

            weighted_data_bands AS (
                SELECT
                    wdrs.*,
                    COALESCE(LAG(wdrs.right_cumulative) OVER (ORDER BY wdrs.year), 0) AS left_cumulative
                FROM weighted_data_running_sum wdrs
            )

        SELECT
            wdb.year
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
                FROM data d
                     JOIN selected_year USING (year)
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
    s.year
FROM selected_assets sa
     CROSS JOIN selected_year s
ORDER BY sa.ts
