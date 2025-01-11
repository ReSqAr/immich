WITH
    CONSTANTS AS (
        SELECT
            $1::BIGINT            AS SEED,
            $2::INT               AS RESULT_LIMIT,
            $3::uuid[]            AS USER_IDS,
            INTERVAL '15 minutes' AS MIN_TIME_BETWEEN_PHOTOS
    ),

    year_data AS (
        SELECT
            EXTRACT(YEAR FROM ts) AS year,
            SQRT(COUNT(*))        AS weight
        FROM asset_analysis c
             CROSS JOIN CONSTANTS co
        WHERE
              c.normalized_quality_score >= 0
          AND c."ownerId" = ANY (co.USER_IDS)
        GROUP BY year, co.RESULT_LIMIT
        HAVING
            COUNT(*) > co.RESULT_LIMIT
    ),
    year_w AS (
        SELECT
            cd.year,
            cd.weight,
            SUM(cd.weight) OVER ()                 AS total_weight,
            SUM(cd.weight) OVER (ORDER BY cd.year) AS right_cumulative
        FROM year_data cd
    ),
    year_w2 AS (
        SELECT
            cw.year,
            cw.weight,
            cw.total_weight,
            cw.right_cumulative,
            COALESCE(
                            LAG(cw.right_cumulative) OVER (ORDER BY cw.year),
                            0
            ) AS left_cumulative
        FROM year_w cw
    ),
    chosen_year AS (

        SELECT
            cw2.year
        FROM year_w2 cw2
             CROSS JOIN CONSTANTS c
        WHERE
            c.SEED % ROUND(1367 * cw2.total_weight)::BIGINT BETWEEN 1367 * cw2.left_cumulative AND 1367 * cw2.right_cumulative
        LIMIT 1
    ),

    data AS (
        SELECT
            ad.id,
            ad.ts,
            1 + COALESCE(ad.normalized_quality_score, 0) AS weight
        FROM asset_analysis ad
             JOIN chosen_year cc ON EXTRACT(YEAR FROM ad.ts) = cc.year
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
    cc.year
FROM filtered_candidates fc
     CROSS JOIN CONSTANTS c
     CROSS JOIN chosen_year cc
WHERE
    fc.row_number <= c.RESULT_LIMIT
ORDER BY fc.ts
