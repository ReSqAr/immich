WITH
    CONSTANTS AS (
        SELECT
            INTERVAL '3 months' AS LOOKBACK_WINDOW,
            INTERVAL '6 HOURS'  AS MIN_TIME_BETWEEN_PHOTOS,
            0.0                 AS MIN_QUALITY_SCORE,
            $1::BIGINT          AS SEED,
            $2::INT             AS RESULT_LIMIT,
            $3::uuid[]          AS USER_IDS
    ),
    data AS (
        SELECT
            ad.id,
            ad.ts                           AS ts,
            1 + ad.normalized_quality_score AS weight
        FROM asset_analysis AS ad
             CROSS JOIN CONSTANTS c
        WHERE
              ad.ts >= CURRENT_TIMESTAMP - c.LOOKBACK_WINDOW
          AND ad.normalized_quality_score >= c.MIN_QUALITY_SCORE
          AND ad."ownerId" = ANY (c.USER_IDS)
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
            COALESCE(
                            LAG(w.right_cumulative) OVER (ORDER BY w.ts),
                            0
            ) AS left_cumulative
        FROM w
    ),
    candidates AS (
        SELECT
            i AS draw_number,
            w2.id,
            w2.ts,
            w2.weight
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
    id
FROM filtered_candidates
     CROSS JOIN CONSTANTS c
WHERE
    row_number <= c.RESULT_LIMIT
ORDER BY draw_number
