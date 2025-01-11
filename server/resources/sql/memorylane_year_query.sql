
  WITH
    CONSTANTS AS (
      SELECT
        $1::BIGINT            AS SEED,
        $2::INT               AS RESULT_LIMIT,
        $3::uuid[]            AS USER_IDS,
        INTERVAL '15 minutes' AS MIN_TIME_BETWEEN_PHOTOS
    ),

/* ---------------------------------------------------------------------------
   1) Randomly pick exactly ONE year.
      We do this by giving each year a weight = sqrt # of good assets, summing them, then doing
      one random draw.
--------------------------------------------------------------------------- */
    year_data AS (
      SELECT
        EXTRACT(YEAR FROM ts) AS year,
        SQRT(COUNT(*))        AS weight
      FROM asset_analysis c
           CROSS JOIN CONSTANTS co
      WHERE c.normalized_quality_score >= 0
        AND c."ownerId" = ANY (co.USER_IDS)
      GROUP BY year, co.RESULT_LIMIT
      HAVING COUNT(*) > co.RESULT_LIMIT
    ),
    year_w AS (
      /* Compute total_weight, plus a running sum (right_cumulative). */
      SELECT
        cd.year,
        cd.weight,
        SUM(cd.weight) OVER ()                 AS total_weight,
        SUM(cd.weight) OVER (ORDER BY cd.year) AS right_cumulative
      FROM year_data cd
    ),
    year_w2 AS (
      /* Define left_cumulative using LAG(...) */
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
      /*
         Pick the single year whose interval covers r % total_weight.
         This always returns exactly one row.
      */
      SELECT
        cw2.year
      FROM year_w2 cw2
           CROSS JOIN CONSTANTS c
      WHERE c.SEED % ROUND(1367 * cw2.total_weight)::BIGINT BETWEEN 1367 * cw2.left_cumulative AND 1367 * cw2.right_cumulative
      LIMIT 1
    ),

/* ---------------------------------------------------------------------------
   2) From that chosen year, select random photos:
      - Only photos with quality >= 0
      - Weight = 1 + normalized_quality_score
      - Enforce 15min min separation
      - Return up to 12 total
      - Sort final results by timestamp
--------------------------------------------------------------------------- */
    data AS (
      /* Pull assets in the chosen year and define the new weight. */
      SELECT
        ad.id,
        ad.ts,
        1 + COALESCE(ad.normalized_quality_score, 0) AS weight
      FROM asset_analysis ad
           JOIN chosen_year cc ON EXTRACT(YEAR FROM ad.ts) = cc.year
    ),
    w AS (
      /* Compute total_weight across these photos, plus their running total. */
      SELECT
        d.id,
        d.ts,
        d.weight,
        SUM(d.weight) OVER ()              AS total_weight,
        SUM(d.weight) OVER (ORDER BY d.ts) AS right_cumulative
      FROM data d
    ),
    w2 AS (
      /* LAG(...) to define each row's [left_cumulative, right_cumulative). */
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
      /*
         For each random draw, find the photo whose [left_cumulative, right_cumulative)
         covers r % total_weight. Order them by draw_number so we can filter by
         “first come, first served” below.
      */
      SELECT
        i AS draw_number,
        w2.id,
        w2.ts
        FROM w2
         CROSS JOIN CONSTANTS c
         JOIN generate_series(0, 2 * c.RESULT_LIMIT) i
              ON (((c.SEED # i)::BIGINT * 73244475::BIGINT) % 4294967296::BIGINT) % ROUND(1367 * w2.total_weight)::BIGINT BETWEEN 1367 * w2.left_cumulative AND 1367 * w2.right_cumulative
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
        WHERE sc.prev_ts IS NULL
           OR sc.ts - sc.prev_ts >= c.MIN_TIME_BETWEEN_PHOTOS
        ORDER BY sc.draw_number
    )

/* ---------------------------------------------------------------------------
   Final selection: up to 12 photos, sorted by time
--------------------------------------------------------------------------- */
  SELECT
    fc.id,
    cc.year
  FROM filtered_candidates fc
       CROSS JOIN CONSTANTS c
       CROSS JOIN chosen_year cc
  WHERE fc.row_number <= c.RESULT_LIMIT
  ORDER BY fc.ts
