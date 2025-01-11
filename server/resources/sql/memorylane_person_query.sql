
    WITH
        CONSTANTS AS (
            SELECT
                $1::BIGINT            AS SEED,
                $2::INT               AS RESULT_LIMIT,
                $3::uuid[]            AS USER_IDS,
                INTERVAL '15 minutes' AS MIN_TIME_BETWEEN_PHOTOS,
                2 * $2::INT           AS MIN_PICTURES_PER_PERSON
        ),

/* ---------------------------------------------------------------------------
   1) Get all persons with their photo count (normalized score >= 1)
   and randomly select one person using weighted selection
--------------------------------------------------------------------------- */
        person_data AS (
            SELECT
                p.id                               AS person_id,
                p.name                             AS person_name,
                COUNT(DISTINCT af.id)              AS photo_count,
                SQRT(COUNT(DISTINCT af.id)::FLOAT) AS weight
            FROM person p
                 JOIN asset_faces af ON p.id = af."personId"
                 JOIN asset_analysis aa ON af."assetId" = aa.id
                 CROSS JOIN CONSTANTS co
            WHERE p."ownerId" = ANY (co.USER_IDS)
              AND COALESCE(aa.normalized_quality_score, 0) >= 0
            GROUP BY p.id, p.name, co.MIN_PICTURES_PER_PERSON
            HAVING COUNT(DISTINCT af.id) > co.MIN_PICTURES_PER_PERSON
        ),
        person_w AS (
            SELECT
                pd.person_id,
                pd.person_name,
                pd.weight,
                SUM(pd.weight) OVER ()                      AS total_weight,
                SUM(pd.weight) OVER (ORDER BY pd.person_id) AS right_cumulative
            FROM person_data pd
        ),
        person_w2 AS (
            SELECT
                pw.person_id,
                pw.person_name,
                pw.weight,
                pw.total_weight,
                pw.right_cumulative,
                COALESCE(LAG(pw.right_cumulative) OVER (ORDER BY pw.person_id), 0) AS left_cumulative
            FROM person_w pw
        ),

        chosen_person AS (
            SELECT
                pw2.person_id,
                pw2.person_name
            FROM person_w2 pw2
                 CROSS JOIN CONSTANTS c
            WHERE c.SEED % ROUND(1367 * pw2.total_weight)::BIGINT BETWEEN 1367 * pw2.left_cumulative AND 1367 * pw2.right_cumulative
            LIMIT 1
        ),

/* ---------------------------------------------------------------------------
   2) Get all photos of the chosen person with normalized score >= 1
   and select random photos with quality score weights
--------------------------------------------------------------------------- */
        data AS (
            SELECT
                aa.id,
                aa.ts,
                1 + COALESCE(aa.normalized_quality_score, 0) AS weight
            FROM asset_analysis aa
                 JOIN asset_faces af ON aa.id = af."assetId"
                 JOIN chosen_person cp ON af."personId" = cp.person_id
            WHERE aa.normalized_quality_score >= 0
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
   Final selection: Return chosen photos and person name
--------------------------------------------------------------------------- */
    SELECT
        fc.id,
        cp.person_name
    FROM filtered_candidates fc
         CROSS JOIN CONSTANTS c
         CROSS JOIN chosen_person cp
    WHERE fc.row_number <= c.RESULT_LIMIT
    ORDER BY draw_number;
