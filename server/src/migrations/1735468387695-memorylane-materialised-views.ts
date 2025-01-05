import {MigrationInterface, QueryRunner} from "typeorm";


const asset_home_detection_view = `
CREATE OR REPLACE VIEW asset_home_detection AS
WITH

-- 1) Some parameters
constants AS (
    SELECT
        5000  AS radius_m,    -- 5 km radius
        0.005 AS deg_per_bin, -- latitude/longitude bin factor
        25    AS bin_delta -- upper bound for radius expressed as bin widths

    -- the math looks as follows:
    -- 1km ≈ 0.009° (equator) - 0.023° (Reykjavik)
    -- → Δ = 0.005° ≈ 556m - 2556m (bin width)
    -- → 5km ≈ 0.045° - 0.115° → Δ ≈ 9 - 23 bins
),

-- 2) Original data
data AS (
    SELECT
        a.id,
        a."ownerId",
        a."localDateTime" AS ts,
        e.latitude,
        e.longitude
    FROM assets a
         JOIN exif e ON a.id = e."assetId"
    WHERE a."deletedAt" IS NULL
      AND e.latitude IS NOT NULL
      AND e.longitude IS NOT NULL
),

binned_data AS (
    SELECT
        id,
        "ownerId",
        latitude,
        longitude,

        DATE_TRUNC('day', ts)          AS sector_ts_bin_day,
        FLOOR(latitude / deg_per_bin)  AS latitude_bin,
        FLOOR(longitude / deg_per_bin) AS longitude_bin
    FROM data
         CROSS JOIN constants c
),

-- 3) Collapse photos into "sector-day" rows
--    We'll pick a single representative lat/lon for that (owner, day, bin).
sector_day AS (
    SELECT
        "ownerId",
        sector_ts_bin_day,
        latitude_bin,
        longitude_bin,
        AVG(latitude)                              AS rep_latitude,  -- representative latitude for the sector
        AVG(longitude)                             AS rep_longitude, -- representative longitude for the sector
        ll_to_earth(AVG(latitude), AVG(longitude)) AS coordinates
    FROM binned_data
    GROUP BY "ownerId", sector_ts_bin_day, latitude_bin, longitude_bin
),

-- 5) Self-join the "sector-day" table within radius and within ±90 days
--    This is drastically smaller than joining every photo with every other photo.
sector_day_pairs AS (
    SELECT DISTINCT
        ca."ownerId",
        ca.latitude_bin,
        ca.longitude_bin,
        ca.sector_ts_bin_day AS sector_ts_bin_day,
        ph.sector_ts_bin_day AS photo_ts_bin_day
    FROM sector_day ca
         CROSS JOIN constants c
         JOIN sector_day ph
              ON ca."ownerId" = ph."ownerId"
                  AND ph.sector_ts_bin_day BETWEEN ca.sector_ts_bin_day - INTERVAL '90 DAYS' AND ca.sector_ts_bin_day + INTERVAL '90 DAYS'
                  AND ph.latitude_bin BETWEEN ca.latitude_bin - bin_delta AND ca.latitude_bin + bin_delta
                  AND ph.longitude_bin BETWEEN ca.longitude_bin - bin_delta AND ca.longitude_bin + bin_delta
                  AND earth_distance(ca.coordinates, ph.coordinates) <= radius_m
),

-- 6) Summarize "scores" at the sector-day level
--    E.g. sum of day differences, or distinct day counts, etc.
sector_day_scores AS (
    WITH
        sector_day_deltas AS (
            SELECT
                "ownerId",
                sector_ts_bin_day,
                latitude_bin,
                longitude_bin,
                EXTRACT(DAY FROM (photo_ts_bin_day - sector_ts_bin_day)) AS delta
            FROM sector_day_pairs
            UNION ALL
            SELECT
                "ownerId",
                sector_ts_bin_day,
                latitude_bin,
                longitude_bin,
                -90 AS delta -- left boundary
            FROM sector_day_pairs
            UNION ALL
            SELECT
                "ownerId",
                sector_ts_bin_day,
                latitude_bin,
                longitude_bin,
                +90 AS delta -- right boundary
            FROM sector_day_pairs
        ),
        sector_day_prev_deltas AS (
            SELECT
                "ownerId",
                sector_ts_bin_day,
                latitude_bin,
                longitude_bin,
                delta,
                LAG(delta) OVER (PARTITION BY "ownerId", sector_ts_bin_day, latitude_bin, longitude_bin ORDER BY delta) AS prev_delta
            FROM sector_day_deltas
        ),
        metrics AS (
            SELECT
                "ownerId",
                sector_ts_bin_day,
                latitude_bin,
                longitude_bin,

                MAX(delta - prev_delta) AS metric_max_gap,
                COUNT(delta)            AS metric_days_visited
            FROM sector_day_prev_deltas
            GROUP BY "ownerId", sector_ts_bin_day, latitude_bin, longitude_bin
        )
    SELECT
        "ownerId",
        sector_ts_bin_day,
        latitude_bin,
        longitude_bin,
        - metric_max_gap AS score,
        metric_max_gap,
        metric_days_visited
    FROM metrics
),

-- 7) Compute a final "score" for each (owner, sector, month)
final AS (
    WITH
        range AS (
            SELECT
                MIN(sector_ts_bin_day) AS data_start,
                MAX(sector_ts_bin_day) AS data_end
            FROM binned_data
        ),
        monthly_best AS (
            SELECT DISTINCT ON ("ownerId", sector_ts_bin_month)
                "ownerId",
                sector_ts_bin_day,
                DATE_TRUNC('month', sector_ts_bin_day) AS sector_ts_bin_month,
                rep_latitude                           AS latitude,
                rep_longitude                          AS longitude,
                score,
                metric_max_gap,
                metric_days_visited
            FROM sector_day_scores
                 JOIN sector_day USING ("ownerId", sector_ts_bin_day, latitude_bin, longitude_bin)
            ORDER BY "ownerId", sector_ts_bin_month, score DESC, metric_days_visited DESC, rep_latitude, rep_longitude, sector_ts_bin_day
        ),
        monthly_best_extended_window AS (
            SELECT DISTINCT ON ("ownerId", ts_bin_month)
                "ownerId",
                DATE_TRUNC('month', sector_ts_bin_month + (delta * INTERVAL '1 day')) AS ts_bin_month,
                sector_ts_bin_day,
                latitude,
                longitude,
                score,
                metric_max_gap,
                metric_days_visited
            FROM monthly_best
                 CROSS JOIN GENERATE_SERIES(-65, 95) delta -- previous 3 and next 3 months (asymmetric!)
                 JOIN range ON sector_ts_bin_day + (delta * INTERVAL '1 day') BETWEEN data_start AND data_end
            ORDER BY "ownerId", ts_bin_month, score DESC, metric_days_visited DESC, latitude, longitude, sector_ts_bin_day
        )
    SELECT
        "ownerId",
        MIN(ts_bin_month)                                                AS start_day,
        MAX(DATE_TRUNC('month', ts_bin_month + (32 * INTERVAL '1 day'))) AS end_day,
        sector_ts_bin_day,
        latitude,
        longitude,
        score,
        metric_max_gap,
        metric_days_visited
    FROM monthly_best_extended_window
    GROUP BY "ownerId", sector_ts_bin_day, latitude, longitude, score, metric_max_gap, metric_days_visited
)
SELECT
    "ownerId",
    start_day,
    end_day,
    sector_ts_bin_day,
    latitude,
    longitude,
    score,
    metric_max_gap,
    metric_days_visited
FROM final
ORDER BY start_day;
`

const asset_photo_classification_view = `
CREATE OR REPLACE VIEW asset_photo_classification AS
WITH
    constants AS (
        SELECT
            100000::float8 AS home_radius_meters -- 100 km in meters
    ),
    home_coords AS (
        SELECT
            "ownerId",
            ll_to_earth(latitude, longitude) AS coordinates,
            start_day                        AS start,
            end_day                          AS end
        FROM asset_home_detection
    ),
    enriched_assets AS (
        SELECT
            a.id,
            a."ownerId",
            a."localDateTime" AS ts,
            e.latitude        AS exif_latitude,
            e.longitude       AS exif_longitude,
            CASE
                WHEN e.latitude IS NOT NULL AND e.longitude IS NOT NULL THEN ll_to_earth(e.latitude, e.longitude)
                END           AS coordinates
        FROM assets a
             LEFT JOIN exif e ON a.id = e."assetId"
        WHERE a."deletedAt" IS NULL
    ),
    min_distances AS (
        SELECT
            a.id,
            MIN(earth_distance(a.coordinates, h.coordinates)) AS min_distance_meters
        FROM enriched_assets a
             LEFT JOIN home_coords h ON a."ownerId" = h."ownerId" AND a.ts BETWEEN h.start AND h.end
        WHERE a.coordinates IS NOT NULL
        GROUP BY a.id
    )
SELECT
    d.id,
    d."ownerId",
    d.ts,
    d.exif_latitude,
    d.exif_longitude,
    d.coordinates,
    md.min_distance_meters AS distance_meters,
    CASE
        WHEN md.min_distance_meters IS NULL
            THEN 'unknown'
        WHEN md.min_distance_meters <= c.home_radius_meters
            THEN 'home'
        ELSE 'trip'
        END                AS classification
FROM enriched_assets d
     LEFT JOIN min_distances md ON d.id = md.id
     CROSS JOIN constants c;
     `

const asset_dbscan_view = `
-- First view: asset_dbscan
CREATE OR REPLACE VIEW asset_dbscan AS
WITH
    data AS (
        /* 1) Basic extraction from \`assets\` (rename columns, etc.) - only one representative per duplicate group*/
        SELECT DISTINCT ON (COALESCE(a."duplicateId", a.id), a."ownerId")
            a.id,
            a."ownerId",
            a."localDateTime" AS ts,
            c.classification,
            c.coordinates
        FROM assets a
             JOIN asset_photo_classification c ON a.id = c.id
        WHERE "deletedAt" IS NULL
        ORDER BY COALESCE(a."duplicateId", a.id),
                 a."ownerId",
                 a.id -- Stable, deterministic ordering within each group
    ),

/* 2) Count how many home/unknown rows are within ±24h => used to mark home/unknown core if >=10 */
/*    Among home/unknown core points, group them using consecutive gap <=24h */
    home_core_clusters AS (
        WITH
            time_points AS (
                SELECT
                    d.*,
                    COUNT(*) OVER (
                        PARTITION BY d."ownerId"
                        ORDER BY d.ts
                        RANGE BETWEEN INTERVAL '1 day' PRECEDING AND INTERVAL '1 day' FOLLOWING
                        ) AS neighbor_count
                FROM data d
                WHERE classification IN ('home', 'unknown')
            ),
            ordered_core AS (
                SELECT
                    tp.*,
                    LAG(tp.ts) OVER (PARTITION BY tp."ownerId" ORDER BY tp.ts) AS prev_ts
                FROM time_points tp
                WHERE tp.neighbor_count >= 10 -- MinPts=10
            )
        /* Summation of new_cluster_flag => unique core_cluster_id */
        SELECT
            oc.id,
            oc."ownerId",
            oc.ts,
            oc.classification,
            TRUE  AS is_core,
            SUM(CASE WHEN (oc.ts - oc.prev_ts) > INTERVAL '1 day' THEN 1 ELSE 0 END) OVER (
                PARTITION BY oc."ownerId"
                ORDER BY oc.ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS core_cluster_id
        FROM ordered_core oc
    ),

/* 3) Count how many trip rows are within ±7d => used to mark trip core if >=50 */
/*    Among trip core points, group them using consecutive gap <=7d */
    trip_core_clusters AS (
        WITH
            time_points AS (
                SELECT
                    d.*,
                    COUNT(*) OVER (
                        PARTITION BY d."ownerId"
                        ORDER BY d.ts
                        RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '7 days' FOLLOWING
                        ) AS neighbor_count
                FROM data d
                WHERE classification = 'trip'
            ),
            ordered_core AS (
                SELECT
                    tp.*,
                    LAG(tp.ts) OVER (PARTITION BY tp."ownerId" ORDER BY tp.ts)          AS prev_ts,
                    LAG(tp.coordinates) OVER (PARTITION BY tp."ownerId" ORDER BY tp.ts) AS prev_coord
                FROM time_points tp
                WHERE tp.neighbor_count >= 50 -- MinPts=50
            )
        /* Summation of new_cluster_flag => unique core_cluster_id */
        SELECT
            oc.id,
            oc."ownerId",
            oc.ts,
            oc.classification,
            TRUE  AS is_core,
            SUM(CASE
                    WHEN (oc.ts - oc.prev_ts) > INTERVAL '7 days' THEN 1
                    WHEN (earth_distance(oc.coordinates, oc.prev_coord)) > 500000 THEN 1 -- basic bridge detection
                    ELSE 0
                END) OVER (
                PARTITION BY oc."ownerId"
                ORDER BY oc.ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS core_cluster_id
        FROM ordered_core oc
    ),

    trip_core_cluster_stats AS (
        SELECT
            core_cluster_id,
            "ownerId",
            TRUE    AS is_core,
            MIN(ts) AS core_cluster_start,
            MAX(ts) AS core_cluster_end
        FROM trip_core_clusters
        GROUP BY core_cluster_id, "ownerId"
        HAVING COUNT(*) > 20
    ),

    assign_unknown_to_overlapping_trip AS (
        SELECT
            id,
            TRUE                   AS assigned_to_trip_core,
            MIN(s.core_cluster_id) AS trip_core_cluster_id
        FROM data d
             JOIN trip_core_cluster_stats s ON s."ownerId" = d."ownerId"
        WHERE classification = 'unknown'
          AND ts BETWEEN core_cluster_start AND core_cluster_end
        GROUP BY id
    ),

    all_points AS (
        /* Merge core info back to *all* points, so core rows have home/trip cluster_id, others = NULL */
        SELECT
            d.id,
            d."ownerId",
            d.ts,
            d.classification,
            hc.core_cluster_id                                                             AS home_core_cluster_id,
            COALESCE(hc.is_core, FALSE) AND NOT COALESCE(rcu.assigned_to_trip_core, FALSE) AS is_home_core,
            COALESCE(tcc.core_cluster_id, rcu.trip_core_cluster_id)                        AS trip_core_cluster_id,
            COALESCE(tcc.is_core, rcu.assigned_to_trip_core, FALSE)                        AS is_trip_core
        FROM data d
             LEFT JOIN home_core_clusters hc USING (id)
             LEFT JOIN trip_core_clusters tc USING (id)
             LEFT JOIN assign_unknown_to_overlapping_trip rcu USING (id)
             LEFT JOIN trip_core_cluster_stats tcc
                       ON tc.core_cluster_id = tcc.core_cluster_id AND tc."ownerId" = tcc."ownerId"
    ),

    rerank_cluster_id AS (
        SELECT
            id,
            "ownerId",
            ts,
            classification,
            CASE
                WHEN is_home_core THEN 2 * home_core_cluster_id
                WHEN is_trip_core THEN 2 * trip_core_cluster_id + 1
                ELSE -1
                END                      AS cluster_id,
            is_trip_core OR is_home_core AS is_core
        FROM all_points
    ),

/*
  6) For convenience, label each row as core/border/noise.
     - core => is_core = TRUE
     - noise => cluster_id = -1
     - border => not core AND not noise
*/
    labeled_points AS (
        SELECT
            cca.id,
            cca."ownerId",
            cca.ts,
            cca.cluster_id,
            (cca.cluster_id = -1)                      AS is_noise,
            cca.is_core,
            (NOT cca.is_core AND cca.cluster_id != -1) AS is_border
        FROM rerank_cluster_id cca
    )
SELECT *
FROM labeled_points;
`

const asset_dbscan_clusters_view = `
-- Second view: asset_dbscan_clusters
/*
     Gather cluster-level stats (start, end, cardinality, duration).
     We skip cluster_id = -1, because that's noise.
*/
CREATE OR REPLACE VIEW asset_dbscan_clusters AS
WITH
    cluster_data AS (
        SELECT
            ad.cluster_id,
            ad."ownerId",
            ad.ts,
            q.score,
            COALESCE(e.country, 'unknown') AS country
        FROM asset_dbscan ad
             LEFT JOIN exif e ON ad.id = e."assetId"
             LEFT JOIN quality_assessments q ON ad.id = q."assetId"
    ),
    normalized_scores AS (
        SELECT
            cluster_id,
            "ownerId",
            ts,
            score,
            country,
            CASE
                WHEN score IS NOT NULL AND STDDEV(score) OVER (PARTITION BY "ownerId") != 0
                    THEN (score - AVG(score) OVER (PARTITION BY "ownerId")) / STDDEV(score) OVER (PARTITION BY "ownerId")
                ELSE NULL
                END AS normalized_score
        FROM cluster_data
    ),
    cluster_stats AS (
        SELECT
            cluster_id,
            "ownerId",
            MIN(ts)                                       AS cluster_start,
            MAX(ts)                                       AS cluster_end,
            MAX(ts) - MIN(ts)                             AS cluster_duration,
            COUNT(*)                                      AS cluster_cardinality,
            COUNT(*) FILTER (WHERE normalized_score >= 0) AS cluster_cardinality_score_ge_0,
            COUNT(*) FILTER (WHERE normalized_score >= 1) AS cluster_cardinality_score_ge_1,
            (
                SELECT
                    JSON_AGG(
                            JSON_BUILD_OBJECT(
                                    'country', sub.country,
                                    'count', sub.country_count
                            )
                    )
                FROM (
                         SELECT
                             ns.country,
                             COUNT(*) AS country_count
                         FROM normalized_scores ns
                         WHERE ns.cluster_id = cs.cluster_id
                           AND ns."ownerId" = cs."ownerId"
                         GROUP BY ns.country
                     ) sub
            )                                             AS countries
        FROM normalized_scores cs
        WHERE cluster_id != -1
        GROUP BY cluster_id, "ownerId"
        ORDER BY "ownerId", cluster_id -- Modified ordering
    )
SELECT *
FROM cluster_stats;
`

const asset_analysis_materialised_view = `
-- Third view: asset_analysis
CREATE MATERIALIZED VIEW asset_analysis AS
WITH final as (
    SELECT
        lp.id,
        lp."ownerId",
        lp.ts,
        lp.cluster_id,
        COALESCE(cs.cluster_cardinality, 0) AS cluster_cardinality,
        COALESCE(cs.cluster_cardinality_score_ge_0, 0) AS cluster_cardinality_score_ge_0,
        COALESCE(cs.cluster_cardinality_score_ge_1, 0) AS cluster_cardinality_score_ge_1,
        COALESCE(cs.cluster_start, lp.ts) AS cluster_start,
        COALESCE(cs.cluster_end, lp.ts) AS cluster_end,
        COALESCE(cs.cluster_duration, INTERVAL '0') AS cluster_duration,
        lp.is_core,
        lp.is_noise,
        lp.is_border,
        CASE WHEN lp.is_noise THEN 'noise' ELSE 'cluster' END AS label,

        /* Add location data from exif */
        e.city,
        e.state,
        e.country,

        /* Add quality scores - both raw and normalized using window functions */
        q.score as quality_score,
        CASE
            WHEN q.score IS NOT NULL AND STDDEV(q.score) OVER (PARTITION BY lp."ownerId") != 0  -- Added partition
            THEN (q.score - AVG(q.score) OVER (PARTITION BY lp."ownerId")) / STDDEV(q.score) OVER (PARTITION BY lp."ownerId")
            ELSE NULL
        END as normalized_quality_score,

        /* Time-based density metrics including new 5min window */
        COUNT(*) FILTER (WHERE TRUE) OVER (
            PARTITION BY lp."ownerId"  -- Added partition
            ORDER BY lp.ts
            RANGE BETWEEN INTERVAL '5 minutes' PRECEDING AND INTERVAL '5 minutes' FOLLOWING
        ) AS neighbors_5m,

        COUNT(*) FILTER (WHERE TRUE) OVER (
            PARTITION BY lp."ownerId"  -- Added partition
            ORDER BY lp.ts
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND INTERVAL '1 hour' FOLLOWING
        ) AS neighbors_1h,

        COUNT(*) FILTER (WHERE TRUE) OVER (
            PARTITION BY lp."ownerId"  -- Added partition
            ORDER BY lp.ts
            RANGE BETWEEN INTERVAL '1 day' PRECEDING AND INTERVAL '1 day' FOLLOWING
        ) AS neighbors_1d,

        COUNT(*) FILTER (WHERE TRUE) OVER (
            PARTITION BY lp."ownerId"  -- Added partition
            ORDER BY lp.ts
            RANGE BETWEEN INTERVAL '7 days' PRECEDING AND INTERVAL '7 days' FOLLOWING
        ) AS neighbors_7d,

        COUNT(*) FILTER (WHERE TRUE) OVER (
            PARTITION BY lp."ownerId"  -- Added partition
            ORDER BY lp.ts
            RANGE BETWEEN INTERVAL '30 days' PRECEDING AND INTERVAL '30 days' FOLLOWING
        ) AS neighbors_30d,

        COUNT(*) FILTER (WHERE TRUE) OVER (
            PARTITION BY lp."ownerId"  -- Added partition
            ORDER BY lp.ts
            RANGE BETWEEN INTERVAL '180 days' PRECEDING AND INTERVAL '180 days' FOLLOWING
        ) AS neighbors_180d

    FROM asset_dbscan lp
    LEFT JOIN asset_dbscan_clusters cs ON lp.cluster_id = cs.cluster_id
    LEFT JOIN exif e ON lp.id = e."assetId"
    LEFT JOIN quality_assessments q ON lp.id = q."assetId"
)
SELECT * FROM final ORDER BY "ownerId", ts, id;  -- Modified ordering
`

export class MemorylaneMaterialisedViews1735468387695 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(asset_home_detection_view);
        await queryRunner.query(asset_photo_classification_view);
        await queryRunner.query(asset_dbscan_view);
        await queryRunner.query(asset_dbscan_clusters_view);
        await queryRunner.query(asset_analysis_materialised_view);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query("DROP MATERIALIZED VIEW IF EXISTS asset_analysis;");
        await queryRunner.query("DROP VIEW IF EXISTS asset_dbscan_clusters;");
        await queryRunner.query("DROP VIEW IF EXISTS asset_dbscan;");
        await queryRunner.query("DROP VIEW IF EXISTS asset_photo_classification;");
        await queryRunner.query("DROP VIEW IF EXISTS asset_home_detection;");
    }
}
