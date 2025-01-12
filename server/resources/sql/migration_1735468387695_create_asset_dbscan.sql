CREATE MATERIALIZED VIEW asset_dbscan AS
WITH
    data AS (
        SELECT DISTINCT ON (COALESCE(a."duplicateId", a.id), a."ownerId")
            a.id,
            a."ownerId",
            a."localDateTime" AS ts,
            c.classification,
            c.coordinates
        FROM assets a
             JOIN asset_photo_classification c ON a.id = c.id
        WHERE
            "deletedAt" IS NULL
        ORDER BY COALESCE(a."duplicateId", a.id),
                 a."ownerId",
                 a.id
    ),

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
                WHERE
                    classification IN ('home', 'unknown')
            ),
            ordered_core AS (
                SELECT
                    tp.*,
                    LAG(tp.ts) OVER (PARTITION BY tp."ownerId" ORDER BY tp.ts) AS prev_ts
                FROM time_points tp
                WHERE
                    tp.neighbor_count >= 10 -- MinPts=10
            )
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
                WHERE
                    classification = 'trip'
            ),
            ordered_core AS (
                SELECT
                    tp.*,
                    LAG(tp.ts) OVER (PARTITION BY tp."ownerId" ORDER BY tp.ts)          AS prev_ts,
                    LAG(tp.coordinates) OVER (PARTITION BY tp."ownerId" ORDER BY tp.ts) AS prev_coord
                FROM time_points tp
                WHERE
                    tp.neighbor_count >= 50 -- MinPts=50
            )
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
        HAVING
            COUNT(*) > 20
    ),

    assign_unknown_to_overlapping_trip AS (
        SELECT
            id,
            TRUE                   AS assigned_to_trip_core,
            MIN(s.core_cluster_id) AS trip_core_cluster_id
        FROM data d
             JOIN trip_core_cluster_stats s ON s."ownerId" = d."ownerId"
        WHERE
              classification = 'unknown'
          AND ts BETWEEN core_cluster_start AND core_cluster_end
        GROUP BY id
    ),

    all_points AS (
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
