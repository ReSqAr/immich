import {MigrationInterface, QueryRunner} from "typeorm";


const asset_dbscan_materialised_view = `
-- First view: asset_dbscan
CREATE MATERIALIZED VIEW asset_dbscan AS
WITH data AS (
    /* 1) Basic extraction from \`assets\` (rename columns, etc.) - only one representative per duplicate group*/
    SELECT DISTINCT ON (COALESCE("duplicateId", id), "ownerId")
        id,
        "ownerId",
        "localDateTime" as ts
    FROM assets
    WHERE "deletedAt" IS NULL
    ORDER BY
        COALESCE("duplicateId", id),
        "ownerId",
        id  -- Stable, deterministic ordering within each group
),

time_points AS (
    /* 2) Count how many rows are within ±24h => used to mark core if >=MinPts */
    SELECT
        d.id,
        d."ownerId",
        d.ts,
        COUNT(*) OVER (
            PARTITION BY d."ownerId"  -- Added partition
            ORDER BY d.ts
            RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND INTERVAL '24 hours' FOLLOWING
        ) AS neighbor_count
    FROM data d
),

core_points AS (
    SELECT
        tp.id,
        tp."ownerId",
        tp.ts,
        tp.neighbor_count,
        (tp.neighbor_count >= 10) AS is_core  -- MinPts=10
    FROM time_points tp
),

/* 3) Among core points, group them using consecutive gap <=24h */
core_clusters AS (
    WITH ordered_core AS (
        SELECT
            cp.id,
            cp."ownerId",
            cp.ts,
            LAG(cp.ts) OVER (
                PARTITION BY cp."ownerId"  -- Added partition
                ORDER BY cp.ts
            ) AS prev_ts
        FROM core_points cp
        WHERE cp.is_core = TRUE
    )
    SELECT
        oc.id,
        oc."ownerId",
        oc.ts,
        CASE
            WHEN (oc.ts - oc.prev_ts) > INTERVAL '24 hours' THEN 1
            ELSE 0
        END AS new_cluster_flag
    FROM ordered_core oc
),

assigned_core_clusters AS (
    /* Summation of new_cluster_flag => unique core_cluster_id */
    SELECT
        cc.id,
        cc."ownerId",
        cc.ts,
        SUM(cc.new_cluster_flag) OVER (
            PARTITION BY cc."ownerId"  -- Added partition
            ORDER BY cc.ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS core_cluster_id
    FROM core_clusters cc
),

all_points AS MATERIALIZED (
    /* Merge core info back to *all* points, so core rows have cluster_id, others = NULL */
    SELECT
        cp.id,
        cp."ownerId",
        cp.ts,
        cp.is_core,
        ac.core_cluster_id
    FROM core_points cp
    LEFT JOIN assigned_core_clusters ac USING (id)
),

extended_window AS (
    /*
       4) For each row, gather:
          - earliest_cluster_id (the "most recent" core cluster within 24h behind)
          - earliest_core_ts     (the ts of that cluster)
          - next_cluster_id      (the "soonest" core cluster within 24h ahead)
          - next_core_ts         (the ts of that cluster)

       We use CASE WHEN ap.core_cluster_id IS NOT NULL to only consider core rows
       in those window aggregates.
    */
    SELECT
        ap.*,

        -- earliest cluster behind me in time
        MAX(
          CASE WHEN ap.core_cluster_id IS NOT NULL THEN ap.core_cluster_id END
        ) OVER (
          PARTITION BY ap."ownerId"  -- Added partition
          ORDER BY ap.ts
          RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
        ) AS earliest_cluster_id,

        MAX(
          CASE WHEN ap.core_cluster_id IS NOT NULL THEN ap.ts END
        ) OVER (
          PARTITION BY ap."ownerId"  -- Added partition
          ORDER BY ap.ts
          RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
        ) AS earliest_core_ts,

        -- next cluster ahead of me in time
        MIN(
          CASE WHEN ap.core_cluster_id IS NOT NULL THEN ap.core_cluster_id END
        ) OVER (
          PARTITION BY ap."ownerId"  -- Added partition
          ORDER BY ap.ts
          RANGE BETWEEN CURRENT ROW AND INTERVAL '24 hours' FOLLOWING
        ) AS next_cluster_id,

        MIN(
          CASE WHEN ap.core_cluster_id IS NOT NULL THEN ap.ts END
        ) OVER (
          PARTITION BY ap."ownerId"  -- Added partition
          ORDER BY ap.ts
          RANGE BETWEEN CURRENT ROW AND INTERVAL '24 hours' FOLLOWING
        ) AS next_core_ts

    FROM all_points ap
),

closest_cluster_assignment AS (
    /*
      5) Final step:
         - If is_core => keep your cluster_id
         - Else => pick whichever cluster is physically closer in time
                   among earliest_cluster_id vs. next_cluster_id
    */
    SELECT
        ew.id,
        ew."ownerId",
        ew.ts,
        ew.is_core,

        CASE
          WHEN ew.is_core THEN
            /* keep your own cluster ID */
            ew.core_cluster_id
          ELSE
            /* border or noise => check earliest & next */
            CASE
              WHEN ew.earliest_cluster_id IS NULL
                   AND ew.next_cluster_id IS NULL THEN
                -1  -- no cluster behind or ahead => noise

              WHEN ew.earliest_cluster_id IS NOT NULL
                   AND ew.next_cluster_id IS NULL THEN
                /* only an earlier cluster => pick that */
                ew.earliest_cluster_id

              WHEN ew.earliest_cluster_id IS NULL
                   AND ew.next_cluster_id IS NOT NULL THEN
                /* only a later cluster => pick that */
                ew.next_cluster_id

              ELSE
                /* Both exist => pick whichever is physically closer */
                CASE
                  WHEN ABS(EXTRACT(EPOCH FROM (ew.ts - ew.earliest_core_ts)))
                     < ABS(EXTRACT(EPOCH FROM (ew.next_core_ts - ew.ts)))
                  THEN ew.earliest_cluster_id

                  WHEN ABS(EXTRACT(EPOCH FROM (ew.ts - ew.earliest_core_ts)))
                     > ABS(EXTRACT(EPOCH FROM (ew.next_core_ts - ew.ts)))
                  THEN ew.next_cluster_id

                  ELSE
                    /*
                      Tie => pick earliest or next at your discretion.
                      We'll pick earliest. (Standard DBSCAN doesn't mind either.)
                    */
                    ew.earliest_cluster_id
                END
            END
          END
        AS final_cluster_id
    FROM extended_window ew
),

/*
  6) For convenience, label each row as core/border/noise.
     - core => is_core = TRUE
     - noise => final_cluster_id = -1
     - border => not core AND not noise
*/
labeled_points AS (
    SELECT
        cca.id,
        cca."ownerId",
        cca.ts,
        cca.final_cluster_id,
        (cca.final_cluster_id = -1) AS is_noise,
        cca.is_core,
        (NOT cca.is_core AND cca.final_cluster_id != -1) AS is_border
    FROM closest_cluster_assignment cca
)
SELECT * from labeled_points;
`

const asset_dbscan_clusters_materialised_view = `
-- Second view: asset_dbscan_clusters
/*
     Gather cluster-level stats (start, end, cardinality, duration).
     We skip final_cluster_id = -1, because that's noise.
*/
CREATE MATERIALIZED VIEW asset_dbscan_clusters AS
WITH cluster_data AS (
    SELECT
        ad.final_cluster_id,
        ad."ownerId",
        ad.ts,
        q.score
    FROM asset_dbscan ad
    LEFT JOIN quality_assessment q ON ad.id = q."assetId"
),
normalized_scores AS (
    SELECT
        final_cluster_id,
        "ownerId",
        ts,
        score,
        CASE
            WHEN score IS NOT NULL AND STDDEV(score) OVER (PARTITION BY "ownerId") != 0  -- Added partition
            THEN (score - AVG(score) OVER (PARTITION BY "ownerId")) / STDDEV(score) OVER (PARTITION BY "ownerId")
            ELSE NULL
        END as normalized_score
    FROM cluster_data
),
cluster_stats AS (
    SELECT
        final_cluster_id,
        "ownerId",
        MIN(ts) AS cluster_start,
        MAX(ts) AS cluster_end,
        MAX(ts) - MIN(ts) AS cluster_duration,
        COUNT(*) AS cluster_cardinality,
        COUNT(*) FILTER (WHERE normalized_score >= 0) as cluster_cardinality_score_ge_0,
        COUNT(*) FILTER (WHERE normalized_score >= 1) as cluster_cardinality_score_ge_1
    FROM normalized_scores
    WHERE final_cluster_id != -1
    GROUP BY final_cluster_id, "ownerId"
    ORDER BY "ownerId", final_cluster_id  -- Modified ordering
)
SELECT * FROM cluster_stats;
`

const asset_analysis_materialised_view = `
-- Third view: asset_analysis
CREATE MATERIALIZED VIEW asset_analysis AS
WITH final as (
    SELECT
        lp.id,
        lp."ownerId",
        lp.ts,
        lp.final_cluster_id AS cluster_id,
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
    LEFT JOIN asset_dbscan_clusters cs ON lp.final_cluster_id = cs.final_cluster_id
    LEFT JOIN exif e ON lp.id = e."assetId"
    LEFT JOIN quality_assessment q ON lp.id = q."assetId"
)
SELECT * FROM final ORDER BY "ownerId", ts, id;  -- Modified ordering
`

export class MemorylaneMaterialisedViews1735468387695 implements MigrationInterface {

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(asset_dbscan_materialised_view);
        await queryRunner.query(asset_dbscan_clusters_materialised_view);
        await queryRunner.query(asset_analysis_materialised_view);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query("DROP MATERIALIZED VIEW IF EXISTS asset_analysis;");
        await queryRunner.query("DROP MATERIALIZED VIEW IF EXISTS asset_dbscan_clusters;");
        await queryRunner.query("DROP MATERIALIZED VIEW IF EXISTS asset_dbscan;");
    }
}
