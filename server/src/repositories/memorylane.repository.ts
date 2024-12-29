import { Inject, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { AssetEntity } from 'src/entities/asset.entity';
import { ILoggerRepository } from 'src/interfaces/logger.interface';
import { IMemorylaneRepository } from 'src/interfaces/memorylane.interface';
import { Repository } from 'typeorm';

interface Memorylane {
  title: string;
  assetIds: string[];
}

@Injectable()
export class MemorylaneRepository implements IMemorylaneRepository {
  constructor(
    @InjectRepository(AssetEntity) private assetRepository: Repository<AssetEntity>,
    @Inject(ILoggerRepository) private logger: ILoggerRepository,
  ) {
    this.logger.setContext(MemorylaneRepository.name);
  }

  async refresh(): Promise<void> {
    await this.assetRepository.query('REFRESH MATERIALIZED VIEW asset_dbscan');
    await this.assetRepository.query('REFRESH MATERIALIZED VIEW asset_dbscan_clusters');
    await this.assetRepository.query('REFRESH MATERIALIZED VIEW asset_analysis');
  }

  async recentHighlight(userIds: string[], seed: number, limit: number): Promise<Memorylane> {
    const result = await this.assetRepository.query(recentHighlightsQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    const title = 'Recent highlights';
    return { title, assetIds };
  }

  async cluster(userIds: string[], seed: number, limit: number): Promise<Memorylane> {
    const result = await this.assetRepository.query(clusterQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    let title = 'Cluster';

    if (result && result.length > 0) {
      const { cluster_location_stats: locationStats } = result[0];
      if (locationStats?.cities) {
        // Sort cities by percentage and get top 3
        const topCities = Object.entries(locationStats.cities)
          .sort(([, a], [, b]) => (b as number) - (a as number))
          .slice(0, 3)
          .map(([city]) => city);

        // Join cities with proper grammar
        title =
          topCities.length > 1
            ? `${topCities.slice(0, -1).join(', ')} and ${topCities.slice(-1)}`
            : topCities[0] || 'Cluster';
      }
    }

    return { title, assetIds };
  }
}

const clusterQuery = `
    WITH CONSTANTS AS (SELECT $1::bigint            AS LCG_SEED,
                              $2::int               AS RESULT_LIMIT,
                              $3::uuid[]            AS USER_IDS,
                              INTERVAL '15 minutes' AS MIN_TIME_BETWEEN_PHOTOS),

/* ---------------------------------------------------------------------------
   1) Randomly pick exactly ONE cluster.
      We do this by giving each cluster a weight = 1, summing them, then doing
      one random draw. 
--------------------------------------------------------------------------- */
         cluster_data AS (SELECT c.final_cluster_id                     AS cluster_id,
                                 sqrt(c.cluster_cardinality_score_ge_0) AS weight
                          FROM asset_dbscan_clusters c
                                   CROSS JOIN CONSTANTS co
                          -- Optional: filter out uninteresting clusters if desired
                          WHERE c.cluster_cardinality_score_ge_0 >= co.RESULT_LIMIT
                                AND c."ownerId" = ANY(co.USER_IDS)
                          ),
         cluster_w AS (
             /* Compute total_weight, plus a running sum (right_cumulative). */
             SELECT cd.cluster_id,
                    cd.weight,
                    SUM(cd.weight) OVER ()                       AS total_weight,
                    SUM(cd.weight) OVER (ORDER BY cd.cluster_id) AS right_cumulative
             FROM cluster_data cd),
         cluster_w2 AS (
             /* Define left_cumulative using LAG(...) */
             SELECT cw.cluster_id,
                    cw.weight,
                    cw.total_weight,
                    cw.right_cumulative,
                    COALESCE(
                                    LAG(cw.right_cumulative) OVER (ORDER BY cw.cluster_id),
                                    0
                    ) AS left_cumulative
             FROM cluster_w cw),
         cluster_rng AS (
             /* A single random draw via LCG, seeded by LCG_SEED. */
             WITH RECURSIVE seed_seq(i, seed) AS (SELECT 1, (c.LCG_SEED + 2147483648::bigint) % 2147483648
                                                  FROM CONSTANTS c
                                                  UNION ALL
                                                  SELECT i + 1,
                                                         (1103515245 * seed + 12345) % 2147483648
                                                  FROM seed_seq
                                                           CROSS JOIN CONSTANTS c
                                                  WHERE i < 1)
             SELECT seed_seq.seed::float / 2147483648 AS r
             FROM seed_seq
             WHERE i = 1),
         chosen_cluster AS (
             /*
                Pick the single cluster whose interval covers r * total_weight.
                This always returns exactly one row.
             */
             SELECT cw2.cluster_id
             FROM cluster_w2 cw2
                      CROSS JOIN cluster_rng cr
             WHERE (cr.r * cw2.total_weight) >= cw2.left_cumulative
               AND (cr.r * cw2.total_weight) < cw2.right_cumulative
             LIMIT 1),

/* ---------------------------------------------------------------------------
   2) From that chosen cluster, select random photos:
      - Only photos with quality >= 0
      - Weight = 1 + quality_score
      - Enforce 15min min separation
      - Return up to 12 total
      - Sort final results by timestamp
--------------------------------------------------------------------------- */
         data AS (
             /* Pull assets in the chosen cluster and define the new weight. */
             SELECT ad.id,
                    ad.ts,
                    (1 + COALESCE(ad.quality_score, 0)) AS weight
             FROM asset_analysis ad
                      JOIN chosen_cluster cc ON ad.cluster_id = cc.cluster_id
             WHERE ad.normalized_quality_score >= 0),
         w AS (
             /* Compute total_weight across these photos, plus their running total. */
             SELECT d.id,
                    d.ts,
                    d.weight,
                    SUM(d.weight) OVER ()              AS total_weight,
                    SUM(d.weight) OVER (ORDER BY d.ts) AS right_cumulative
             FROM data d),
         w2 AS (
             /* LAG(...) to define each row's [left_cumulative, right_cumulative). */
             SELECT w.id,
                    w.ts,
                    w.weight,
                    w.total_weight,
                    w.right_cumulative,
                    COALESCE(LAG(w.right_cumulative) OVER (ORDER BY w.ts), 0) AS left_cumulative
             FROM w),
         rng AS (
             /* Generate (2 * RESULT_LIMIT) random draws via the same LCG. */
             WITH RECURSIVE seed_seq(i, seed) AS (SELECT 1, (c.LCG_SEED + 2147483648::bigint) % 2147483648
                                                  FROM CONSTANTS c
                                                  UNION ALL
                                                  SELECT i + 1,
                                                         (1103515245 * seed + 12345) % 2147483648
                                                  FROM seed_seq
                                                           CROSS JOIN CONSTANTS c
                                                  WHERE i < 2 * c.RESULT_LIMIT)
             SELECT i                                 AS draw_number,
                    seed_seq.seed::float / 2147483648 AS r
             FROM seed_seq),
         candidates AS (
             /*
                For each random draw, find the photo whose [left_cumulative, right_cumulative)
                covers r * total_weight. Order them by draw_number so we can filter by
                “first come, first served” below.
             */
             SELECT rng.draw_number,
                    w2.id,
                    w2.ts
             FROM rng
                      JOIN w2
                           ON (rng.r * w2.total_weight) >= w2.left_cumulative
                               AND (rng.r * w2.total_weight) < w2.right_cumulative
             ORDER BY rng.draw_number),
         filtered_candidates AS (
             /*
                Discard any photo if it’s within 15 minutes of an already-chosen photo.
                Then keep only the first RESULT_LIMIT picks.
             */
             SELECT a.draw_number,
                    ROW_NUMBER() OVER (ORDER BY a.draw_number) AS row_number,
                    a.id,
                    a.ts
             FROM candidates a
                      CROSS JOIN CONSTANTS c
             WHERE NOT EXISTS (SELECT 1
                               FROM candidates b
                               WHERE b.draw_number < a.draw_number
                                 AND a.ts - b.ts < c.MIN_TIME_BETWEEN_PHOTOS -- Minimum spacing check
                                 AND b.ts - a.ts < c.MIN_TIME_BETWEEN_PHOTOS -- Minimum spacing check
             )
             ORDER BY a.draw_number),

         location_stats AS (
             /* Get all assets in the selected cluster, before quality filtering */
             SELECT e.city,
                    e.state,
                    e.country
             FROM filtered_candidates ad -- asset_analysis ad
                      --JOIN chosen_cluster cc ON ad.cluster_id = cc.cluster_id
                      LEFT JOIN exif e ON ad.id = e."assetId"),
         location_counts AS (WITH total_stats AS (SELECT COUNT(*) as total_count
                                                  FROM location_stats)
                             SELECT total_stats.total_count AS total_assets,
                                    jsonb_build_object(
                                            'cities',
                                            (SELECT jsonb_object_agg(city, (count::decimal / total_stats.total_count))
                                             FROM (SELECT city, COUNT(*) as count
                                                   FROM location_stats
                                                   WHERE city IS NOT NULL
                                                   GROUP BY city) cities),
                                            'states',
                                            (SELECT jsonb_object_agg(state, (count::decimal / total_stats.total_count))
                                             FROM (SELECT state, COUNT(*) as count
                                                   FROM location_stats
                                                   WHERE state IS NOT NULL
                                                   GROUP BY state) states),
                                            'countries', (SELECT jsonb_object_agg(country,
                                                                                  (count::decimal / total_stats.total_count))
                                                          FROM (SELECT country, COUNT(*) as count
                                                                FROM location_stats
                                                                WHERE country IS NOT NULL
                                                                GROUP BY country) countries)
                                    )                       AS location_distribution
                             FROM total_stats)

/* ---------------------------------------------------------------------------
   Final selection: up to 12 photos, sorted by time
--------------------------------------------------------------------------- */
    SELECT fc.id,
           lc.location_distribution as cluster_location_stats
    FROM filtered_candidates fc
             CROSS JOIN CONSTANTS c
             CROSS JOIN location_counts lc
    WHERE fc.row_number <= c.RESULT_LIMIT
    ORDER BY fc.ts
`;

const recentHighlightsQuery = `
    WITH CONSTANTS AS (SELECT INTERVAL '3 months' as LOOKBACK_WINDOW,
                              INTERVAL '6 HOURS'  as MIN_TIME_BETWEEN_HIGHLIGHTS,
                              1.0                 as MIN_QUALITY_SCORE,
                              $1::bigint          as LCG_SEED,
                              $2::int             as RESULT_LIMIT,
                              $3::uuid[]          as USER_IDS),
         data AS (
             /* 1) Pull relevant rows; rename score -> weight. */
             SELECT ad.id,
                    ad.ts                       as ts,
                    ad.normalized_quality_score AS weight
             FROM asset_analysis AS ad
                      CROSS JOIN CONSTANTS c
             WHERE ad.ts >= CURRENT_TIMESTAMP - c.LOOKBACK_WINDOW
               AND ad.normalized_quality_score >= c.MIN_QUALITY_SCORE
               AND ad."ownerId" = ANY(c.USER_IDS)),
         w AS (
             /* 2) Compute total_weight and right_cumulative. */
             SELECT d.id,
                    d.ts,
                    d.weight,
                 /* total_weight is same for every row. */
                    SUM(d.weight) OVER ()              AS total_weight,
                 /* sum(...) OVER (ORDER BY ts) is the running total, i.e. "right boundary". */
                    SUM(d.weight) OVER (ORDER BY d.ts) AS right_cumulative
             FROM data d),
         w2 AS (
             /* 3) Use LAG(...) to get "left boundary" from the previous row's right_cumulative. */
             SELECT w.id,
                    w.ts,
                    w.weight,
                    w.total_weight,
                    w.right_cumulative,
                    COALESCE(
                                    LAG(w.right_cumulative) OVER (ORDER BY w.ts),
                                    0
                    ) AS left_cumulative
             FROM w),
         rng AS (
             /* 4) Generate 2 * RESULT_LIMIT draws via a recursive LCG seeded by SEED (32-bit int). */
             WITH RECURSIVE seed_seq(i, seed) AS (SELECT 1, (c.LCG_SEED + 2147483648::bigint) % 2147483648
                                                  FROM CONSTANTS c
                                                  UNION ALL
                                                  SELECT i + 1,
                                                         (1103515245 * seed + 12345) % 2147483648
                                                  FROM seed_seq
                                                           CROSS JOIN CONSTANTS c
                                                  WHERE i < 2 * c.RESULT_LIMIT)
             SELECT i                                 AS draw_number,
                 /* Convert seed -> float in [0,1). */
                    seed_seq.seed::float / 2147483648 AS r
             FROM seed_seq),
         candidates as (SELECT rng.draw_number,
                               w2.id,
                               w2.ts,
                               w2.weight
                        FROM rng
                                 JOIN w2
                            /* 5) Interval check: find row whose [left_cumulative, right_cumulative) contains r * total_weight. */
                                      ON (rng.r * w2.total_weight) >= w2.left_cumulative
                                          AND (rng.r * w2.total_weight) < w2.right_cumulative
                        ORDER BY rng.draw_number),
         filtered_candidates AS (SELECT draw_number,
                                        ROW_NUMBER() OVER (ORDER BY draw_number) as row_number,
                                        id,
                                        ts,
                                        weight
                                 FROM candidates a
                                          CROSS JOIN CONSTANTS c
                                 WHERE NOT EXISTS (
                                     -- Look for ANY previously selected point that's too close
                                     SELECT 1
                                     FROM candidates b
                                     WHERE b.draw_number < a.draw_number               -- Only check previous points
                                       AND a.ts - b.ts < c.MIN_TIME_BETWEEN_HIGHLIGHTS -- Minimum spacing check
                                       AND b.ts - a.ts < c.MIN_TIME_BETWEEN_HIGHLIGHTS -- Minimum spacing check
                                 )
                                 ORDER BY draw_number)
    SELECT id
    FROM filtered_candidates
             CROSS JOIN CONSTANTS c
    WHERE row_number <= c.RESULT_LIMIT
    ORDER BY draw_number
`;
