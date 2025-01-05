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

type LocationStatistics = {
  cities?: Record<string, number>;
  states?: Record<string, number>;
  countries?: Record<string, number>;
};

type ClusterMetadata = {
  locationStats: LocationStatistics;
  startDate: Date;
  endDate: Date;
};

function isLocationScattered(locations: Record<string, number>, threshold = 0.05, topN = 2): boolean {
  if (!locations || Object.keys(locations).length === 0) {
    return true;
  }

  const frequencies = Object.values(locations).sort((a, b) => b - a);
  const remainder = frequencies.slice(topN).reduce((sum, freq) => sum + freq, 0);
  return remainder > threshold;
}

function getTopLocations(locations: Record<string, number>, threshold = 0.05, topN = 2): string[] {
  return Object.entries(locations)
    .filter(([location, freq]) => location && location.trim() && freq >= threshold)
    .sort(([, freqA], [, freqB]) => freqB - freqA)
    .slice(0, topN)
    .map(([location]) => location);
}

function formatLocationList(locations: string[]): string {
  if (locations.length === 0) {
    return '';
  }
  if (locations.length === 1) {
    return locations[0];
  }
  return `${locations.slice(0, -1).join(', ')} and ${locations.slice(-1)}`;
}

function formatDateRange(startDate: Date, endDate: Date): string {
  const start = new Date(startDate);
  const end = new Date(endDate);

  // Same day
  if (start.toDateString() === end.toDateString()) {
    return start.toLocaleDateString(undefined, { month: 'long', day: 'numeric', year: 'numeric' });
  }

  // Same month and year
  if (start.getMonth() === end.getMonth() && start.getFullYear() === end.getFullYear()) {
    return `${start.toLocaleDateString(undefined, { month: 'long' })} ${start.getDate()}-${end.getDate()}, ${start.getFullYear()}`;
  }

  // Same year
  if (start.getFullYear() === end.getFullYear()) {
    return `${start.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })} - ${end.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}, ${start.getFullYear()}`;
  }

  // Different years
  return `${start.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })} - ${end.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })}`;
}

function generateClusterTitle(metadata: ClusterMetadata): string {
  const { locationStats, startDate, endDate } = metadata;

  let location = '';

  // Handle location part
  if (locationStats?.cities && !isLocationScattered(locationStats.cities)) {
    const topCities = getTopLocations(locationStats.cities);
    if (topCities.length > 0) {
      location = formatLocationList(topCities);
    }
  } else if (locationStats?.states && !isLocationScattered(locationStats.states)) {
    const topStates = getTopLocations(locationStats.states);
    if (topStates.length > 0) {
      location = formatLocationList(topStates);
    }
  } else if (locationStats?.countries && !isLocationScattered(locationStats.countries, 0.05, 10)) {
    const topCountries = getTopLocations(locationStats.countries, 0.05, 10);
    if (topCountries.length > 0) {
      location = formatLocationList(topCountries);
    }
  }

  // Combine location with date range
  const dateRange = formatDateRange(startDate, endDate);
  return location == '' ? dateRange : `${location} (${dateRange})`;
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
    await this.assetRepository.query('REFRESH MATERIALIZED VIEW asset_analysis');
  }

  async recentHighlight(userIds: string[], seed: number, limit: number): Promise<Memorylane> {
    const result = await this.assetRepository.query(recentHighlightsQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    const title = 'Recent highlights';
    return { title, assetIds };
  }

  async year(userIds: string[], seed: number, limit: number): Promise<Memorylane> {
    const result = await this.assetRepository.query(yearQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);


    let title = 'Spotlight';

    if (result && result.length > 0) {
      const { year } = result[0];
      title = `Spotlight on ${year}`;
    }

    return { title, assetIds };
  }

  async cluster(userIds: string[], seed: number, limit: number): Promise<Memorylane> {
    const result = await this.assetRepository.query(clusterQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    let title = 'Cluster';

    if (result && result.length > 0) {
      const { cluster_location_stats: locationStats, cluster_start: startDate, cluster_end: endDate } = result[0];

      title = generateClusterTitle({
        locationStats,
        startDate: new Date(startDate),
        endDate: new Date(endDate),
      });
    }

    return { title, assetIds };
  }

  async person(userIds: string[], seed: number, limit: number): Promise<Memorylane> {
    const result = await this.assetRepository.query(personQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    let title = 'Spotlight';

    if (result && result.length > 0) {
      const { person_name: personName } = result[0];
      title = personName ? `Spotlight on ${personName}` : title;
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
         cluster_data AS (SELECT c.cluster_id                           AS cluster_id,
                                 c.cluster_start                        AS cluster_start,
                                 c.cluster_end                          AS cluster_end,
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
                    cd.cluster_start,
                    cd.cluster_end,
                    cd.weight,
                    SUM(cd.weight) OVER ()                       AS total_weight,
                    SUM(cd.weight) OVER (ORDER BY cd.cluster_id) AS right_cumulative
             FROM cluster_data cd),
         cluster_w2 AS (
             /* Define left_cumulative using LAG(...) */
             SELECT cw.cluster_id,
                    cw.cluster_start,
                    cw.cluster_end,
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
             SELECT
                 cw2.cluster_id,
                 cw2.cluster_start,
                 cw2.cluster_end
             FROM cluster_w2 cw2
                      CROSS JOIN cluster_rng cr
             WHERE (cr.r * cw2.total_weight) >= cw2.left_cumulative
               AND (cr.r * cw2.total_weight) < cw2.right_cumulative
             LIMIT 1),

/* ---------------------------------------------------------------------------
   2) From that chosen cluster, select random photos:
      - Only photos with quality >= 0
      - Weight = 1 + normalized_quality_score
      - Enforce 15min min separation
      - Return up to 12 total
      - Sort final results by timestamp
--------------------------------------------------------------------------- */
         data AS (
             /* Pull assets in the chosen cluster and define the new weight. */
             SELECT ad.id,
                    ad.ts,
                    (1 + COALESCE(ad.normalized_quality_score, 0)) AS weight
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
         location_counts AS (
            WITH total_stats AS (
                SELECT COUNT(*) as total_count
                FROM location_stats
            ),
            city_total AS (
                SELECT COUNT(*) as total_count
                FROM location_stats
                WHERE city IS NOT NULL
            ),
            city_stats AS (
                SELECT 
                    city,
                    COUNT(*) as count
                FROM location_stats
                WHERE city IS NOT NULL
                GROUP BY city
            ),
            state_total AS (
                SELECT COUNT(*) as total_count
                FROM location_stats
                WHERE state IS NOT NULL
            ),
            state_stats AS (
                SELECT 
                    state,
                    COUNT(*) as count
                FROM location_stats
                WHERE state IS NOT NULL
                GROUP BY state
            ),
            country_total AS (
                SELECT COUNT(*) as total_count
                FROM location_stats
                WHERE country IS NOT NULL
            ),
            country_stats AS (
                SELECT 
                    country,
                    COUNT(*) as count
                FROM location_stats
                WHERE country IS NOT NULL
                GROUP BY country
            )
            SELECT 
                total_stats.total_count AS total_assets,
                jsonb_build_object(
                    'cities', (
                        SELECT jsonb_object_agg(city, (count::decimal / city_total.total_count))
                        FROM city_stats, city_total
                    ),
                    'states', (
                        SELECT jsonb_object_agg(state, (count::decimal / state_total.total_count))
                        FROM state_stats, state_total
                    ),
                    'countries', (
                        SELECT jsonb_object_agg(country, (count::decimal / country_total.total_count))
                        FROM country_stats, country_total
                    )
                ) AS location_distribution
            FROM total_stats
        )

/* ---------------------------------------------------------------------------
   Final selection: up to 12 photos, sorted by time
--------------------------------------------------------------------------- */
    SELECT fc.id,
       lc.location_distribution as cluster_location_stats,
       cc.cluster_start,
       cc.cluster_end
    FROM filtered_candidates fc
             CROSS JOIN CONSTANTS c
             CROSS JOIN location_counts lc
             CROSS JOIN chosen_cluster cc
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

const personQuery = `
WITH CONSTANTS AS (
    SELECT $1::bigint AS LCG_SEED,
           $2::int AS RESULT_LIMIT,
           $3::uuid[] AS USER_IDS,
           INTERVAL '15 minutes' AS MIN_TIME_BETWEEN_PHOTOS,
           2 * $2::int AS MIN_PICTURES_PER_PERSON
),

/* ---------------------------------------------------------------------------
   1) Get all persons with their photo count (normalized score >= 1)
   and randomly select one person using weighted selection
--------------------------------------------------------------------------- */
person_data AS (
    SELECT
        p.id AS person_id,
        p.name AS person_name,
        COUNT(DISTINCT af.id) as photo_count,
        SQRT(COUNT(DISTINCT af.id)::float) as weight
    FROM person p
    JOIN asset_faces af ON p.id = af."personId"
    JOIN asset_analysis aa ON af."assetId" = aa.id
    CROSS JOIN CONSTANTS co
    WHERE p."ownerId" = ANY(co.USER_IDS)
    AND aa.normalized_quality_score >= 1
    GROUP BY p.id, p.name, co.MIN_PICTURES_PER_PERSON
    HAVING COUNT(DISTINCT af.id) > co.MIN_PICTURES_PER_PERSON
),
person_w AS (
    SELECT
        pd.person_id,
        pd.person_name,
        pd.weight,
        SUM(pd.weight) OVER () AS total_weight,
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
person_rng AS (
    WITH RECURSIVE seed_seq(i, seed) AS (
        SELECT 1, (c.LCG_SEED + 2147483648::bigint) % 2147483648
        FROM CONSTANTS c
        UNION ALL
        SELECT i + 1, (1103515245 * seed + 12345) % 2147483648
        FROM seed_seq
        CROSS JOIN CONSTANTS c
        WHERE i < 1
    )
    SELECT seed_seq.seed::float / 2147483648 AS r
    FROM seed_seq
    WHERE i = 1
),
chosen_person AS (
    SELECT
        pw2.person_id,
        pw2.person_name
    FROM person_w2 pw2
    CROSS JOIN person_rng pr
    WHERE (pr.r * pw2.total_weight) >= pw2.left_cumulative
    AND (pr.r * pw2.total_weight) < pw2.right_cumulative
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
        COALESCE(aa.normalized_quality_score, 1) AS weight
    FROM asset_analysis aa
    JOIN asset_faces af ON aa.id = af."assetId"
    JOIN chosen_person cp ON af."personId" = cp.person_id
    WHERE aa.normalized_quality_score >= 1
),
w AS (
    SELECT
        d.id,
        d.ts,
        d.weight,
        SUM(d.weight) OVER () AS total_weight,
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
rng AS (
    WITH RECURSIVE seed_seq(i, seed) AS (
        SELECT 1, (c.LCG_SEED + 2147483648::bigint) % 2147483648
        FROM CONSTANTS c
        UNION ALL
        SELECT i + 1, (1103515245 * seed + 12345) % 2147483648
        FROM seed_seq
        CROSS JOIN CONSTANTS c
        WHERE i < 2 * c.RESULT_LIMIT
    )
    SELECT
        i AS draw_number,
        seed_seq.seed::float / 2147483648 AS r
    FROM seed_seq
),
candidates AS (
    SELECT
        rng.draw_number,
        w2.id,
        w2.ts
    FROM rng
    JOIN w2 ON (rng.r * w2.total_weight) >= w2.left_cumulative
    AND (rng.r * w2.total_weight) < w2.right_cumulative
    ORDER BY rng.draw_number
),
filtered_candidates AS (
    SELECT
        a.draw_number,
        ROW_NUMBER() OVER (ORDER BY a.draw_number) AS row_number,
        a.id,
        a.ts
    FROM candidates a
    CROSS JOIN CONSTANTS c
    WHERE NOT EXISTS (
        SELECT 1
        FROM candidates b
        WHERE b.draw_number < a.draw_number
        AND a.ts - b.ts < c.MIN_TIME_BETWEEN_PHOTOS
        AND b.ts - a.ts < c.MIN_TIME_BETWEEN_PHOTOS
    )
    ORDER BY draw_number
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
ORDER BY fc.ts;
`;

const yearQuery = `
    WITH
        CONSTANTS AS (
            SELECT
                $1::BIGINT            AS LCG_SEED,
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
            WHERE c.normalized_quality_score >= 1
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
        year_rng AS (
            /* A single random draw via LCG, seeded by LCG_SEED. */
            WITH
                RECURSIVE
                seed_seq(i, seed) AS (
                    SELECT
                        1,
                        (c.LCG_SEED + 2147483648::BIGINT) % 2147483648
                    FROM CONSTANTS c
                    UNION ALL
                    SELECT
                        i + 1,
                        (1103515245 * seed + 12345) % 2147483648
                    FROM seed_seq
                         CROSS JOIN CONSTANTS c
                    WHERE i < 1
                )
            SELECT
                seed_seq.seed::FLOAT / 2147483648 AS r
            FROM seed_seq
            WHERE i = 1
        ),
        chosen_year AS (
            /*
               Pick the single year whose interval covers r * total_weight.
               This always returns exactly one row.
            */
            SELECT
                cw2.year
            FROM year_w2 cw2
                 CROSS JOIN year_rng cr
            WHERE (cr.r * cw2.total_weight) >= cw2.left_cumulative
              AND (cr.r * cw2.total_weight) < cw2.right_cumulative
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
                (1 + COALESCE(ad.normalized_quality_score, 0)) AS weight
            FROM asset_analysis ad
                 JOIN chosen_year cc ON EXTRACT(YEAR FROM ad.ts) = cc.year
            WHERE ad.normalized_quality_score >= 1
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
        rng AS (
            /* Generate (2 * RESULT_LIMIT) random draws via the same LCG. */
            WITH
                RECURSIVE
                seed_seq(i, seed) AS (
                    SELECT
                        1,
                        (c.LCG_SEED + 2147483648::BIGINT) % 2147483648
                    FROM CONSTANTS c
                    UNION ALL
                    SELECT
                        i + 1,
                        (1103515245 * seed + 12345) % 2147483648
                    FROM seed_seq
                         CROSS JOIN CONSTANTS c
                    WHERE i < 2 * c.RESULT_LIMIT
                )
            SELECT
                i                                 AS draw_number,
                seed_seq.seed::FLOAT / 2147483648 AS r
            FROM seed_seq
        ),
        candidates AS (
            /*
               For each random draw, find the photo whose [left_cumulative, right_cumulative)
               covers r * total_weight. Order them by draw_number so we can filter by
               “first come, first served” below.
            */
            SELECT
                rng.draw_number,
                w2.id,
                w2.ts
            FROM rng
                 JOIN w2
                      ON (rng.r * w2.total_weight) >= w2.left_cumulative
                          AND (rng.r * w2.total_weight) < w2.right_cumulative
            ORDER BY rng.draw_number
        ),
        filtered_candidates AS (
            /*
               Discard any photo if it’s within 15 minutes of an already-chosen photo.
               Then keep only the first RESULT_LIMIT picks.
            */
            SELECT
                a.draw_number,
                ROW_NUMBER() OVER (ORDER BY a.draw_number) AS row_number,
                a.id,
                a.ts
            FROM candidates a
                 CROSS JOIN CONSTANTS c
            WHERE NOT EXISTS (
                SELECT
                    1
                FROM candidates b
                WHERE b.draw_number < a.draw_number
                  AND a.ts - b.ts < c.MIN_TIME_BETWEEN_PHOTOS -- Minimum spacing check
                  AND b.ts - a.ts < c.MIN_TIME_BETWEEN_PHOTOS -- Minimum spacing check
            )
            ORDER BY a.draw_number
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
`;