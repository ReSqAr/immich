import { Inject, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { AssetEntity } from 'src/entities/asset.entity';
import { ILoggerRepository } from 'src/interfaces/logger.interface';
import {
  IMemorylaneRepository,
  MemoryLaneCluster,
  MemoryLanePerson,
  MemoryLaneRecentHighlights,
  MemoryLaneYear,
} from 'src/interfaces/memorylane.interface';
import { Repository } from 'typeorm';

type LocationStatistics = {
  cities?: Record<string, number>;
  states?: Record<string, number>;
  countries?: Record<string, number>;
};

function isLocationScattered(locations: Record<string, number>, threshold = 0.05, topN = 2): boolean {
  if (!locations || Object.keys(locations).length === 0) {
    return true;
  }

  const frequencies = Object.entries(locations)
    .filter(([location]) => location !== 'unknown')
    .map(([, freq]) => freq)
    .sort((a, b) => b - a);

  if (frequencies.length === 0) {
    return true;
  }

  const remainder = frequencies.slice(topN).reduce((sum, freq) => sum + freq, 0);
  return remainder > threshold;
}

function getTopLocations(locations: Record<string, number>, threshold = 0.05, topN = 2): string[] {
  return Object.entries(locations)
    .filter(([location, freq]) => location && location.trim() && location !== 'unknown' && freq >= threshold)
    .sort(([, freqA], [, freqB]) => freqB - freqA)
    .slice(0, topN)
    .map(([location]) => location);
}

function extractLocations(locationStats: LocationStatistics): string[] {
  if (locationStats?.cities && !isLocationScattered(locationStats.cities)) {
    const topCities = getTopLocations(locationStats.cities);
    if (topCities.length > 0) {
      return topCities;
    }
  } else if (locationStats?.states && !isLocationScattered(locationStats.states)) {
    const topStates = getTopLocations(locationStats.states);
    if (topStates.length > 0) {
      return topStates;
    }
  } else if (locationStats?.countries && !isLocationScattered(locationStats.countries, 0.05, 10)) {
    const topCountries = getTopLocations(locationStats.countries, 0.05, 10);
    if (topCountries.length > 0) {
      return topCountries;
    }
  }

  return [];
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
    const startTime = Date.now();

    await this.assetRepository.query('REFRESH MATERIALIZED VIEW asset_dbscan');
    await this.assetRepository.query('REFRESH MATERIALIZED VIEW asset_dbscan_clusters');
    await this.assetRepository.query('REFRESH MATERIALIZED VIEW asset_analysis');

    const duration = Date.now() - startTime;
    this.logger.debug(`refreshed all memorylane materialized views in ${duration}ms`);
  }

  async cluster(userIds: string[], seed: number, limit: number): Promise<MemoryLaneCluster> {
    const result = await this.assetRepository.query(clusterQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    if (!result || result.length == 0) {
      return {clusterID: undefined, locations: [], startDate: undefined, endDate: undefined, assetIds};
    }

    const {
      cluster_id: clusterID,
      cluster_start: startDate,
      cluster_end: endDate,
      cluster_location_distribution: clusterLocationDistribution,
    } = result[0];

    return {
      clusterID,
      locations: extractLocations(clusterLocationDistribution),
      startDate: new Date(startDate),
      endDate: new Date(endDate),
      assetIds,
    };
  }

  async person(userIds: string[], seed: number, limit: number): Promise<MemoryLanePerson> {
    const result = await this.assetRepository.query(personQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    let personName = undefined;
    if (result && result.length > 0) {
      personName = result[0]['person_name'];
    }

    return { personName, assetIds };
  }

  async recentHighlight(userIds: string[], seed: number, limit: number): Promise<MemoryLaneRecentHighlights> {
    const result = await this.assetRepository.query(recentHighlightsQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    return { assetIds };
  }

  async year(userIds: string[], seed: number, limit: number): Promise<MemoryLaneYear> {
    const result = await this.assetRepository.query(yearQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    let year = undefined;
    if (result && result.length > 0) {
      year = result[0]['year'];
    }

    return { year: `${year}`, assetIds };
  }
}

const clusterQuery = `
    WITH
        CONSTANTS AS (
            SELECT
                $1::BIGINT            AS SEED,
                $2::INT               AS RESULT_LIMIT,
                $3::uuid[]            AS USER_IDS,
                INTERVAL '15 minutes' AS MIN_TIME_BETWEEN_PHOTOS
        ),

/* ---------------------------------------------------------------------------
   1) Randomly pick exactly ONE cluster.
      We do this by giving each cluster a weight = 1, summing them, then doing
      one random draw. 
--------------------------------------------------------------------------- */
        cluster_data AS (
            SELECT
                c.cluster_id                           AS cluster_id,
                c.cluster_start                        AS cluster_start,
                c.cluster_end                          AS cluster_end,
                JSONB_BUILD_OBJECT(
                    'cities', c.cities,
                    'states', c.states,
                    'countries', c.countries
                ) AS cluster_location_distribution,
                SQRT(c.cluster_cardinality_score_ge_0) AS weight
            FROM asset_dbscan_clusters c
                 CROSS JOIN CONSTANTS co
            -- Optional: filter out uninteresting clusters if desired
            WHERE c.cluster_cardinality_score_ge_0 >= co.RESULT_LIMIT
              AND c."ownerId" = ANY (co.USER_IDS)
        ),
        cluster_w AS (
            /* Compute total_weight, plus a running sum (right_cumulative). */
            SELECT
                cd.cluster_id,
                cd.cluster_start,
                cd.cluster_end,
                cd.cluster_location_distribution,
                cd.weight,
                SUM(cd.weight) OVER ()                       AS total_weight,
                SUM(cd.weight) OVER (ORDER BY cd.cluster_id) AS right_cumulative
            FROM cluster_data cd
        ),
        cluster_w2 AS (
            /* Define left_cumulative using LAG(...) */
            SELECT
                cw.cluster_id,
                cw.cluster_start,
                cw.cluster_end,
                cw.cluster_location_distribution,
                cw.weight,
                cw.total_weight,
                cw.right_cumulative,
                COALESCE(
                                LAG(cw.right_cumulative) OVER (ORDER BY cw.cluster_id),
                                0
                ) AS left_cumulative
            FROM cluster_w cw
        ),
        chosen_cluster AS (
            /*
               Pick the single cluster whose interval covers r % total_weight.
               This always returns exactly one row.
            */
            SELECT
                cw2.cluster_id,
                cw2.cluster_start,
                cw2.cluster_end,
                cw2.cluster_location_distribution
            FROM cluster_w2 cw2
                 CROSS JOIN CONSTANTS c
            WHERE (c.SEED % ROUND(1367 * cw2.total_weight)::BIGINT) BETWEEN 1367 * cw2.left_cumulative AND 1367 * cw2.right_cumulative
            LIMIT 1
        ),

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
            SELECT
                ad.id,
                ad.ts,
                1 + ad.normalized_quality_score AS weight
            FROM asset_analysis ad
                 JOIN chosen_cluster cc ON ad.cluster_id = cc.cluster_id
            WHERE ad.normalized_quality_score >= 0
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
                i as draw_number,
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
        cc.cluster_id,
        cc.cluster_start,
        cc.cluster_end,
        cc.cluster_location_distribution
    FROM filtered_candidates fc
         CROSS JOIN CONSTANTS c
         CROSS JOIN chosen_cluster cc
    WHERE fc.row_number <= c.RESULT_LIMIT
    ORDER BY fc.ts
`;

const recentHighlightsQuery = `
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
      /* 1) Pull relevant rows; rename score -> weight. */
      SELECT
        ad.id,
        ad.ts                       AS ts,
        1 + ad.normalized_quality_score AS weight
      FROM asset_analysis AS ad
           CROSS JOIN CONSTANTS c
      WHERE ad.ts >= CURRENT_TIMESTAMP - c.LOOKBACK_WINDOW
        AND ad.normalized_quality_score >= c.MIN_QUALITY_SCORE
        AND ad."ownerId" = ANY (c.USER_IDS)
    ),
    w AS (
      /* 2) Compute total_weight and right_cumulative. */
      SELECT
        d.id,
        d.ts,
        d.weight,
        /* total_weight is same for every row. */
        SUM(d.weight) OVER ()              AS total_weight,
        /* sum(...) OVER (ORDER BY ts) is the running total, i.e. "right boundary". */
        SUM(d.weight) OVER (ORDER BY d.ts) AS right_cumulative
      FROM data d
    ),
    w2 AS (
      /* 3) Use LAG(...) to get "left boundary" from the previous row's right_cumulative. */
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

  SELECT
    id
  FROM filtered_candidates
       CROSS JOIN CONSTANTS c
  WHERE row_number <= c.RESULT_LIMIT
  ORDER BY draw_number
`;

const personQuery = `
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
`;

const yearQuery = `
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
`;
