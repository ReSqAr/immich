CREATE MATERIALIZED VIEW asset_dbscan_clusters AS
WITH
    cluster_data AS (
        SELECT
            ad.cluster_id,
            ad."ownerId",
            ad.ts,
            q.score,
            COALESCE(e.city, 'unknown')    AS city,
            COALESCE(e.state, 'unknown')   AS state,
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
            city,
            state,
            country,
            CASE
                WHEN score IS NOT NULL AND STDDEV(score) OVER (PARTITION BY "ownerId") != 0
                    THEN (score - AVG(score) OVER (PARTITION BY "ownerId")) / STDDEV(score) OVER (PARTITION BY "ownerId")
                ELSE NULL
                END AS normalized_score
        FROM cluster_data
    ),
    cluster_basic_stats AS (
        SELECT
            cluster_id,
            "ownerId",
            MIN(ts)                                       AS cluster_start,
            MAX(ts)                                       AS cluster_end,
            MAX(ts) - MIN(ts)                             AS cluster_duration,
            COUNT(*)                                      AS cluster_cardinality,
            COUNT(*) FILTER (WHERE normalized_score >= 0) AS cluster_cardinality_score_ge_0,
            COUNT(*) FILTER (WHERE normalized_score >= 1) AS cluster_cardinality_score_ge_1
        FROM normalized_scores cs
        WHERE
            cluster_id != -1
        GROUP BY cluster_id, "ownerId"
    ),

    cities AS (
        WITH
            city_counts AS (
                SELECT
                    cluster_id,
                    "ownerId",
                    city,
                    COUNT(*) AS city_count
                FROM normalized_scores
                WHERE
                    cluster_id != -1
                GROUP BY cluster_id, "ownerId", city
            ),
            city_totals AS (
                SELECT
                    cluster_id,
                    "ownerId",
                    COUNT(*) AS total_cities
                FROM normalized_scores
                WHERE
                    cluster_id != -1
                GROUP BY cluster_id, "ownerId"
            )
        SELECT
            cc.cluster_id,
            cc."ownerId",
            JSONB_OBJECT_AGG(cc.city, ROUND((cc.city_count::DECIMAL / ct.total_cities), 4)) AS cities
        FROM city_counts cc
             JOIN city_totals ct
                  ON cc.cluster_id = ct.cluster_id AND cc."ownerId" = ct."ownerId"
        GROUP BY cc.cluster_id, cc."ownerId"
    ),

    states AS (
        WITH
            state_counts AS (
                SELECT
                    cluster_id,
                    "ownerId",
                    state,
                    COUNT(*) AS state_count
                FROM normalized_scores
                WHERE
                    cluster_id != -1
                GROUP BY cluster_id, "ownerId", state
            ),
            state_totals AS (
                SELECT
                    cluster_id,
                    "ownerId",
                    COUNT(*) AS total_states
                FROM normalized_scores
                WHERE
                    cluster_id != -1
                GROUP BY cluster_id, "ownerId"
            )
        SELECT
            sc.cluster_id,
            sc."ownerId",
            JSONB_OBJECT_AGG(sc.state, ROUND((sc.state_count::DECIMAL / st.total_states), 4)) AS states
        FROM state_counts sc
             JOIN state_totals st
                  ON sc.cluster_id = st.cluster_id AND sc."ownerId" = st."ownerId"
        GROUP BY sc.cluster_id, sc."ownerId"
    ),

    countries AS (
        WITH
            country_counts AS (
                SELECT
                    cluster_id,
                    "ownerId",
                    country,
                    COUNT(*) AS country_count
                FROM normalized_scores
                WHERE
                    cluster_id != -1
                GROUP BY cluster_id, "ownerId", country
            ),
            country_totals AS (
                SELECT
                    cluster_id,
                    "ownerId",
                    COUNT(*) AS total_countries
                FROM normalized_scores
                WHERE
                    cluster_id != -1
                GROUP BY cluster_id, "ownerId"
            )
        SELECT
            cc.cluster_id,
            cc."ownerId",
            JSONB_OBJECT_AGG(cc.country, ROUND((cc.country_count::DECIMAL / ct.total_countries), 4)) AS countries
        FROM country_counts cc
             JOIN country_totals ct
                  ON cc.cluster_id = ct.cluster_id AND cc."ownerId" = ct."ownerId"
        GROUP BY cc.cluster_id, cc."ownerId"
    ),

    cluster_location_distribution AS (
        SELECT
            cb.cluster_id,
            cb."ownerId",
            COALESCE(ci.cities, '{}'::JSONB)    AS cities,
            COALESCE(st.states, '{}'::JSONB)    AS states,
            COALESCE(ct.countries, '{}'::JSONB) AS countries
        FROM cluster_basic_stats cb
             LEFT JOIN cities ci ON cb.cluster_id = ci.cluster_id AND cb."ownerId" = ci."ownerId"
             LEFT JOIN states st ON cb.cluster_id = st.cluster_id AND cb."ownerId" = st."ownerId"
             LEFT JOIN countries ct ON cb.cluster_id = ct.cluster_id AND cb."ownerId" = ct."ownerId"
    ),

    cluster_stats_with_location AS (
        SELECT
            cb.cluster_id,
            cb."ownerId",
            cb.cluster_start,
            cb.cluster_end,
            cb.cluster_duration,
            cb.cluster_cardinality,
            cb.cluster_cardinality_score_ge_0,
            cb.cluster_cardinality_score_ge_1,
            cld.cities,
            cld.states,
            cld.countries
        FROM cluster_basic_stats cb
             LEFT JOIN cluster_location_distribution cld
                       ON cb.cluster_id = cld.cluster_id AND cb."ownerId" = cld."ownerId"
    )

SELECT *
FROM cluster_stats_with_location;

