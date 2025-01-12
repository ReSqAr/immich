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

data AS (
    SELECT
        a.id,
        a."ownerId",
        a."localDateTime" AS ts,
        e.latitude,
        e.longitude
    FROM assets a
         JOIN exif e ON a.id = e."assetId"
    WHERE
          a."deletedAt" IS NULL
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
