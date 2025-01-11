
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
