-- =================================================================
-- 1. CLEANUP & CONFIG
-- =================================================================
DROP TABLE IF EXISTS scoring_config;
DROP TABLE IF EXISTS paddling_pois;
DROP TABLE IF EXISTS official_routes;
DROP TABLE IF EXISTS rapids_features;
DROP TABLE IF EXISTS land_cover;
DROP TABLE IF EXISTS paddling_segments;
DROP TABLE IF EXISTS paddling_areas_stats;

CREATE TABLE scoring_config AS SELECT 
    300 as poi_proximity_dist, 50 as base_feasibility, 40 as poi_bonus, 1.2 as sinuosity_threshold, 
    90 as score_winding, 70 as score_normal, 40 as score_boring, 60 as score_lake, 10 as score_portage,
    15 as bonus_forest, -10 as penalty_field, 20 as bonus_rapids;

-- =================================================================
-- 2. IMPORT CONTEXT DATA (PERMANENT TABLES)
-- =================================================================

-- A. Official Canoe Routes (STRICT: No Ferries!)
CREATE TABLE official_routes AS
SELECT osm_id, way as geom 
FROM planet_osm_line 
WHERE route = 'canoe'; -- Only explicit canoe routes

CREATE INDEX idx_routes_geom ON official_routes USING GIST(geom);

-- B. Rapids
CREATE TABLE rapids_features AS
SELECT osm_id, way as geom FROM planet_osm_line WHERE "waterway" = 'rapids'
UNION ALL
SELECT osm_id, way as geom FROM planet_osm_point WHERE "waterway" IN ('rapids', 'waterfall');

CREATE INDEX idx_rapids_geom ON rapids_features USING GIST(geom);

-- C. POIs (Shelters, Piers, etc.)
CREATE TABLE paddling_pois AS
SELECT way as geom, 'shelter' as type 
FROM planet_osm_point 
WHERE "tourism" IN ('camp_site','lean_to','picnic_site','wilderness_hut')
   OR "man_made" IN ('pier', 'mast') -- Masts are often good landmarks
   OR "leisure" = 'slipway'
UNION ALL
SELECT ST_Centroid(way), 'shelter' 
FROM planet_osm_polygon 
WHERE "tourism" IN ('camp_site','lean_to');

CREATE INDEX idx_pad_pois ON paddling_pois USING GIST(geom);

-- D. Land Cover (Expanded Definition)
-- Includes natural=wood which is common in Finland
CREATE TABLE land_cover AS
SELECT 
    osm_id, 
    ST_Simplify(way, 10) as geom, 
    CASE 
        WHEN "landuse" IN ('forest', 'wood') OR "natural" IN ('wood', 'scrub', 'heath') THEN 'forest'
        WHEN "landuse" IN ('farmland', 'farm', 'meadow', 'grass', 'orchard') THEN 'field'
        ELSE 'other'
    END as type
FROM planet_osm_polygon
WHERE "landuse" IN ('forest', 'wood', 'farmland', 'farm', 'meadow', 'grass', 'orchard') 
   OR "natural" IN ('wood', 'scrub', 'heath');

CREATE INDEX idx_land_cover ON land_cover USING GIST(geom);

-- =================================================================
-- 3. BUILD SEGMENTS
-- =================================================================
CREATE TABLE paddling_segments AS
WITH segment_gen AS (
    SELECT gid as parent_id, osm_id, name, type, network_id,
        CASE WHEN ST_Length(geom) > 1000 THEN CEIL(ST_Length(geom) / 1000.0)::int ELSE 1 END as num_chunks, geom
    FROM paddling_network
)
SELECT ROW_NUMBER() OVER() as seg_id, parent_id, osm_id, name, type, network_id,
    ST_LineSubstring(geom, (n-1)::float/num_chunks, n::float/num_chunks) as geom
FROM segment_gen CROSS JOIN LATERAL generate_series(1, num_chunks) as n;

CREATE INDEX idx_pad_seg_geom ON paddling_segments USING GIST(geom);

-- Add Columns
ALTER TABLE paddling_segments ADD COLUMN length_m float;
ALTER TABLE paddling_segments ADD COLUMN sinuosity numeric;
ALTER TABLE paddling_segments ADD COLUMN environment text; 
ALTER TABLE paddling_segments ADD COLUMN has_poi_signal boolean DEFAULT FALSE;
ALTER TABLE paddling_segments ADD COLUMN is_official_route boolean DEFAULT FALSE;
ALTER TABLE paddling_segments ADD COLUMN has_rapids boolean DEFAULT FALSE;
ALTER TABLE paddling_segments ADD COLUMN land_type text DEFAULT 'mixed';

-- Calc Base Metrics
UPDATE paddling_segments SET length_m = ST_Length(geom);
UPDATE paddling_segments SET sinuosity = CASE WHEN ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) < 1 THEN 3.0 ELSE ROUND((length_m::numeric / ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom))::numeric), 2) END;

-- =================================================================
-- 4. ADVANCED DETECTION
-- =================================================================

-- Official Routes (Spatial Match)
UPDATE paddling_segments s SET is_official_route = TRUE FROM official_routes r 
WHERE ST_Intersects(s.geom, r.geom);

-- Rapids (Proximity 50m)
UPDATE paddling_segments s SET has_rapids = TRUE FROM rapids_features r 
WHERE ST_DWithin(s.geom, r.geom, 50);

-- POI Signals (Proximity 300m)
UPDATE paddling_segments s SET has_poi_signal = TRUE FROM paddling_pois p 
WHERE ST_DWithin(s.geom, p.geom, 300);

-- LAND COVER FIX: Two-Pass Search
-- Pass 1: Strict Check (Within 50m)
UPDATE paddling_segments s SET land_type = sub.type
FROM (
    SELECT s.seg_id, lc.type,
        ROW_NUMBER() OVER(PARTITION BY s.seg_id ORDER BY ST_Distance(s.geom, lc.geom) ASC) as rn
    FROM paddling_segments s
    JOIN land_cover lc ON ST_DWithin(s.geom, lc.geom, 50)
) sub
WHERE s.seg_id = sub.seg_id AND sub.rn = 1;

-- Pass 2: Desperate Check (Within 300m) for items still 'mixed'
UPDATE paddling_segments s SET land_type = sub.type
FROM (
    SELECT s.seg_id, lc.type,
        ROW_NUMBER() OVER(PARTITION BY s.seg_id ORDER BY ST_Distance(s.geom, lc.geom) ASC) as rn
    FROM paddling_segments s
    JOIN land_cover lc ON ST_DWithin(s.geom, lc.geom, 300)
    WHERE s.land_type = 'mixed'
) sub
WHERE s.seg_id = sub.seg_id AND sub.rn = 1 AND s.land_type = 'mixed';

-- Environment Logic
UPDATE paddling_segments s SET environment = CASE 
    WHEN type IN ('lake_crossing', 'star_connector') THEN 'lake_route'
    WHEN type = 'portage_link' THEN 'portage'
    WHEN is_official_route THEN 'river_official'
    ELSE 'river' END;

-- Lake Override
UPDATE paddling_segments SET land_type = 'water' WHERE environment = 'lake_route';

-- =================================================================
-- 5. SCORING & EXPORT
-- =================================================================
ALTER TABLE paddling_segments ADD COLUMN feasibility_score int;
ALTER TABLE paddling_segments ADD COLUMN fun_score int;

-- Fun Score
UPDATE paddling_segments s SET fun_score = (SELECT 
    CASE 
        WHEN s.type = 'portage_link' THEN 10 
        WHEN s.environment = 'lake_route' THEN 60 
        ELSE (
            CASE WHEN s.sinuosity > 1.2 THEN 90 ELSE 70 END + 
            CASE WHEN s.land_type = 'forest' THEN 15 WHEN s.land_type = 'field' THEN -10 ELSE 0 END + 
            CASE WHEN s.has_rapids THEN 20 ELSE 0 END
        ) 
    END FROM scoring_config);

-- Clamp
UPDATE paddling_segments SET fun_score = 100 WHERE fun_score > 100;
UPDATE paddling_segments SET fun_score = 0 WHERE fun_score < 0;

-- Feasibility
UPDATE paddling_segments s SET feasibility_score = (SELECT 
    CASE 
        WHEN s.is_official_route THEN 100 
        WHEN s.type = 'portage_link' THEN 100 
        WHEN s.has_poi_signal THEN 90 
        ELSE 50 
    END FROM scoring_config);

-- Neighbor Propagation
CREATE TEMPORARY TABLE neighbor_stats AS
SELECT a.seg_id, MAX(b.feasibility_score) as max_neighbor_score
FROM paddling_segments a
JOIN paddling_segments b ON ST_Touches(a.geom, b.geom) AND a.seg_id != b.seg_id
GROUP BY a.seg_id;

UPDATE paddling_segments s
SET feasibility_score = GREATEST(s.feasibility_score, (s.feasibility_score * 0.7 + ns.max_neighbor_score * 0.3)::int)
FROM neighbor_stats ns
WHERE s.seg_id = ns.seg_id AND s.feasibility_score < ns.max_neighbor_score;

-- Area Stats
CREATE TABLE paddling_areas_stats AS
SELECT network_id, MAX(name) as area_name, ST_Union(geom) as geom
FROM paddling_segments GROUP BY network_id;