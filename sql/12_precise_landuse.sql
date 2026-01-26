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
    15 as bonus_forest, -15 as penalty_field, -20 as penalty_urban, 20 as bonus_rapids;

-- =================================================================
-- 2. IMPORT CONTEXT DATA
-- =================================================================

-- A. Official Canoe Routes
CREATE TABLE official_routes AS
SELECT osm_id, way as geom FROM planet_osm_line WHERE route = 'canoe';
CREATE INDEX idx_routes_geom ON official_routes USING GIST(geom);

-- B. Rapids
CREATE TABLE rapids_features AS
SELECT osm_id, way as geom FROM planet_osm_line WHERE "waterway" = 'rapids'
UNION ALL
SELECT osm_id, way as geom FROM planet_osm_point WHERE "waterway" IN ('rapids', 'waterfall');
CREATE INDEX idx_rapids_geom ON rapids_features USING GIST(geom);

-- C. POIs
CREATE TABLE paddling_pois AS
SELECT way as geom, 'shelter' as type FROM planet_osm_point 
WHERE "tourism" IN ('camp_site','lean_to','picnic_site','wilderness_hut') OR "man_made" IN ('pier', 'mast') OR "leisure" = 'slipway'
UNION ALL
SELECT ST_Centroid(way), 'shelter' FROM planet_osm_polygon WHERE "tourism" IN ('camp_site','lean_to');
CREATE INDEX idx_pad_pois ON paddling_pois USING GIST(geom);

-- D. LAND COVER (Expanded Categories)
CREATE TABLE land_cover AS
SELECT 
    osm_id, 
    ST_MakeValid(way) as geom, -- Ensure validity for intersection
    CASE 
        WHEN "landuse" IN ('forest', 'wood') OR "natural" IN ('wood', 'scrub', 'heath') THEN 'forest'
        WHEN "landuse" IN ('farmland', 'farm', 'meadow', 'grass', 'orchard') THEN 'field'
        WHEN "landuse" IN ('residential', 'industrial', 'commercial', 'retail', 'farmyard') THEN 'urban'
        ELSE 'other'
    END as type
FROM planet_osm_polygon
WHERE "landuse" IN ('forest', 'wood', 'farmland', 'farm', 'meadow', 'grass', 'orchard', 'residential', 'industrial', 'commercial', 'retail', 'farmyard') 
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

ALTER TABLE paddling_segments ADD COLUMN length_m float;
ALTER TABLE paddling_segments ADD COLUMN sinuosity numeric;
ALTER TABLE paddling_segments ADD COLUMN environment text; 
ALTER TABLE paddling_segments ADD COLUMN has_poi_signal boolean DEFAULT FALSE;
ALTER TABLE paddling_segments ADD COLUMN is_official_route boolean DEFAULT FALSE;
ALTER TABLE paddling_segments ADD COLUMN has_rapids boolean DEFAULT FALSE;
ALTER TABLE paddling_segments ADD COLUMN land_type text DEFAULT 'mixed';

UPDATE paddling_segments SET length_m = ST_Length(geom);
UPDATE paddling_segments SET sinuosity = CASE WHEN ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) < 1 THEN 3.0 ELSE ROUND((length_m::numeric / ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom))::numeric), 2) END;

-- =================================================================
-- 4. ADVANCED DETECTION (AREA OVERLAP)
-- =================================================================

UPDATE paddling_segments s SET is_official_route = TRUE FROM official_routes r WHERE ST_Intersects(s.geom, r.geom);
UPDATE paddling_segments s SET has_rapids = TRUE FROM rapids_features r WHERE ST_DWithin(s.geom, r.geom, 50);
UPDATE paddling_segments s SET has_poi_signal = TRUE FROM paddling_pois p WHERE ST_DWithin(s.geom, p.geom, 300);

-- *** THE FIX: PERCENTAGE BASED LAND USE ***
-- 1. Create a 50m buffer around the segment.
-- 2. Intersect with land cover.
-- 3. If a type covers > 50% of the buffer area, assign it.
WITH land_stats AS (
    SELECT 
        s.seg_id,
        lc.type,
        SUM(ST_Area(ST_Intersection(ST_Buffer(s.geom, 50), lc.geom))) as type_area,
        ST_Area(ST_Buffer(s.geom, 50)) as total_buffer_area
    FROM paddling_segments s
    JOIN land_cover lc ON ST_Intersects(ST_Buffer(s.geom, 50), lc.geom)
    GROUP BY s.seg_id, lc.type, total_buffer_area
)
UPDATE paddling_segments s
SET land_type = ls.type
FROM land_stats ls
WHERE s.seg_id = ls.seg_id
  AND ls.type_area > (ls.total_buffer_area * 0.5); -- 50% Threshold

-- Environment Logic
UPDATE paddling_segments s SET environment = CASE 
    WHEN type IN ('lake_crossing', 'star_connector') THEN 'lake_route'
    WHEN type = 'portage_link' THEN 'portage'
    WHEN is_official_route THEN 'river_official'
    ELSE 'river' END;

UPDATE paddling_segments SET land_type = 'water' WHERE environment = 'lake_route';

-- =================================================================
-- 5. SCORING & EXPORT
-- =================================================================
ALTER TABLE paddling_segments ADD COLUMN feasibility_score int;
ALTER TABLE paddling_segments ADD COLUMN fun_score int;

UPDATE paddling_segments s SET fun_score = (SELECT 
    CASE 
        WHEN s.type = 'portage_link' THEN 10 
        WHEN s.environment = 'lake_route' THEN 60 
        ELSE (
            CASE WHEN s.sinuosity > 1.2 THEN 90 ELSE 70 END + 
            CASE WHEN s.land_type = 'forest' THEN 15 
                 WHEN s.land_type = 'field' THEN -15 
                 WHEN s.land_type = 'urban' THEN -20 
                 ELSE 0 END + 
            CASE WHEN s.has_rapids THEN 20 ELSE 0 END
        ) 
    END FROM scoring_config);

UPDATE paddling_segments SET fun_score = 100 WHERE fun_score > 100;
UPDATE paddling_segments SET fun_score = 0 WHERE fun_score < 0;

UPDATE paddling_segments s SET feasibility_score = (SELECT 
    CASE 
        WHEN s.is_official_route THEN 100 
        WHEN s.type = 'portage_link' THEN 100 
        WHEN s.has_poi_signal THEN 90 
        ELSE 50 
    END FROM scoring_config);

-- Propagation
CREATE TEMPORARY TABLE neighbor_stats AS
SELECT a.seg_id, MAX(b.feasibility_score) as max_neighbor_score
FROM paddling_segments a
JOIN paddling_segments b ON ST_Touches(a.geom, b.geom) AND a.seg_id != b.seg_id
GROUP BY a.seg_id;

UPDATE paddling_segments s
SET feasibility_score = GREATEST(s.feasibility_score, (s.feasibility_score * 0.7 + ns.max_neighbor_score * 0.3)::int)
FROM neighbor_stats ns
WHERE s.seg_id = ns.seg_id AND s.feasibility_score < ns.max_neighbor_score;

-- =================================================================
-- 6. AREA STATS (CORRECTED)
-- =================================================================
DROP TABLE IF EXISTS paddling_areas_stats;

CREATE TABLE paddling_areas_stats AS
SELECT 
    network_id,
    -- Get the best name (longest named river in the group)
    (SELECT name FROM paddling_segments s2 
     WHERE s2.network_id = s.network_id 
     AND name NOT LIKE 'Unnamed%' 
     AND type = 'river' 
     ORDER BY length_m DESC LIMIT 1) as area_name,
     
    -- Calculate the missing stats
    COUNT(*) as segment_count,
    ROUND(SUM(length_m)::numeric / 1000, 2) as total_km,
    ROUND(AVG(fun_score), 0) as avg_fun,
    MAX(feasibility_score) as max_feasibility,
    
    ST_Union(geom) as geom
FROM paddling_segments s
WHERE network_id IS NOT NULL
GROUP BY network_id;