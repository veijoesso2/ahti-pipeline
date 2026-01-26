-- 1. CONFIG
DROP TABLE IF EXISTS scoring_config;
CREATE TEMPORARY TABLE scoring_config AS SELECT 
    300 as poi_proximity_dist, 50 as base_feasibility, 40 as poi_bonus, 1.2 as sinuosity_threshold, 
    90 as score_winding, 70 as score_normal, 40 as score_boring, 60 as score_lake, 10 as score_portage,
    15 as bonus_forest, -10 as penalty_field, 20 as bonus_rapids;

-- 2. IMPORT CONTEXT (STRICTER)
CREATE TEMPORARY TABLE official_routes AS
SELECT osm_id, way as geom FROM planet_osm_line WHERE route = 'canoe'; -- STRICTLY CANOE

CREATE INDEX idx_routes_geom ON official_routes USING GIST(geom);

CREATE TEMPORARY TABLE rapids_features AS
SELECT osm_id, way as geom FROM planet_osm_line WHERE "waterway" = 'rapids'
UNION ALL
SELECT osm_id, way as geom FROM planet_osm_point WHERE "waterway" IN ('rapids', 'waterfall');

CREATE INDEX idx_rapids_geom ON rapids_features USING GIST(geom);

-- 3. IMPORT POIS (Ensured Table Exists)
DROP TABLE IF EXISTS paddling_pois;
CREATE TABLE paddling_pois AS
SELECT way as geom, 'shelter' as type FROM planet_osm_point WHERE "tourism" IN ('camp_site','lean_to','picnic_site')
UNION ALL
SELECT ST_Centroid(way), 'shelter' FROM planet_osm_polygon WHERE "tourism" IN ('camp_site','lean_to');

CREATE INDEX idx_pad_pois ON paddling_pois USING GIST(geom);

-- 4. RE-BUILD SEGMENTS & METRICS
DROP TABLE IF EXISTS paddling_segments;
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

-- 5. DETECT ATTRIBUTES (With Coordinate Safety)
UPDATE paddling_segments s SET is_official_route = TRUE FROM official_routes r 
WHERE ST_Intersects(s.geom, r.geom);

UPDATE paddling_segments s SET has_rapids = TRUE FROM rapids_features r 
WHERE ST_DWithin(s.geom, r.geom, 50);

UPDATE paddling_segments s SET has_poi_signal = TRUE FROM paddling_pois p 
WHERE ST_DWithin(s.geom, p.geom, 300);

-- LAND COVER FIX: Explicit Transform to 3857 to match OSM data
UPDATE paddling_segments s SET land_type = sub.type
FROM (
    SELECT s.seg_id, 
        CASE WHEN p.landuse IN ('forest','wood') THEN 'forest' 
             WHEN p.landuse IN ('farm','farmland','meadow') THEN 'field' 
             ELSE 'mixed' END as type,
        ROW_NUMBER() OVER(PARTITION BY s.seg_id ORDER BY ST_Distance(s.geom, p.way) ASC) as rn
    FROM paddling_segments s
    JOIN planet_osm_polygon p 
      ON ST_DWithin(s.geom, p.way, 100) -- Check 100m radius
      WHERE p.landuse IN ('forest','wood','farm','farmland','meadow')
) sub
WHERE s.seg_id = sub.seg_id AND sub.rn = 1;

-- 6. SCORING
ALTER TABLE paddling_segments ADD COLUMN feasibility_score int;
ALTER TABLE paddling_segments ADD COLUMN fun_score int;

UPDATE paddling_segments s SET environment = CASE 
    WHEN type IN ('lake_crossing', 'star_connector') THEN 'lake_route'
    WHEN type = 'portage_link' THEN 'portage'
    WHEN is_official_route THEN 'river_official'
    ELSE 'river' END;

-- Force Lake Land Type
UPDATE paddling_segments SET land_type = 'water' WHERE environment = 'lake_route';

-- Apply Scores
UPDATE paddling_segments s SET fun_score = (SELECT 
    CASE WHEN s.type = 'portage_link' THEN 10 WHEN s.environment = 'lake_route' THEN 60 
    ELSE (CASE WHEN s.sinuosity > 1.2 THEN 90 ELSE 70 END + 
          CASE WHEN s.land_type = 'forest' THEN 15 WHEN s.land_type = 'field' THEN -10 ELSE 0 END + 
          CASE WHEN s.has_rapids THEN 20 ELSE 0 END) END FROM scoring_config);
          
UPDATE paddling_segments s SET feasibility_score = (SELECT 
    CASE WHEN s.is_official_route THEN 100 WHEN s.type = 'portage_link' THEN 100 
    WHEN s.has_poi_signal THEN 90 ELSE 50 END FROM scoring_config);

-- 7. EXPORT STATS
DROP TABLE IF EXISTS paddling_areas_stats;
CREATE TABLE paddling_areas_stats AS
SELECT network_id, MAX(name) as area_name, ST_Union(geom) as geom
FROM paddling_segments GROUP BY network_id;