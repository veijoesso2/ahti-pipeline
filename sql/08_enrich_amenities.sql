-- 1. IMPORT PADDLING AMENITIES (The "Signals")
-- We grab these from the raw OSM tables and add them to candidate_objects
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom)
SELECT 
    osm_id, 
    'osm_point', 
    CASE 
        WHEN "access" IN ('yes','permissive') THEN 'access_point'
        WHEN "leisure" = 'slipway' THEN 'slipway'
        WHEN "man_made" = 'pier' THEN 'pier'
        WHEN "tourism" IN ('camp_site', 'lean_to', 'picnic_site', 'wilderness_hut') THEN 'shelter'
        WHEN "waterway" = 'fuel' THEN 'service'
        ELSE 'amenity'
    END as type,
    COALESCE(name, 'Amenity'), 
    way as geom
FROM planet_osm_point
WHERE "leisure" IN ('slipway', 'marina')
   OR "man_made" = 'pier'
   OR "tourism" IN ('camp_site', 'lean_to', 'picnic_site', 'wilderness_hut')
   OR "waterway" IN ('access_point', 'fuel');

-- Also check polygons (some camps are drawn as areas)
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom)
SELECT 
    osm_id, 'osm_polygon', 'shelter', COALESCE(name, 'Camp Area'), ST_Centroid(way)
FROM planet_osm_polygon
WHERE "tourism" IN ('camp_site', 'lean_to', 'picnic_site');

-- =================================================================
-- 2. RE-RUN SEGMENTATION & SCORING (With Fixes)
-- =================================================================

DROP TABLE IF EXISTS paddling_segments;
DROP TABLE IF EXISTS paddling_areas_stats;
DROP TABLE IF EXISTS scoring_config;
DROP TABLE IF EXISTS paddling_pois;

-- A. CONFIG (Tweak weights here)
CREATE TEMPORARY TABLE scoring_config AS
SELECT 
    300 as poi_proximity_dist,      -- Increased radius to 300m (catches stuff on shore)
    50  as base_feasibility,       
    40  as poi_bonus,              
    1.2 as sinuosity_threshold,     
    1.05 as straight_threshold,     
    90 as score_winding_river,
    70 as score_normal_river,
    40 as score_straight_ditch,
    60 as score_lake_route,
    10 as score_portage             
;

-- B. INDEX THE NEW POIS
CREATE TEMPORARY TABLE paddling_pois AS
SELECT geom, type 
FROM candidate_objects 
WHERE type IN ('slipway', 'pier', 'shelter', 'access_point', 'service', 'amenity');

CREATE INDEX idx_pad_pois ON paddling_pois USING GIST(geom);

-- C. SEGMENTATION
CREATE TABLE paddling_segments AS
WITH segment_gen AS (
    SELECT 
        gid as parent_id, osm_id, name, type, network_id,
        CASE 
            WHEN ST_Length(geom) > 1000 THEN CEIL(ST_Length(geom) / 1000.0)::int
            ELSE 1 
        END as num_chunks,
        geom
    FROM paddling_network
)
SELECT 
    ROW_NUMBER() OVER() as seg_id,
    parent_id, osm_id, name, type, network_id,
    ST_LineSubstring(geom, (n-1)::float/num_chunks, n::float/num_chunks) as geom
FROM segment_gen
CROSS JOIN LATERAL generate_series(1, num_chunks) as n;

CREATE INDEX idx_pad_seg_geom ON paddling_segments USING GIST(geom);
CREATE INDEX idx_pad_seg_net ON paddling_segments(network_id);

-- D. CALCULATE METRICS
ALTER TABLE paddling_segments ADD COLUMN length_m float;
ALTER TABLE paddling_segments ADD COLUMN sinuosity numeric;
ALTER TABLE paddling_segments ADD COLUMN environment text; 
ALTER TABLE paddling_segments ADD COLUMN has_poi_signal boolean DEFAULT FALSE;

UPDATE paddling_segments SET length_m = ST_Length(geom);

UPDATE paddling_segments 
SET sinuosity = CASE 
    WHEN ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) < 1 THEN 3.0 
    ELSE ROUND((length_m::numeric / ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom))::numeric), 2)
END;

-- E. ENVIRONMENT LOGIC (FIXED)
-- Priority: 
-- 1. Virtual Lines (Crossings) -> Lake Route
-- 2. Portages -> Portage
-- 3. Named Rivers (e.g. Mankinvirta) -> River (Even if inside a lake!)
-- 4. Unnamed lines inside Lakes -> Lake Route
UPDATE paddling_segments s
SET environment = CASE 
    WHEN type IN ('lake_crossing', 'star_connector') THEN 'lake_route'
    WHEN type = 'portage_link' THEN 'portage'
    -- If it has a name and is not virtual, trust it is a river
    WHEN name IS NOT NULL AND name NOT LIKE 'Unnamed%' THEN 'river' 
    -- Only call it "lake_route" if it is anonymous AND inside a lake
    WHEN EXISTS (
        SELECT 1 FROM candidate_objects l 
        WHERE l.type = 'lake' AND ST_Area(l.geom) > 50000
        AND ST_Length(ST_Intersection(s.geom, l.geom)) > (s.length_m * 0.5)
    ) THEN 'lake_route'
    ELSE 'river'
END;

-- F. DETECT SIGNALS (Spatial Join)
UPDATE paddling_segments s
SET has_poi_signal = TRUE
FROM paddling_pois p, scoring_config c
WHERE ST_DWithin(s.geom, p.geom, c.poi_proximity_dist);

-- G. APPLY SCORES
ALTER TABLE paddling_segments ADD COLUMN feasibility_score int;
ALTER TABLE paddling_segments ADD COLUMN fun_score int;

UPDATE paddling_segments s
SET feasibility_score = (
    SELECT 
        CASE 
            WHEN s.type = 'portage_link' THEN 100 
            -- Bonus for POIs
            WHEN s.has_poi_signal THEN LEAST(base_feasibility + poi_bonus, 100)
            WHEN s.environment = 'lake_route' THEN 80
            ELSE base_feasibility
        END
    FROM scoring_config
);

UPDATE paddling_segments s
SET fun_score = (
    SELECT 
        CASE 
            WHEN s.type = 'portage_link' THEN score_portage
            WHEN s.environment = 'lake_route' THEN score_lake_route
            WHEN s.sinuosity > sinuosity_threshold THEN score_winding_river
            WHEN s.sinuosity < straight_threshold THEN score_straight_ditch
            ELSE score_normal_river
        END
    FROM scoring_config
);

-- H. AREA AGGREGATION
CREATE TABLE paddling_areas_stats AS
SELECT 
    network_id,
    (SELECT name FROM paddling_segments s2 
     WHERE s2.network_id = s.network_id AND name NOT LIKE 'Unnamed%' AND type = 'river' 
     ORDER BY length_m DESC LIMIT 1) as area_name,
    COUNT(*) as segment_count,
    ROUND(SUM(length_m)::numeric / 1000, 2) as total_km,
    ROUND(AVG(fun_score), 0) as avg_fun,
    MAX(feasibility_score) as max_feasibility,
    ST_Union(geom) as geom
FROM paddling_segments s
WHERE network_id IS NOT NULL
GROUP BY network_id;