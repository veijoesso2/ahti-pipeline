-- =================================================================
-- 1. CONFIGURATION
-- =================================================================
DROP TABLE IF EXISTS scoring_config;
CREATE TEMPORARY TABLE scoring_config AS
SELECT 
    300 as poi_proximity_dist,
    50  as base_feasibility,       
    40  as poi_bonus,              
    1.2 as sinuosity_threshold,     
    
    -- Fun Scores
    90 as score_winding,
    70 as score_normal,
    40 as score_boring,
    60 as score_lake,
    10 as score_portage,
    
    -- Environmental Modifiers
    15  as bonus_forest,    -- Padding in woods is nice
    -10 as penalty_field,   -- Paddling in fields is windy/boring
    20  as bonus_rapids     -- Whitewater is fun!
;

-- =================================================================
-- 2. IMPORT CONTEXT DATA (Routes, Forests, Fields)
-- =================================================================

-- A. Official Canoe Routes (The "Golden Ticket")
-- We look for lines with explicit route tags
CREATE TEMPORARY TABLE official_routes AS
SELECT osm_id, way as geom 
FROM planet_osm_line 
WHERE route IN ('canoe', 'boat', 'ferry') 
   OR "waterway" IN ('rapids', 'waterfall');

CREATE INDEX idx_routes_geom ON official_routes USING GIST(geom);

-- B. Land Cover (Forests vs Fields)
CREATE TEMPORARY TABLE land_cover AS
SELECT 
    osm_id, 
    way as geom,
    CASE 
        WHEN "landuse" IN ('forest', 'wood') OR "natural" = 'wood' THEN 'forest'
        WHEN "landuse" IN ('farmland', 'farm', 'meadow', 'grass') THEN 'field'
        ELSE 'other'
    END as type
FROM planet_osm_polygon
WHERE "landuse" IN ('forest', 'wood', 'farmland', 'farm', 'meadow', 'grass') 
   OR "natural" = 'wood';

CREATE INDEX idx_land_cover ON land_cover USING GIST(geom);

-- =================================================================
-- 3. RE-BUILD SEGMENTS (Same as before, but keeping logic clean)
-- =================================================================
DROP TABLE IF EXISTS paddling_segments;

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

-- =================================================================
-- 4. CALCULATE BASE METRICS
-- =================================================================
ALTER TABLE paddling_segments ADD COLUMN length_m float;
ALTER TABLE paddling_segments ADD COLUMN sinuosity numeric;
ALTER TABLE paddling_segments ADD COLUMN environment text; 
ALTER TABLE paddling_segments ADD COLUMN has_poi_signal boolean DEFAULT FALSE;
ALTER TABLE paddling_segments ADD COLUMN is_official_route boolean DEFAULT FALSE;
ALTER TABLE paddling_segments ADD COLUMN land_type text DEFAULT 'mixed';

UPDATE paddling_segments SET length_m = ST_Length(geom);

UPDATE paddling_segments 
SET sinuosity = CASE 
    WHEN ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) < 1 THEN 3.0 
    ELSE ROUND((length_m::numeric / ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom))::numeric), 2)
END;

-- =================================================================
-- 5. ADVANCED DETECTION
-- =================================================================

-- A. Detect Official Routes (Spatial Match)
UPDATE paddling_segments s
SET is_official_route = TRUE
FROM official_routes r
WHERE ST_Intersects(s.geom, r.geom)
AND ST_Length(ST_Intersection(s.geom, r.geom)) > (s.length_m * 0.5);

-- B. Detect Land Cover (Forest vs Field)
-- This takes the centroid of the segment and checks what it's inside.
UPDATE paddling_segments s
SET land_type = lc.type
FROM land_cover lc
WHERE ST_Intersects(ST_PointOnSurface(s.geom), lc.geom);

-- C. Detect POI Signals (Existing Logic)
UPDATE paddling_segments s
SET has_poi_signal = TRUE
FROM paddling_pois p, scoring_config c
WHERE ST_DWithin(s.geom, p.geom, c.poi_proximity_dist);

-- D. Set Environment
UPDATE paddling_segments s
SET environment = CASE 
    WHEN type IN ('lake_crossing', 'star_connector') THEN 'lake_route'
    WHEN type = 'portage_link' THEN 'portage'
    WHEN is_official_route AND type != 'connector' THEN 'river_official'
    WHEN name IS NOT NULL AND name NOT LIKE 'Unnamed%' THEN 'river' 
    WHEN EXISTS (
        SELECT 1 FROM candidate_objects l 
        WHERE l.type = 'lake' AND ST_Area(l.geom) > 50000
        AND ST_Length(ST_Intersection(s.geom, l.geom)) > (s.length_m * 0.5)
    ) THEN 'lake_route'
    ELSE 'river'
END;

-- =================================================================
-- 6. SCORING PHASE 1: RAW SCORES
-- =================================================================
ALTER TABLE paddling_segments ADD COLUMN feasibility_score int;
ALTER TABLE paddling_segments ADD COLUMN fun_score int;

-- A. Fun Score (With Landuse Modifiers)
UPDATE paddling_segments s
SET fun_score = (
    SELECT 
        CASE 
            WHEN s.type = 'portage_link' THEN score_portage
            WHEN s.environment = 'lake_route' THEN score_lake
            
            -- Base River Score
            ELSE (
                CASE 
                    WHEN s.sinuosity > sinuosity_threshold THEN score_winding
                    ELSE score_normal 
                END
                -- Add Modifiers
                + (CASE WHEN s.land_type = 'forest' THEN bonus_forest ELSE 0 END)
                + (CASE WHEN s.land_type = 'field' THEN penalty_field ELSE 0 END)
                + (CASE WHEN s.is_official_route THEN bonus_rapids ELSE 0 END) -- Assume official routes have fun features
            )
        END
    FROM scoring_config
);

-- Clamp Scores to 0-100
UPDATE paddling_segments SET fun_score = 100 WHERE fun_score > 100;
UPDATE paddling_segments SET fun_score = 0 WHERE fun_score < 0;

-- B. Feasibility Score (Raw)
UPDATE paddling_segments s
SET feasibility_score = (
    SELECT 
        CASE 
            WHEN s.is_official_route THEN 100 -- Golden Ticket
            WHEN s.type = 'portage_link' THEN 100 
            WHEN s.has_poi_signal THEN LEAST(base_feasibility + poi_bonus, 100)
            WHEN s.environment = 'lake_route' THEN 80
            ELSE base_feasibility
        END
    FROM scoring_config
);

-- =================================================================
-- 7. SCORING PHASE 2: PROPAGATION (The "Bleed")
-- If I am low confidence, but my neighbor is 100%, boost me!
-- =================================================================

-- Create a temporary table of neighbor maximums
CREATE TEMPORARY TABLE neighbor_stats AS
SELECT 
    a.seg_id,
    MAX(b.feasibility_score) as max_neighbor_score
FROM paddling_segments a
JOIN paddling_segments b 
  ON ST_Touches(a.geom, b.geom) -- Find connected segments
  AND a.seg_id != b.seg_id
GROUP BY a.seg_id;

-- Apply the Boost (Blend 70% own score, 30% neighbor score)
UPDATE paddling_segments s
SET feasibility_score = GREATEST(
    s.feasibility_score, 
    (s.feasibility_score * 0.7 + ns.max_neighbor_score * 0.3)::int
)
FROM neighbor_stats ns
WHERE s.seg_id = ns.seg_id
AND s.feasibility_score < ns.max_neighbor_score;

-- =================================================================
-- 8. AREA STATS
-- =================================================================
DROP TABLE IF EXISTS paddling_areas_stats;
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