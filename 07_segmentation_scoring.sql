DROP TABLE IF EXISTS paddling_segments;
DROP TABLE IF EXISTS paddling_areas_stats;

-- =================================================================
-- 1. SCORING CONFIGURATION (THE CONTROL PANEL)
-- Update these weights to tune your map later!
-- =================================================================
CREATE TEMPORARY TABLE scoring_config AS
SELECT 
    -- FEASIBILITY (0-100)
    200 as poi_proximity_dist,      -- Look for slipways/shelters within X meters
    50  as base_feasibility,        -- Default confidence that a river is paddleable
    40  as poi_bonus,               -- Bonus if near a paddling POI
    -30 as obstacle_penalty,        -- Penalty for "intermittent" or "tunnel" (if data exists)

    -- FUN FACTOR (0-100)
    1.2 as sinuosity_threshold,     -- Above this, it's "Winding"
    1.05 as straight_threshold,     -- Below this, it's "Boring"
    
    -- Fun Scores
    90 as score_winding_river,
    70 as score_normal_river,
    40 as score_straight_ditch,
    60 as score_lake_route,
    10 as score_portage             -- Hiking with a canoe is rarely "fun"
;

-- =================================================================
-- 2. PREPARE POI SIGNALS
-- Find indicators that imply "Humans paddle here"
-- =================================================================
-- We assume these tags exist in candidate_objects or similar. 
-- If you need to import them, ensure your osm extraction grabs:
-- waterway=access_point, leisure=slipway, man_made=pier, tourism=camp_site
CREATE TEMPORARY TABLE paddling_pois AS
SELECT geom, type 
FROM candidate_objects 
WHERE type IN ('slipway', 'pier', 'camp_site', 'access_point', 'shelter');

CREATE INDEX idx_pad_pois ON paddling_pois USING GIST(geom);

-- =================================================================
-- 3. UNIFORM SEGMENTATION (The Chopper)
-- =================================================================
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
-- 4. CALCULATE METRICS
-- =================================================================
ALTER TABLE paddling_segments ADD COLUMN length_m float;
ALTER TABLE paddling_segments ADD COLUMN sinuosity numeric;
ALTER TABLE paddling_segments ADD COLUMN environment text; 
ALTER TABLE paddling_segments ADD COLUMN has_poi_signal boolean DEFAULT FALSE;

-- A. Geometry Stats
UPDATE paddling_segments SET length_m = ST_Length(geom);

UPDATE paddling_segments 
SET sinuosity = CASE 
    WHEN ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) < 1 THEN 3.0 
    ELSE ROUND((length_m::numeric / ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom))::numeric), 2)
END;

-- B. Environment Detection
UPDATE paddling_segments s
SET environment = CASE 
    WHEN EXISTS (
        SELECT 1 FROM candidate_objects l 
        WHERE l.type = 'lake' AND ST_Area(l.geom) > 50000
        AND ST_Length(ST_Intersection(s.geom, l.geom)) > (s.length_m * 0.5)
    ) THEN 'lake_route'
    WHEN type IN ('lake_crossing', 'star_connector') THEN 'lake_route'
    WHEN type = 'portage_link' THEN 'portage'
    ELSE 'river'
END;

-- C. Detect POI Signals (Spatial Join)
UPDATE paddling_segments s
SET has_poi_signal = TRUE
FROM paddling_pois p, scoring_config c
WHERE ST_DWithin(s.geom, p.geom, c.poi_proximity_dist);

-- =================================================================
-- 5. APPLY SCORING (THE LOGIC ENGINE)
-- =================================================================
ALTER TABLE paddling_segments ADD COLUMN feasibility_score int;
ALTER TABLE paddling_segments ADD COLUMN fun_score int;

-- A. Feasibility (Can I paddle?)
UPDATE paddling_segments s
SET feasibility_score = (
    SELECT 
        CASE 
            -- Portages are explicitly meant for travel
            WHEN s.type = 'portage_link' THEN 100 
            -- POI nearby means high confidence
            WHEN s.has_poi_signal THEN LEAST(base_feasibility + poi_bonus, 100)
            -- Lakes are usually navigable
            WHEN s.environment = 'lake_route' THEN 80
            -- Base river confidence
            ELSE base_feasibility
        END
    FROM scoring_config
);

-- B. Fun Factor (Do I want to paddle?)
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

-- =================================================================
-- 6. AREA AGGREGATION
-- =================================================================
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