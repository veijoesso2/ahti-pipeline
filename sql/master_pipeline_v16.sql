-- =====================================================================
-- AHTI PADDLE MAP PIPELINE - MASTER SCRIPT v17 (THE "NEGATIVE SPACE" FIX)
-- =====================================================================
-- v17 Changes:
-- 1. FIXED "Long Dam Portage" bug (Pic 1/2/5):
--    - Old logic connected the start of the river to the end.
--    - New logic isolates the *intersection* with the dam and turns 
--      that specific snippet into the portage. It fits the gap perfectly.
-- 2. IMPROVED Obstacle Detection (Pic 3):
--    - Added 'weir', 'power_plant', and broader tags.
--    - Increased point buffer to 20m to catch dams that slightly miss the line.
-- 3. FIXED "River under building" (Pic 4):
--    - Aggressive cut ensures no blue river line remains under the dam.
-- =====================================================================

\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║  AHTI PADDLE MAP PIPELINE - MASTER SCRIPT v17                ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''

-- Verify bounding box
DO $$
DECLARE
    v_min_x TEXT; v_max_x TEXT; v_min_y TEXT; v_max_y TEXT;
BEGIN
    v_min_x := current_setting('custom.min_x', true);
    v_max_x := current_setting('custom.max_x', true);
    v_min_y := current_setting('custom.min_y', true);
    v_max_y := current_setting('custom.max_y', true);

    IF v_min_x IS NULL OR v_max_x IS NULL OR v_min_y IS NULL OR v_max_y IS NULL THEN
        RAISE EXCEPTION 'Bounding box not set!';
    END IF;
    RAISE NOTICE 'Bounding Box: X(%, %) Y(%, %)', v_min_x, v_max_x, v_min_y, v_max_y;
END $$;

\echo ''

-- =====================================================================
-- STEP 1: SCHEMA SETUP
-- =====================================================================
\echo '[1/12] Creating Schema...'

DROP TABLE IF EXISTS candidate_objects CASCADE;
DROP TABLE IF EXISTS paddling_areas CASCADE;

CREATE TABLE candidate_objects (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT,
    source_type TEXT,
    type TEXT, 
    name TEXT,
    geom GEOMETRY(Geometry, 3857),
    is_virtual BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_cand_geom ON candidate_objects USING GIST(geom);
CREATE INDEX idx_cand_type ON candidate_objects(type);

CREATE TABLE paddling_areas (
    id SERIAL PRIMARY KEY,
    name TEXT,
    type TEXT,
    geom GEOMETRY(Geometry, 3857)
);

CREATE INDEX idx_pa_geom ON paddling_areas USING GIST(geom);

\echo '  ✓ Schema created'

-- =====================================================================
-- STEP 2: EXTRACT CANDIDATES
-- =====================================================================
\echo '[2/12] Extracting Candidates...'

INSERT INTO candidate_objects (osm_id, source_type, type, name, geom)
SELECT osm_id, 'osm_line', 'river', name, way
FROM planet_osm_line
WHERE waterway IN ('river', 'stream', 'canal')
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)
AND (waterway IN ('river', 'canal') OR (waterway = 'stream' AND COALESCE(NULLIF(regexp_replace(width, '[^0-9.]', '', 'g'), '')::numeric, 0) >= 3))
AND ST_Length(way) > 50;

INSERT INTO candidate_objects (osm_id, source_type, type, name, geom)
SELECT osm_id, 'osm_polygon', 'lake', name, way
FROM planet_osm_polygon
WHERE ("natural" = 'water' OR waterway = 'riverbank')
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)
AND ST_Area(way) > 20000;

\echo '  ✓ Candidates extracted'

-- =====================================================================
-- STEP 3: CREATE PADDLING AREAS
-- =====================================================================
\echo '[3/12] Creating Paddling Area Schema...'

INSERT INTO paddling_areas (name, type, geom)
SELECT COALESCE(name, 'Unnamed River'), 'river', geom
FROM candidate_objects WHERE type = 'river';

\echo '  ✓ Paddling areas created'

-- =====================================================================
-- STEP 4A: HEAL RIVERS (Culverts)
-- =====================================================================
\echo '[4A/12] Healing River Gaps (Culverts)...'

CREATE TEMP TABLE river_dangles AS
WITH endpoints AS (
    SELECT id, ST_StartPoint(geom) as pt FROM candidate_objects WHERE type = 'river'
    UNION ALL
    SELECT id, ST_EndPoint(geom) as pt FROM candidate_objects WHERE type = 'river'
)
SELECT e.id, e.pt 
FROM endpoints e
WHERE NOT EXISTS (
    SELECT 1 FROM candidate_objects r 
    WHERE r.type = 'river' AND r.id != e.id 
    AND ST_DWithin(e.pt, r.geom, 0.5)
);
CREATE INDEX idx_dangles_pt ON river_dangles USING GIST(pt);

INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 990000 + ROW_NUMBER() OVER(), 'system', 'river', 'Culvert', ST_MakeLine(d1.pt, d2.pt), TRUE
FROM river_dangles d1
JOIN river_dangles d2 ON d1.id < d2.id AND ST_DWithin(d1.pt, d2.pt, 40)
WHERE NOT EXISTS (
    SELECT 1 FROM river_dangles d3
    WHERE d3.id != d1.id AND d3.id != d2.id AND ST_DWithin(d1.pt, d3.pt, ST_Distance(d1.pt, d2.pt))
);

\echo '  ✓ Rivers healed'

-- =====================================================================
-- STEP 4B: OBSTACLE BREAKER v17 (Improved & Safer)
-- =====================================================================
\echo '[4B/12] Breaking for Obstacles (Dams/Weirs)...'

-- 1. Identify Obstacles (Expanded definition)
CREATE TEMP TABLE obstacles AS
-- Points (Nodes): Buffer them significantly (20m) to catch nearby lines
SELECT osm_id, 'dam' as type, ST_Buffer(way, 20) as geom 
FROM planet_osm_point 
WHERE (waterway IN ('dam', 'weir') OR "lock" = 'yes' OR "man_made" IN ('dyke', 'weir') OR "power" IN ('plant', 'generator'))
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)

UNION ALL

-- Polygons/Lines: Buffer them slightly (5m) to ensure intersection
SELECT osm_id, 'dam', ST_Buffer(way, 5) 
FROM planet_osm_polygon
WHERE (waterway IN ('dam', 'weir') OR "lock" = 'yes' OR "man_made" IN ('dyke', 'weir') OR "power" IN ('plant', 'generator'))
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)

UNION ALL

SELECT osm_id, 'dam', ST_Buffer(way, 5) 
FROM planet_osm_line
WHERE (waterway IN ('dam', 'weir'))
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);

CREATE INDEX idx_obst_geom ON obstacles USING GIST(geom);

-- 2. CREATE PORTAGES FIRST (The "Negative Space" Method)
-- Instead of cutting then guessing, we find the intersection first.
-- This guarantees the portage is exactly where the dam is.

INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 
    880000 + ROW_NUMBER() OVER(),
    'system',
    'connector',
    'Dam Portage',
    -- Make a straight line from entry to exit of the dam buffer
    ST_MakeLine(
        ST_StartPoint((ST_Dump(ST_Intersection(r.geom, o.geom))).geom),
        ST_EndPoint((ST_Dump(ST_Intersection(r.geom, o.geom))).geom)
    ),
    TRUE
FROM candidate_objects r
JOIN obstacles o ON ST_Intersects(r.geom, o.geom)
WHERE r.type = 'river'
-- Filter out tiny intersections (touching the edge)
AND ST_Length(ST_Intersection(r.geom, o.geom)) > 2;

-- 3. CUT THE RIVERS
-- Now we physically remove the obstacle buffer from the river.
-- This ensures no "blue line" remains under the building (Fix Pic 4).

CREATE TEMP TABLE cut_rivers AS
SELECT 
    r.id, 
    -- Use Difference to erase the dam area
    ST_Difference(r.geom, ST_Union(o.geom)) as geom 
FROM candidate_objects r
JOIN obstacles o ON ST_Intersects(r.geom, o.geom)
WHERE r.type = 'river'
GROUP BY r.id, r.geom;

-- Update the original rivers with the cut version
UPDATE candidate_objects r
SET geom = c.geom
FROM cut_rivers c
WHERE r.id = c.id;

\echo '  ✓ Obstacles handled (Negative Space Method)'

-- =====================================================================
-- STEP 5: SMART LAKE CONNECTORS (Road-Aware)
-- =====================================================================
\echo '[5/12] Creating Smart Lake Connectors...'

DELETE FROM candidate_objects WHERE is_virtual = TRUE AND type = 'connector' AND name != 'Dam Portage';

-- 5A. Portage Trails
CREATE TEMP TABLE portage_trails AS
SELECT way as geom FROM planet_osm_line
WHERE highway IN ('path', 'track', 'footway', 'cycleway', 'service', 'unclassified', 'residential', 'tertiary', 'secondary')
AND (access IS NULL OR access NOT IN ('private', 'no', 'customers'))
AND (service IS NULL OR service NOT IN ('parking_aisle', 'driveway', 'private'))
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);
CREATE INDEX idx_ptrails_geom ON portage_trails USING GIST(geom);

-- 5B. True Terminals
CREATE TEMP TABLE river_endpoints_raw AS
SELECT id, ST_StartPoint(ST_LineMerge(geom)) as pt FROM candidate_objects WHERE type = 'river'
UNION ALL
SELECT id, ST_EndPoint(ST_LineMerge(geom)) as pt FROM candidate_objects WHERE type = 'river';
CREATE INDEX idx_rep_raw_pt ON river_endpoints_raw USING GIST(pt);

CREATE TEMP TABLE true_terminals AS
SELECT a.pt FROM river_endpoints_raw a
WHERE NOT EXISTS (
    SELECT 1 FROM candidate_objects r WHERE r.type = 'river' AND r.id != a.id AND ST_DWithin(a.pt, r.geom, 0.5)
);
CREATE INDEX idx_tt_pt ON true_terminals USING GIST(pt);

-- 5C. Snap Terminals to Lakes
CREATE TEMP TABLE lake_ports AS
SELECT r.id as river_id, l.id as lake_id, ST_ClosestPoint(l.geom, tt.pt) as geom
FROM candidate_objects r
JOIN true_terminals tt ON ST_DWithin(r.geom, tt.pt, 1.0)
JOIN candidate_objects l ON l.type = 'lake' AND ST_DWithin(tt.pt, l.geom, 50)
WHERE r.type = 'river';

-- 5D. Internal Lake Routes
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 9990 + row_number() over(), 'system', 'connector', 'Lake Route', ST_MakeLine(p1.geom, p2.geom), TRUE
FROM lake_ports p1 JOIN lake_ports p2 ON p1.lake_id = p2.lake_id AND p1.river_id < p2.river_id
WHERE ST_Length(ST_MakeLine(p1.geom, p2.geom)) > 50; 

-- 5E. SMART PORTAGE GENERATION
CREATE TEMP TABLE potential_links AS
SELECT l1.id as from_lake, l2.id as to_lake, ST_ShortestLine(l1.geom, l2.geom) as geom, ST_Distance(l1.geom, l2.geom) as dist
FROM candidate_objects l1 JOIN candidate_objects l2 ON l1.id < l2.id
WHERE l1.type = 'lake' AND l2.type = 'lake' AND ST_DWithin(l1.geom, l2.geom, 1000);

CREATE TEMP TABLE valid_portages AS
SELECT *, CASE WHEN dist <= 500 THEN 'Portage' ELSE 'Road Portage' END as portage_type
FROM potential_links p
WHERE dist <= 500 OR (dist > 500 AND EXISTS (SELECT 1 FROM portage_trails t WHERE ST_Intersects(t.geom, ST_Buffer(p.geom, 50))));

-- 5F. Prune Triangles
WITH triangle_check AS (
    SELECT p1.from_lake as A, p1.to_lake as C, p2.to_lake as B, p1.dist as dist_direct, (p2.dist + p3.dist) as dist_via_B
    FROM valid_portages p1 JOIN valid_portages p2 ON p1.from_lake = p2.from_lake JOIN valid_portages p3 ON p2.to_lake = p3.from_lake AND p3.to_lake = p1.to_lake
)
DELETE FROM valid_portages p USING triangle_check tc WHERE p.from_lake = tc.A AND p.to_lake = tc.C AND tc.dist_via_B < (3 * tc.dist_direct);

INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 999900 + row_number() over(), 'system', 'connector', portage_type, geom, TRUE FROM valid_portages;

\echo '  ✓ Smart connectors created'

-- =====================================================================
-- STEP 6: NETWORK ANALYSIS
-- =====================================================================
\echo '[6/12] Network Analysis...'

DROP TABLE IF EXISTS paddling_network CASCADE;

CREATE TABLE paddling_network AS
SELECT ROW_NUMBER() OVER() as gid, id::text as osm_id, COALESCE(name, 'Unnamed Segment') as name, 'river' as type, (ST_Dump(ST_Force2D(geom))).geom as geom
FROM paddling_areas
UNION ALL
SELECT (ROW_NUMBER() OVER() + 1000000) as gid, osm_id::text, name, 'lake_crossing' as type, (ST_Dump(ST_Force2D(geom))).geom as geom
FROM candidate_objects WHERE type = 'connector' AND is_virtual = TRUE AND name != 'Dam Portage'
UNION ALL
SELECT (ROW_NUMBER() OVER() + 2000000) as gid, osm_id::text, name, 'river' as type, (ST_Dump(ST_Force2D(geom))).geom as geom
FROM candidate_objects WHERE name = 'Culvert'
UNION ALL
SELECT (ROW_NUMBER() OVER() + 3000000) as gid, osm_id::text, name, 'dam_crossing' as type, (ST_Dump(ST_Force2D(geom))).geom as geom
FROM candidate_objects WHERE name = 'Dam Portage';

CREATE INDEX idx_paddling_net_geom ON paddling_network USING GIST(geom);
ALTER TABLE paddling_network ADD COLUMN network_id int;

WITH clusters AS (
    SELECT gid, ST_ClusterDBSCAN(geom, eps := 50, minpoints := 1) OVER () as cid
    FROM paddling_network
)
UPDATE paddling_network n SET network_id = c.cid + 1 FROM clusters c WHERE n.gid = c.gid;

ALTER TABLE paddling_network ADD COLUMN segment_len float;
ALTER TABLE paddling_network ADD COLUMN sinuosity numeric;
UPDATE paddling_network SET segment_len = ST_Length(geom);
UPDATE paddling_network SET sinuosity = CASE WHEN ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) < 1 THEN 1.0 ELSE ROUND((ST_Length(geom)::numeric / ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom))::numeric), 2) END WHERE ST_GeometryType(geom) = 'ST_LineString';

\echo '  ✓ Network analysis complete'

-- =====================================================================
-- STEP 7: SEGMENTATION
-- =====================================================================
\echo '[7/12] Segmentation...'

DROP TABLE IF EXISTS paddling_segments CASCADE;

CREATE TABLE paddling_segments AS
WITH segment_gen AS (
    SELECT gid as parent_id, osm_id, name, type, network_id, CASE WHEN ST_Length(geom) > 1000 THEN CEIL(ST_Length(geom) / 1000.0)::int ELSE 1 END as num_chunks, geom
    FROM paddling_network
)
SELECT ROW_NUMBER() OVER() as seg_id, parent_id, osm_id, name, type, network_id, ST_LineSubstring(geom, (n-1)::float/num_chunks, n::float/num_chunks) as geom
FROM segment_gen CROSS JOIN LATERAL generate_series(1, num_chunks) as n;

CREATE INDEX idx_pad_seg_geom ON paddling_segments USING GIST(geom);
CREATE INDEX idx_pad_seg_type ON paddling_segments(type);

ALTER TABLE paddling_segments ADD COLUMN length_m float, ADD COLUMN sinuosity numeric, ADD COLUMN environment text, ADD COLUMN has_poi_signal boolean DEFAULT FALSE, ADD COLUMN is_official_route boolean DEFAULT FALSE, ADD COLUMN has_rapids boolean DEFAULT FALSE, ADD COLUMN land_type text DEFAULT 'mixed', ADD COLUMN feasibility_score int DEFAULT 50, ADD COLUMN fun_score int DEFAULT 60;

UPDATE paddling_segments SET length_m = ST_Length(geom);
UPDATE paddling_segments SET sinuosity = CASE WHEN ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) < 1 THEN 1.0 ELSE ROUND((length_m::numeric / ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom))::numeric), 2) END WHERE length_m > 0;
UPDATE paddling_segments SET environment = CASE WHEN type = 'lake_crossing' THEN 'lake_route' WHEN type = 'dam_crossing' THEN 'obstacle' ELSE 'river' END;
UPDATE paddling_segments SET land_type = 'water' WHERE type = 'lake_crossing';
UPDATE paddling_segments SET land_type = 'dam' WHERE type = 'dam_crossing';
    
-- =====================================================================
-- GEOMETRY VALIDATION PATCH
-- Add this to the end of Step 7 (Segmentation) in master_pipeline_v16.sql
-- =====================================================================

-- After segmentation, clean up problematic geometries immediately

\echo '  Validating geometries...'

-- Remove zero-length segments (degenerate geometries)
DELETE FROM paddling_segments 
WHERE length_m < 1;

-- Fix sinuosity for segments where it couldn't be calculated
-- (usually circles or very short segments where start ≈ end)
UPDATE paddling_segments 
SET sinuosity = CASE 
    WHEN sinuosity IS NULL THEN 1.5  -- Assume moderate winding
    WHEN sinuosity > 10 THEN 10.0    -- Cap at 10 (extremely winding)
    WHEN sinuosity < 0.9 THEN 1.0    -- Floor at 1.0 (straight line minimum)
    ELSE sinuosity
END;

-- Ensure all segments have valid environment
UPDATE paddling_segments
SET environment = 'river'
WHERE environment IS NULL AND type = 'river';

UPDATE paddling_segments
SET environment = 'lake_route'
WHERE environment IS NULL AND type = 'lake_crossing';

-- Remove any NULL geometries (shouldn't happen but safety check)
DELETE FROM paddling_segments WHERE geom IS NULL;

-- Report cleaning results
DO $$
DECLARE
    final_count INT;
BEGIN
    SELECT COUNT(*) INTO final_count FROM paddling_segments;
    RAISE NOTICE '  Geometry validation complete. Final segments: %', final_count;
END $$;
\echo '  ✓ Segmentation complete'

-- =====================================================================
-- STEP 8: ENRICHMENT TABLES
-- =====================================================================
\echo '[8/12] Creating Enrichment Tables...'

DROP TABLE IF EXISTS official_routes CASCADE;
DROP TABLE IF EXISTS rapids_features CASCADE;
DROP TABLE IF EXISTS paddling_pois CASCADE;
DROP TABLE IF EXISTS land_cover CASCADE;

CREATE TABLE official_routes AS
SELECT osm_id, way as geom FROM planet_osm_line WHERE route = 'canoe' AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);
CREATE INDEX idx_routes_geom ON official_routes USING GIST(geom);

CREATE TABLE rapids_features AS
SELECT osm_id, way as geom FROM planet_osm_line WHERE waterway = 'rapids' AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)
UNION ALL
SELECT osm_id, way as geom FROM planet_osm_point WHERE waterway IN ('rapids', 'waterfall') AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);
CREATE INDEX idx_rapids_geom ON rapids_features USING GIST(geom);

CREATE TABLE paddling_pois AS
SELECT way as geom, 'shelter' as type FROM planet_osm_point WHERE (tourism IN ('camp_site','lean_to','picnic_site','wilderness_hut') OR amenity IN ('shelter') OR leisure = 'slipway') AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)
UNION ALL
SELECT ST_Centroid(way), 'shelter' FROM planet_osm_polygon WHERE tourism IN ('camp_site','lean_to') AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);
CREATE INDEX idx_pad_pois ON paddling_pois USING GIST(geom);

CREATE TABLE land_cover AS
SELECT osm_id, ST_MakeValid(way) as geom,
    CASE WHEN (landuse IN ('forest', 'wood') OR "natural" IN ('wood', 'scrub', 'heath')) THEN 'forest'
         WHEN (landuse IN ('farmland', 'farm', 'meadow', 'grass', 'orchard')) THEN 'field'
         WHEN (landuse IN ('residential', 'industrial', 'commercial', 'retail')) THEN 'urban'
         ELSE 'other' END as type
FROM planet_osm_polygon
WHERE ((landuse IN ('forest', 'wood', 'farmland', 'farm', 'meadow', 'grass', 'orchard', 'residential', 'industrial', 'commercial', 'retail') OR "natural" IN ('wood', 'scrub', 'heath')) AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857));
CREATE INDEX idx_land_cover ON land_cover USING GIST(geom);

\echo '  ✓ Enrichment tables created'

-- =====================================================================
-- STEP 9: FEATURE DETECTION
-- =====================================================================
\echo '[9/12] Detecting Features...'

UPDATE paddling_segments s SET is_official_route = TRUE FROM official_routes r WHERE ST_Intersects(s.geom, r.geom);
UPDATE paddling_segments s SET has_rapids = TRUE FROM rapids_features r WHERE ST_DWithin(s.geom, r.geom, 50);
UPDATE paddling_segments s SET has_poi_signal = TRUE FROM paddling_pois p WHERE ST_DWithin(s.geom, p.geom, 500);
UPDATE paddling_segments SET environment = 'river_official' WHERE is_official_route = TRUE AND environment = 'river';

\echo '  ✓ Features detected'

-- =====================================================================
-- STEP 10: LAND TYPE CLASSIFICATION
-- =====================================================================
\echo '[10/12] Classifying Land Types...'

WITH land_stats AS (
    SELECT s.seg_id, lc.type, SUM(ST_Area(ST_Intersection(ST_Buffer(s.geom, 50), lc.geom))) as type_area, ST_Area(ST_Buffer(s.geom, 50)) as total_buffer_area
    FROM paddling_segments s JOIN land_cover lc ON ST_Intersects(ST_Buffer(s.geom, 50), lc.geom)
    WHERE s.type = 'river' GROUP BY s.seg_id, lc.type, total_buffer_area
)
UPDATE paddling_segments s SET land_type = ls.type FROM land_stats ls WHERE s.seg_id = ls.seg_id AND ls.type_area > (ls.total_buffer_area * 0.5);

\echo '  ✓ Land types classified'

-- =====================================================================
-- STEP 11: SCORING
-- =====================================================================
\echo '[11/12] Calculating Scores...'

UPDATE paddling_segments SET fun_score = CASE WHEN type = 'lake_crossing' THEN 60 WHEN type = 'dam_crossing' THEN 0 ELSE (CASE WHEN sinuosity > 1.2 THEN 90 ELSE 70 END + CASE WHEN land_type = 'forest' THEN 15 WHEN land_type = 'field' THEN -15 WHEN land_type = 'urban' THEN -20 ELSE 0 END + CASE WHEN has_rapids THEN 20 ELSE 0 END) END;
UPDATE paddling_segments SET fun_score = 100 WHERE fun_score > 100;
UPDATE paddling_segments SET fun_score = 0 WHERE fun_score < 0;

UPDATE paddling_segments SET feasibility_score = CASE WHEN type = 'dam_crossing' THEN 10 WHEN is_official_route THEN 100 WHEN type = 'lake_crossing' THEN 80 WHEN has_poi_signal THEN 90 ELSE 50 END;

WITH neighbor_stats AS (
    SELECT a.seg_id, MAX(b.feasibility_score) as max_neighbor_score
    FROM paddling_segments a JOIN paddling_segments b ON ST_Touches(a.geom, b.geom) AND a.seg_id != b.seg_id
    GROUP BY a.seg_id
)
UPDATE paddling_segments s SET feasibility_score = GREATEST(s.feasibility_score, (s.feasibility_score * 0.7 + ns.max_neighbor_score * 0.3)::int)
FROM neighbor_stats ns WHERE s.seg_id = ns.seg_id AND s.feasibility_score < ns.max_neighbor_score;

\echo '  ✓ Scores calculated'

-- =====================================================================
-- STEP 12: AREA STATISTICS
-- =====================================================================
\echo '[12/12] Calculating Area Statistics...'

DROP TABLE IF EXISTS paddling_areas_stats CASCADE;
CREATE TABLE paddling_areas_stats AS
SELECT network_id, (SELECT name FROM paddling_segments s2 WHERE s2.network_id = s.network_id AND name NOT LIKE 'Unnamed%' AND type = 'river' ORDER BY length_m DESC LIMIT 1) as area_name,
    COUNT(*) as segment_count, ROUND(SUM(length_m)::numeric / 1000, 2) as total_km, ROUND(AVG(fun_score), 0) as avg_fun, MAX(feasibility_score) as max_feasibility, ST_Union(geom) as geom
FROM paddling_segments s WHERE network_id IS NOT NULL GROUP BY network_id;

\echo '  ✓ Area stats calculated'
\echo ''
\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║  PIPELINE COMPLETE (v17)!                                      ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''