-- =====================================================================
-- AHTI PADDLE MAP - FAST TOPOLOGY & DEBUG v5 (Fix: osm_id error)
-- =====================================================================

-- 1. CLEANUP PREVIOUS RUNS
-- 1. CLEANUP PREVIOUS RUNS (Crucial for permanent debug tables)
DROP TABLE IF EXISTS candidate_objects CASCADE;
DROP TABLE IF EXISTS paddling_areas CASCADE;
DROP TABLE IF EXISTS paddling_segments CASCADE;
DROP TABLE IF EXISTS debug_obstacles CASCADE;
DROP TABLE IF EXISTS debug_raw_endpoints CASCADE;
DROP TABLE IF EXISTS debug_lake_candidates CASCADE;
DROP TABLE IF EXISTS debug_final_ports CASCADE; -- Added this missing drop
DROP TABLE IF EXISTS debug_missed_snaps CASCADE; -- Added this missing drop
-- 2. SETUP
DO $$
DECLARE
    v_min_x TEXT; v_max_x TEXT; v_min_y TEXT; v_max_y TEXT;
BEGIN
    v_min_x := current_setting('custom.min_x', true);
    v_max_x := current_setting('custom.max_x', true);
    v_min_y := current_setting('custom.min_y', true);
    v_max_y := current_setting('custom.max_y', true);
    IF v_min_x IS NULL THEN RAISE EXCEPTION 'Bounding Box not set'; END IF;
END $$;

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

-- FIX: Added osm_id to paddling_areas
CREATE TABLE paddling_areas (
    id SERIAL PRIMARY KEY, 
    osm_id BIGINT,
    name TEXT, 
    type TEXT, 
    geom GEOMETRY(Geometry, 3857)
);

-- 3. EXTRACTION
\echo '[1/5] Extracting Data...'
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

-- FIX: Insert osm_id here
INSERT INTO paddling_areas (osm_id, name, type, geom)
SELECT osm_id, COALESCE(name, 'Unnamed River'), 'river', geom 
FROM candidate_objects WHERE type = 'river';

-- 4. OBSTACLE HANDLING (With Debug Table)
\echo '[2/5] Handling Obstacles...'
CREATE TABLE debug_obstacles AS
SELECT osm_id, 'obstacle' as type, ST_Buffer(way, 20) as geom 
FROM planet_osm_point 
WHERE (waterway IN ('dam', 'weir') OR "lock" = 'yes' OR "man_made" IN ('dyke', 'weir') OR "power" IN ('plant', 'generator'))
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)
UNION ALL
SELECT osm_id, 'obstacle', ST_Buffer(way, 5) 
FROM planet_osm_polygon
WHERE (waterway IN ('dam', 'weir') OR "lock" = 'yes')
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);

-- Create Portages (Bridges)
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 
    880000 + ROW_NUMBER() OVER(), 'system', 'connector', 'Dam Portage',
    ST_MakeLine(
        ST_StartPoint((ST_Dump(ST_Intersection(r.geom, o.geom))).geom),
        ST_EndPoint((ST_Dump(ST_Intersection(r.geom, o.geom))).geom)
    ), TRUE
FROM candidate_objects r JOIN debug_obstacles o ON ST_Intersects(r.geom, o.geom)
WHERE r.type = 'river' AND ST_Length(ST_Intersection(r.geom, o.geom)) > 1.0;

-- Cut Rivers
CREATE TEMP TABLE river_cuts AS
SELECT r.id as old_id, (ST_Dump(ST_Difference(r.geom, ST_Union(o.geom)))).geom as geom, r.osm_id, r.name
FROM candidate_objects r JOIN debug_obstacles o ON ST_Intersects(r.geom, o.geom)
WHERE r.type = 'river' AND ST_Length(ST_Intersection(r.geom, o.geom)) > 1.0
GROUP BY r.id, r.geom, r.osm_id, r.name;

DELETE FROM candidate_objects WHERE id IN (SELECT old_id FROM river_cuts);
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom)
SELECT osm_id, 'system', 'river', name, geom FROM river_cuts WHERE ST_Length(geom) > 1;

-- 5. CONNECTIVITY DEBUGGING
\echo '[3/5] Debugging Connections...'
-- =====================================================================
-- STEP 5: CONNECTIVITY MESH
-- =====================================================================
\echo '[3/5] Generating Global Lake Mesh...'

-- Capture ALL Endpoints
CREATE TABLE debug_raw_endpoints AS
SELECT id as source_id, ST_StartPoint(geom) as geom FROM candidate_objects WHERE type = 'river'
UNION ALL
SELECT id as source_id, ST_EndPoint(geom) as geom FROM candidate_objects WHERE type = 'river';

-- Success Snap
CREATE TABLE debug_lake_candidates AS
SELECT ep.source_id, ep.geom
FROM debug_raw_endpoints ep
WHERE EXISTS (
    SELECT 1 FROM candidate_objects l 
    WHERE l.type = 'lake' AND ST_DWithin(ep.geom, l.geom, 300)
);

-- Fail Snap (For Autopsy)
CREATE TABLE debug_missed_snaps AS
SELECT ep.source_id, ep.geom, ROUND(ST_Distance(ep.geom, l.geom)::numeric, 1) as dist_to_lake
FROM debug_raw_endpoints ep
CROSS JOIN LATERAL (
    SELECT geom FROM candidate_objects l WHERE l.type = 'lake' 
    ORDER BY ep.geom <-> l.geom LIMIT 1
) l
WHERE NOT EXISTS (SELECT 1 FROM debug_lake_candidates c WHERE c.source_id = ep.source_id)
AND ST_Distance(ep.geom, l.geom) < 1000;

-- Create Final Ports
CREATE TABLE debug_final_ports AS
SELECT 
    row_number() over() as id, -- Explicit ID for the join
    c.source_id, 
    l.id as lake_id, 
    ST_ClosestPoint(l.geom, c.geom) as geom
FROM debug_lake_candidates c
JOIN candidate_objects l ON l.type = 'lake' AND ST_DWithin(c.geom, l.geom, 300);
-- =====================================================================
-- STEP 5D REPLACEMENT: AGNOSTIC LAKE MESH
-- =====================================================================
\echo '[3/5] Generating Agnostic Lake Mesh...'

-- Clear any failed attempts
DELETE FROM candidate_objects WHERE type = 'connector' AND name = 'Lake Route';

INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 
    9990 + row_number() over(), 
    'system', 
    'connector', 
    'Lake Route', 
    ST_MakeLine(p1.geom, p2.geom), 
    TRUE
FROM debug_final_ports p1 
JOIN debug_final_ports p2 ON p1.lake_id = p2.lake_id 
-- FIX: Use the unique port ID, not the source river ID.
-- This allows two terminals from the same river to connect across a bay.
WHERE p1.id < p2.id 
-- Safety: Don't connect terminals that are essentially the same spot (< 5m)
AND ST_Distance(p1.geom, p2.geom) > 5;

-- RE-PRUNE TRIANGLES (Optional but recommended for large lakes)
-- If A->B and B->C exist, and A->C is much longer, remove A->C.
-- This keeps the "Spider Web" from becoming a "Blue Blob".

-- 6. FINAL SEGMENTATION (Ensure all types are captured)
\echo '[4/5] Finalizing...'
DROP TABLE IF EXISTS paddling_segments CASCADE;
CREATE TABLE paddling_segments AS
SELECT 
    ROW_NUMBER() OVER() as seg_id, 
    osm_id, 
    name, 
    type, 
    CASE 
        WHEN name = 'Lake Route' THEN 'lake_route' 
        WHEN name = 'Dam Portage' THEN 'obstacle' 
        ELSE 'river' 
    END as environment,
    geom, 
    ST_Length(geom) as length_m
FROM (
    SELECT osm_id, name, 'river' as type, geom FROM paddling_areas
    UNION ALL
    -- Explicitly capture the connectors we just made
    SELECT osm_id, name, 'lake_crossing' as type, geom 
    FROM candidate_objects 
    WHERE type = 'connector' AND is_virtual = TRUE
) sub;

\echo '  ✓ Done'