-- =====================================================================
-- AHTI PIPELINE - FAST DEBUG MODE (GEOMETRY & CONNECTIVITY ONLY)
-- Includes "Missing Lake Connector" Fix
-- =====================================================================

\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║  FAST PIPELINE: GEOMETRY ONLY (No Scoring/Land Cover)          ║'
\echo '╚════════════════════════════════════════════════════════════════╝'

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

-- =====================================================================
-- STEP 1: SCHEMA SETUP
-- =====================================================================
\echo '[1/7] Creating Schema...'
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

-- =====================================================================
-- STEP 2: EXTRACT CANDIDATES
-- =====================================================================
\echo '[2/7] Extracting Candidates...'

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

-- =====================================================================
-- STEP 3: CREATE PADDLING AREAS
-- =====================================================================
\echo '[3/7] Creating Base Layers...'
INSERT INTO paddling_areas (name, type, geom)
SELECT COALESCE(name, 'Unnamed River'), 'river', geom
FROM candidate_objects WHERE type = 'river';

-- =====================================================================
-- STEP 4A: HEAL RIVERS (Culverts)
-- =====================================================================
\echo '[4A/7] Healing Culverts...'
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

-- =====================================================================
-- STEP 4B: OBSTACLE BREAKER (Includes ST_Dump FIX)
-- =====================================================================
\echo '[4B/7] Processing Obstacles (The Fix)...'

-- 1. Identify Obstacles
CREATE TEMP TABLE obstacles AS
SELECT osm_id, 'dam' as type, ST_Buffer(way, 20) as geom 
FROM planet_osm_point 
WHERE (waterway IN ('dam', 'weir') OR "lock" = 'yes' OR "man_made" IN ('dyke', 'weir') OR "power" IN ('plant', 'generator'))
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)
UNION ALL
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

-- 2. Create Dam Portages (Negative Space)
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 
    880000 + ROW_NUMBER() OVER(),
    'system',
    'connector',
    'Dam Portage',
    ST_MakeLine(
        ST_StartPoint((ST_Dump(ST_Intersection(r.geom, o.geom))).geom),
        ST_EndPoint((ST_Dump(ST_Intersection(r.geom, o.geom))).geom)
    ),
    TRUE
FROM candidate_objects r
JOIN obstacles o ON ST_Intersects(r.geom, o.geom)
WHERE r.type = 'river'
AND ST_Length(ST_Intersection(r.geom, o.geom)) > 2;

-- 3. CUT RIVERS (THE FIX: Use ST_Dump to prevent MultiLineStrings)
-- This ensures the river mouths remain visible to Step 5
CREATE TEMP TABLE river_cuts_processed AS
SELECT 
    r.osm_id, r.source_type, r.type, r.name,
    (ST_Dump(ST_Difference(r.geom, ST_Union(o.geom)))).geom as geom,
    r.id as old_id
FROM candidate_objects r
JOIN obstacles o ON ST_Intersects(r.geom, o.geom)
WHERE r.type = 'river'
GROUP BY r.id, r.osm_id, r.source_type, r.type, r.name, r.geom;

DELETE FROM candidate_objects WHERE id IN (SELECT old_id FROM river_cuts_processed);

INSERT INTO candidate_objects (osm_id, source_type, type, name, geom)
SELECT osm_id, source_type, type, name, geom FROM river_cuts_processed
WHERE ST_Length(geom) > 1;

-- =====================================================================
-- STEP 5: CONNECTORS (Topology)
-- =====================================================================
\echo '[5/7] Creating Connectors...'

DELETE FROM candidate_objects WHERE is_virtual = TRUE AND type = 'connector' AND name != 'Dam Portage';

-- 5A. Portage Trails (Needed for logic, even if we don't score them)
CREATE TEMP TABLE portage_trails AS
SELECT way as geom FROM planet_osm_line
WHERE highway IN ('path', 'track', 'footway', 'cycleway', 'service', 'unclassified', 'residential', 'tertiary', 'secondary')
AND (access IS NULL OR access NOT IN ('private', 'no', 'customers'))
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);
CREATE INDEX idx_ptrails_geom ON portage_trails USING GIST(geom);

-- 5B. True Terminals
CREATE TEMP TABLE river_endpoints_raw AS
SELECT id, ST_StartPoint(ST_LineMerge(geom)) as pt FROM candidate_objects WHERE type = 'river'
UNION ALL
SELECT id, ST_EndPoint(ST_LineMerge(geom)) as pt FROM candidate_objects WHERE type = 'river';

CREATE TEMP TABLE true_terminals AS
SELECT a.pt FROM river_endpoints_raw a
WHERE NOT EXISTS (
    SELECT 1 FROM candidate_objects r WHERE r.type = 'river' AND r.id != a.id AND ST_DWithin(a.pt, r.geom, 0.5)
);

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

-- 5E. Portage Generation
CREATE TEMP TABLE potential_links AS
SELECT l1.id as from_lake, l2.id as to_lake, ST_ShortestLine(l1.geom, l2.geom) as geom, ST_Distance(l1.geom, l2.geom) as dist
FROM candidate_objects l1 JOIN candidate_objects l2 ON l1.id < l2.id
WHERE l1.type = 'lake' AND l2.type = 'lake' AND ST_DWithin(l1.geom, l2.geom, 1000);

CREATE TEMP TABLE valid_portages AS
SELECT *, CASE WHEN dist <= 500 THEN 'Portage' ELSE 'Road Portage' END as portage_type
FROM potential_links p
WHERE dist <= 500 OR (dist > 500 AND EXISTS (SELECT 1 FROM portage_trails t WHERE ST_Intersects(t.geom, ST_Buffer(p.geom, 50))));

INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 999900 + row_number() over(), 'system', 'connector', portage_type, geom, TRUE FROM valid_portages;

-- =====================================================================
-- STEP 6: NETWORK
-- =====================================================================
\echo '[6/7] Building Network...'

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

ALTER TABLE paddling_network ADD COLUMN network_id int;
WITH clusters AS (
    SELECT gid, ST_ClusterDBSCAN(geom, eps := 50, minpoints := 1) OVER () as cid
    FROM paddling_network
)
UPDATE paddling_network n SET network_id = c.cid + 1 FROM clusters c WHERE n.gid = c.gid;

-- =====================================================================
-- STEP 7: SEGMENTATION & DUMMY SCORING
-- =====================================================================
\echo '[7/7] Segmentation & Formatting...'

DROP TABLE IF EXISTS paddling_segments CASCADE;

CREATE TABLE paddling_segments AS
WITH segment_gen AS (
    SELECT gid as parent_id, osm_id, name, type, network_id, CASE WHEN ST_Length(geom) > 1000 THEN CEIL(ST_Length(geom) / 1000.0)::int ELSE 1 END as num_chunks, geom
    FROM paddling_network
)
SELECT ROW_NUMBER() OVER() as seg_id, parent_id, osm_id, name, type, network_id, ST_LineSubstring(geom, (n-1)::float/num_chunks, n::float/num_chunks) as geom
FROM segment_gen CROSS JOIN LATERAL generate_series(1, num_chunks) as n;

-- ADD DUMMY COLUMNS so existing export scripts don't crash
ALTER TABLE paddling_segments 
ADD COLUMN length_m float, 
ADD COLUMN environment text, 
ADD COLUMN is_official_route boolean DEFAULT FALSE, 
ADD COLUMN has_rapids boolean DEFAULT FALSE, 
ADD COLUMN fun_score int DEFAULT 50, 
ADD COLUMN feasibility_score int DEFAULT 50;

-- Basic Environment Mapping for Visuals
UPDATE paddling_segments SET length_m = ST_Length(geom);
UPDATE paddling_segments SET environment = 
    CASE 
        WHEN type = 'lake_crossing' THEN 'lake_route' 
        WHEN type = 'dam_crossing' THEN 'obstacle' 
        ELSE 'river' 
    END;
UPDATE paddling_segments SET environment = 'culvert' WHERE name = 'Culvert';

\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║  FAST PIPELINE COMPLETE! (Scoring Skipped)                     ║'
\echo '╚════════════════════════════════════════════════════════════════╝'