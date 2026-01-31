-- =====================================================================
-- AHTI PADDLE MAP PIPELINE - MASTER SCRIPT (FIXED)
-- =====================================================================
-- Fixed critical bugs in segmentation and classification
-- Version: 2.0
-- =====================================================================

\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║  AHTI PADDLE MAP PIPELINE - MASTER SCRIPT v2.0               ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''

-- Verify bounding box is set
DO $$
DECLARE
    v_min_x TEXT;
    v_max_x TEXT;
    v_min_y TEXT;
    v_max_y TEXT;
BEGIN
    v_min_x := current_setting('custom.min_x', true);
    v_max_x := current_setting('custom.max_x', true);
    v_min_y := current_setting('custom.min_y', true);
    v_max_y := current_setting('custom.max_y', true);

    IF v_min_x IS NULL OR v_max_x IS NULL OR v_min_y IS NULL OR v_max_y IS NULL THEN
        RAISE EXCEPTION 'Bounding box not set! Variables: min_x=%, max_x=%, min_y=%, max_y=%', 
            v_min_x, v_max_x, v_min_y, v_max_y;
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
-- STEP 2: EXTRACT CANDIDATES (ROBUST VERSION)
-- =====================================================================
\echo '[2/12] Extracting Candidates...'

-- Extract Rivers
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom)
SELECT 
    osm_id,
    'osm_line',
    'river',
    name,
    way
FROM planet_osm_line
WHERE waterway IN ('river', 'stream', 'canal')
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)
AND (
    waterway IN ('river', 'canal')
    OR (
        waterway = 'stream' 
        AND COALESCE(
            NULLIF(regexp_replace(width, '[^0-9.]', '', 'g'), '')::numeric, 
            0
        ) >= 3
    )
)
AND ST_Length(way) > 50;

-- Extract Lakes
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom)
SELECT 
    osm_id,
    'osm_polygon',
    'lake',
    name,
    way
FROM planet_osm_polygon
WHERE ("natural" = 'water' OR waterway = 'riverbank')
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)
AND ST_Area(way) > 10000;

-- Report counts
DO $$
BEGIN
    RAISE NOTICE '  Rivers extracted: %', (SELECT COUNT(*) FROM candidate_objects WHERE type='river');
    RAISE NOTICE '  Lakes extracted: %', (SELECT COUNT(*) FROM candidate_objects WHERE type='lake');
END $$;

\echo '  ✓ Candidates extracted'

-- =====================================================================
-- STEP 3: CREATE PADDLING AREAS
-- =====================================================================
\echo '[3/12] Creating Paddling Area Schema...'

INSERT INTO paddling_areas (name, type, geom)
SELECT 
    COALESCE(name, 'Unnamed River'),
    'river',
    geom
FROM candidate_objects
WHERE type = 'river';

\echo '  ✓ Paddling areas created'

-- =====================================================================
-- STEP 4: AGGREGATE RIVERS
-- =====================================================================
\echo '[4/12] Aggregating Rivers...'
\echo '  ✓ Rivers aggregated (skipped for small area)'

-- =====================================================================
-- STEP 5: V6 TOPOLOGY (River-First Strategy)
-- =====================================================================
\echo '[5/12] Creating v6 Topology (River-First, Aggressive Pruning)...'

DELETE FROM candidate_objects WHERE is_virtual = TRUE;

-- 1. CLASSIFY NARROW LAKES (The "20m Width" Rule)
-- We add a flag to identify lakes that are actually just rivers.
ALTER TABLE candidate_objects ADD COLUMN IF NOT EXISTS is_narrow boolean DEFAULT FALSE;

-- If shrinking the lake by 10m makes it vanish, it's narrower than 20m everywhere.
-- We mark these as "Narrow" so we don't build star networks on them.
UPDATE candidate_objects 
SET is_narrow = TRUE 
WHERE type = 'lake' 
AND ST_IsEmpty(ST_Buffer(geom, -10)); -- 10m shrink = 20m width check

\echo '    - Identified narrow lakes (skipped for routing)'
-- =====================================================================
-- STEP 5: V6 TOPOLOGY (FIXED)
-- =====================================================================

-- ... (Keep Step 1 "CLASSIFY NARROW LAKES" as it was) ...

-- 2. IDENTIFY PORTS (Exact Endpoints)
-- FIXED: Added 'lake_geom' to the selection list so Step 3 can use it.
DROP TABLE IF EXISTS lake_ports;
CREATE TEMP TABLE lake_ports AS
WITH raw_ends AS (
    SELECT 
        r.id as river_id,
        l.id as lake_id,
        l.geom as lake_geom, -- <--- THIS WAS MISSING
        l.is_narrow,
        CASE 
            WHEN ST_DWithin(ST_StartPoint(r.geom), l.geom, 50) THEN ST_StartPoint(r.geom)
            ELSE ST_EndPoint(r.geom)
        END as port_pt
    FROM candidate_objects r
    JOIN candidate_objects l 
        ON r.type = 'river' AND l.type = 'lake'
        AND ST_DWithin(r.geom, l.geom, 50)
)
SELECT * FROM raw_ends WHERE port_pt IS NOT NULL;

-- 3. GENERATE ROUTES (Wide Lakes Only)
WITH lake_stats AS (
    SELECT 
        lake_id, 
        count(*) as port_count, 
        -- Now 'lake_geom' exists and this will work:
        ST_PointOnSurface(MAX(ST_Buffer(lake_geom, -10))) as hub_pt
    FROM lake_ports
    WHERE is_narrow = FALSE 
    GROUP BY lake_id
),
routes AS (
    -- Case A: Simple Wide Lakes (2 Ports) -> Direct
    SELECT 
        p1.lake_id,
        ST_MakeLine(p1.port_pt, p2.port_pt) as geom
    FROM lake_ports p1
    JOIN lake_ports p2 ON p1.lake_id = p2.lake_id AND p1.river_id < p2.river_id
    JOIN lake_stats s ON p1.lake_id = s.lake_id
    WHERE s.port_count = 2
    
    UNION ALL
    
    -- Case B: Complex Wide Lakes (3+ Ports) -> Hub
    SELECT 
        p.lake_id,
        ST_MakeLine(p.port_pt, s.hub_pt) as geom
    FROM lake_ports p
    JOIN lake_stats s ON p.lake_id = s.lake_id
    WHERE s.port_count > 2
)
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 900000 + row_number() over(), 'system', 'connector', 'Lake Route', geom, TRUE
FROM routes;

-- ... (Continue with Step 4 "AGGRESSIVE SHADOW REMOVAL" as before) ...

-- =====================================================================
-- STEP 4: AGGRESSIVE SHADOW REMOVAL (FIXED)
-- =====================================================================
\echo '    - Pruning parallel "shadow" routes'

WITH river_buffers AS (
    SELECT ST_Union(ST_Buffer(geom, 50)) as kill_zone 
    FROM candidate_objects 
    WHERE type = 'river'
)
DELETE FROM candidate_objects c
USING river_buffers rb
WHERE c.type = 'connector' AND c.name = 'Lake Route'
-- Safety check: Ensure length is not zero before dividing
AND (
    ST_Length(ST_Intersection(c.geom, rb.kill_zone)) / 
    NULLIF(ST_Length(c.geom), 0)
) > 0.70;

\echo '    - Pruned parallel "shadow" routes'

-- =====================================================================
-- STEP 5: TOPOLOGY CLEANUP v5 (Hubs, Gaps & Shadows)
-- =====================================================================
\echo '[5/12] Creating v5 Topology (Hubs, Gaps & Shadows)...'

DELETE FROM candidate_objects WHERE is_virtual = TRUE;

-- 1. IDENTIFY PORTS (Fixing Gaps)
-- Instead of snapping to the lake edge, we grab the exact river endpoint.
CREATE TEMP TABLE lake_ports AS
WITH raw_ends AS (
    SELECT 
        r.id as river_id,
        l.id as lake_id,
        l.geom as lake_geom,
        -- Check both start and end of river to see which touches the lake
        CASE 
            WHEN ST_DWithin(ST_StartPoint(r.geom), l.geom, 50) THEN ST_StartPoint(r.geom)
            ELSE ST_EndPoint(r.geom)
        END as port_pt
    FROM candidate_objects r
    JOIN candidate_objects l 
        ON r.type = 'river' AND l.type = 'lake'
        AND ST_DWithin(r.geom, l.geom, 50)
)
SELECT * FROM raw_ends WHERE port_pt IS NOT NULL;

-- 2. GENERATE ROUTES (Fixing Triangles)
-- Use "Hub-and-Spoke" logic for lakes with >2 rivers
WITH lake_stats AS (
    SELECT lake_id, count(*) as port_count, ST_PointOnSurface(MAX(lake_geom)) as hub_pt
    FROM lake_ports
    GROUP BY lake_id
),
routes AS (
    -- Case A: Simple Lakes (2 Ports) -> Direct Connection
    SELECT 
        p1.lake_id,
        ST_MakeLine(p1.port_pt, p2.port_pt) as geom
    FROM lake_ports p1
    JOIN lake_ports p2 ON p1.lake_id = p2.lake_id AND p1.river_id < p2.river_id
    JOIN lake_stats s ON p1.lake_id = s.lake_id
    WHERE s.port_count = 2
    
    UNION ALL
    
    -- Case B: Complex Lakes (3+ Ports) -> Connect to Hub
    SELECT 
        p.lake_id,
        ST_MakeLine(p.port_pt, s.hub_pt) as geom
    FROM lake_ports p
    JOIN lake_stats s ON p.lake_id = s.lake_id
    WHERE s.port_count > 2
)
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 
    900000 + row_number() over(), 'system', 'connector', 'Lake Route',
    geom, TRUE
FROM routes;

-- 3. REMOVE "SHADOW" ROUTES (Fixing Parallel Lines)
-- If a Lake Route is just shadowing a River, delete it.
DELETE FROM candidate_objects c
WHERE type = 'connector'
AND EXISTS (
    SELECT 1 FROM candidate_objects r
    WHERE r.type = 'river'
    -- If they are very close (40m buffer)
    AND ST_DWithin(c.geom, r.geom, 40)
    -- AND the connector is essentially "inside" the river buffer (parallel)
    AND ST_Length(ST_Intersection(ST_Buffer(c.geom, 20), ST_Buffer(r.geom, 20))) > (0.6 * ST_Length(c.geom))
);

-- 4. CREATE PORTAGES (Link pruning)
-- Links between lakes, with "Triangle Pruning" (A->C vs A->B->C)
CREATE TEMP TABLE raw_portages AS
SELECT 
    l1.id as from_lake, l2.id as to_lake,
    ST_ShortestLine(l1.geom, l2.geom) as geom,
    ST_Distance(l1.geom, l2.geom) as dist
FROM candidate_objects l1
JOIN candidate_objects l2 ON l1.id < l2.id
WHERE l1.type = 'lake' AND l2.type = 'lake'
AND ST_DWithin(l1.geom, l2.geom, 400); -- Max 400m portage

-- Prune triangles (If A->B->C is almost as fast as A->C, keep A->B->C)
DELETE FROM raw_portages p
USING (
    SELECT p1.from_lake, p1.to_lake 
    FROM raw_portages p1
    JOIN raw_portages p2 ON p1.from_lake = p2.from_lake
    JOIN raw_portages p3 ON p2.to_lake = p3.from_lake AND p3.to_lake = p1.to_lake
    WHERE (p2.dist + p3.dist) < (3 * p1.dist) -- Pruning threshold
) bad
WHERE p.from_lake = bad.from_lake AND p.to_lake = bad.to_lake;

INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 990000 + row_number() over(), 'system', 'connector', 'Portage', geom, TRUE
FROM raw_portages;

\echo '  ✓ Topology v5 applied: Gaps bridged, Triangles simplified, Shadows removed.'

-- =====================================================================
-- STEP 6: NETWORK ANALYSIS
-- =====================================================================
\echo '[6/12] Network Analysis...'

DROP TABLE IF EXISTS paddling_network CASCADE;

-- FIXED: Properly assign type for lake crossings
CREATE TABLE paddling_network AS
SELECT 
    ROW_NUMBER() OVER() as gid,
    id::text as osm_id,
    COALESCE(name, 'Unnamed Segment') as name,
    'river' as type,
    (ST_Dump(ST_Force2D(geom))).geom as geom
FROM paddling_areas
UNION ALL
SELECT 
    (ROW_NUMBER() OVER() + 1000000) as gid,
    osm_id::text,
    name,
    'lake_crossing' as type,  -- This is critical!
    (ST_Dump(ST_Force2D(geom))).geom as geom
FROM candidate_objects 
WHERE type = 'connector' AND is_virtual = TRUE;

CREATE INDEX idx_paddling_net_geom ON paddling_network USING GIST(geom);
CREATE INDEX idx_paddling_net_type ON paddling_network(type);

-- Network clustering
ALTER TABLE paddling_network ADD COLUMN network_id int;

WITH clusters AS (
    SELECT 
        gid, 
        ST_ClusterDBSCAN(geom, eps := 50, minpoints := 1) OVER () as cid
    FROM paddling_network
)
UPDATE paddling_network n
SET network_id = c.cid + 1 
FROM clusters c
WHERE n.gid = c.gid;

-- Basic metrics
ALTER TABLE paddling_network ADD COLUMN segment_len float;
ALTER TABLE paddling_network ADD COLUMN sinuosity numeric;

UPDATE paddling_network SET segment_len = ST_Length(geom);

UPDATE paddling_network 
SET sinuosity = 
    CASE 
        WHEN ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) < 1 THEN 1.0 
        ELSE ROUND(
            (ST_Length(geom)::numeric / 
            ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom))::numeric), 2)
    END
WHERE ST_GeometryType(geom) = 'ST_LineString';

\echo '  ✓ Network analysis complete'

-- =====================================================================
-- STEP 7: SEGMENTATION
-- =====================================================================
\echo '[7/12] Segmentation...'

DROP TABLE IF EXISTS paddling_segments CASCADE;

-- Chop into 1km pieces, preserving type
CREATE TABLE paddling_segments AS
WITH segment_gen AS (
    SELECT 
        gid as parent_id, 
        osm_id, 
        name, 
        type,  -- Preserve type!
        network_id,
        CASE WHEN ST_Length(geom) > 1000 
             THEN CEIL(ST_Length(geom) / 1000.0)::int 
             ELSE 1 
        END as num_chunks, 
        geom
    FROM paddling_network
)
SELECT 
    ROW_NUMBER() OVER() as seg_id, 
    parent_id, 
    osm_id, 
    name, 
    type,  -- Type preserved through segmentation
    network_id,
    ST_LineSubstring(geom, (n-1)::float/num_chunks, n::float/num_chunks) as geom
FROM segment_gen 
CROSS JOIN LATERAL generate_series(1, num_chunks) as n;

CREATE INDEX idx_pad_seg_geom ON paddling_segments USING GIST(geom);
CREATE INDEX idx_pad_seg_type ON paddling_segments(type);

-- Add columns
ALTER TABLE paddling_segments 
    ADD COLUMN length_m float,
    ADD COLUMN sinuosity numeric,
    ADD COLUMN environment text,
    ADD COLUMN has_poi_signal boolean DEFAULT FALSE,
    ADD COLUMN is_official_route boolean DEFAULT FALSE,
    ADD COLUMN has_rapids boolean DEFAULT FALSE,
    ADD COLUMN land_type text DEFAULT 'mixed',
    ADD COLUMN feasibility_score int DEFAULT 50,
    ADD COLUMN fun_score int DEFAULT 60;

-- FIXED: Calculate length and sinuosity properly
UPDATE paddling_segments SET length_m = ST_Length(geom);

UPDATE paddling_segments 
SET sinuosity = CASE 
    WHEN ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) < 1 THEN 1.0
    ELSE ROUND((length_m / ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)))::numeric, 2)
END
WHERE length_m > 0;

-- FIXED: Set environment IMMEDIATELY after creation
UPDATE paddling_segments 
SET environment = CASE 
    WHEN type = 'lake_crossing' THEN 'lake_route'
    ELSE 'river' 
END;

-- Set land_type for lake segments
UPDATE paddling_segments 
SET land_type = 'water' 
WHERE type = 'lake_crossing';

SELECT 
    type,
    environment,
    COUNT(*) as count,
    ROUND(SUM(length_m)::numeric/1000, 2) as total_km
FROM paddling_segments
GROUP BY type, environment;

\echo '  ✓ Segmentation complete'

-- =====================================================================
-- STEP 8: ENRICHMENT TABLES
-- =====================================================================
\echo '[8/12] Creating Enrichment Tables...'

DROP TABLE IF EXISTS official_routes CASCADE;
DROP TABLE IF EXISTS rapids_features CASCADE;
DROP TABLE IF EXISTS paddling_pois CASCADE;
DROP TABLE IF EXISTS land_cover CASCADE;

-- Official Routes
CREATE TABLE official_routes AS
SELECT osm_id, way as geom 
FROM planet_osm_line 
WHERE route = 'canoe'
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);
CREATE INDEX idx_routes_geom ON official_routes USING GIST(geom);

-- Rapids
CREATE TABLE rapids_features AS
SELECT osm_id, way as geom 
FROM planet_osm_line 
WHERE waterway = 'rapids'
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)
UNION ALL
SELECT osm_id, way as geom 
FROM planet_osm_point 
WHERE waterway IN ('rapids', 'waterfall')
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);
CREATE INDEX idx_rapids_geom ON rapids_features USING GIST(geom);

-- POIs
CREATE TABLE paddling_pois AS
SELECT way as geom, 'shelter' as type 
FROM planet_osm_point 
WHERE (tourism IN ('camp_site','lean_to','picnic_site','wilderness_hut') 
    OR amenity IN ('shelter')
    OR leisure = 'slipway')
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)
UNION ALL
SELECT ST_Centroid(way), 'shelter' 
FROM planet_osm_polygon 
WHERE tourism IN ('camp_site','lean_to')
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);
CREATE INDEX idx_pad_pois ON paddling_pois USING GIST(geom);

-- Land Cover (FIXED with quoted keywords)
CREATE TABLE land_cover AS
SELECT 
    osm_id, 
    ST_MakeValid(way) as geom,
    CASE 
        WHEN (landuse IN ('forest', 'wood') OR "natural" IN ('wood', 'scrub', 'heath')) THEN 'forest'
        WHEN (landuse IN ('farmland', 'farm', 'meadow', 'grass', 'orchard')) THEN 'field'
        WHEN (landuse IN ('residential', 'industrial', 'commercial', 'retail')) THEN 'urban'
        ELSE 'other'
    END as type
FROM planet_osm_polygon
WHERE ((landuse IN ('forest', 'wood', 'farmland', 'farm', 'meadow', 'grass', 'orchard', 
                   'residential', 'industrial', 'commercial', 'retail') 
    OR "natural" IN ('wood', 'scrub', 'heath'))
AND way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857));

CREATE INDEX idx_land_cover ON land_cover USING GIST(geom);
    
DO $$
DECLARE
    poi_count INT;
    rapids_count INT;
    land_count INT;
BEGIN
    SELECT COUNT(*) INTO poi_count FROM paddling_pois;
    SELECT COUNT(*) INTO rapids_count FROM rapids_features;
    SELECT COUNT(*) INTO land_count FROM land_cover;
    
    RAISE NOTICE '  POIs: %, Rapids: %, Land polygons: %', poi_count, rapids_count, land_count;
END $$;

\echo '  ✓ Enrichment tables created'

-- =====================================================================
-- STEP 9: FEATURE DETECTION
-- =====================================================================
\echo '[9/12] Detecting Features...'

-- Official routes
UPDATE paddling_segments s 
SET is_official_route = TRUE 
FROM official_routes r 
WHERE ST_Intersects(s.geom, r.geom);

-- Rapids
UPDATE paddling_segments s 
SET has_rapids = TRUE 
FROM rapids_features r 
WHERE ST_DWithin(s.geom, r.geom, 50);

-- POIs
UPDATE paddling_segments s 
SET has_poi_signal = TRUE 
FROM paddling_pois p 
WHERE ST_DWithin(s.geom, p.geom, 500);  -- Increased from 300m to 500m

-- Update environment for official routes
UPDATE paddling_segments 
SET environment = 'river_official'
WHERE is_official_route = TRUE AND environment = 'river';

DO $$
DECLARE
    official_count INT;
    rapids_count INT;
    poi_count INT;
BEGIN
    SELECT COUNT(*) INTO official_count FROM paddling_segments WHERE is_official_route;
    SELECT COUNT(*) INTO rapids_count FROM paddling_segments WHERE has_rapids;
    SELECT COUNT(*) INTO poi_count FROM paddling_segments WHERE has_poi_signal;
    
    RAISE NOTICE '  Official: %, Rapids: %, Near POIs: %', official_count, rapids_count, poi_count;
END $$;

\echo '  ✓ Features detected'

-- =====================================================================
-- STEP 10: LAND TYPE CLASSIFICATION
-- =====================================================================
\echo '[10/12] Classifying Land Types...'

-- Only for river segments (not lake crossings)
WITH land_stats AS (
    SELECT 
        s.seg_id,
        lc.type,
        SUM(ST_Area(ST_Intersection(ST_Buffer(s.geom, 50), lc.geom))) as type_area,
        ST_Area(ST_Buffer(s.geom, 50)) as total_buffer_area
    FROM paddling_segments s
    JOIN land_cover lc ON ST_Intersects(ST_Buffer(s.geom, 50), lc.geom)
    WHERE s.type = 'river'  -- Only classify rivers
    GROUP BY s.seg_id, lc.type, total_buffer_area
)
UPDATE paddling_segments s
SET land_type = ls.type
FROM land_stats ls
WHERE s.seg_id = ls.seg_id
  AND ls.type_area > (ls.total_buffer_area * 0.5);

DO $$
BEGIN
    RAISE NOTICE '  Land types: Forest=%, Field=%, Urban=%, Mixed=%',
        (SELECT COUNT(*) FROM paddling_segments WHERE land_type='forest'),
        (SELECT COUNT(*) FROM paddling_segments WHERE land_type='field'),
        (SELECT COUNT(*) FROM paddling_segments WHERE land_type='urban'),
        (SELECT COUNT(*) FROM paddling_segments WHERE land_type='mixed');
END $$;

\echo '  ✓ Land types classified'

-- =====================================================================
-- STEP 11: SCORING
-- =====================================================================
\echo '[11/12] Calculating Scores...'

-- Fun Score (based on type and attributes)
UPDATE paddling_segments 
SET fun_score = CASE 
    WHEN type = 'lake_crossing' THEN 60
    ELSE (
        CASE WHEN sinuosity > 1.2 THEN 90 ELSE 70 END +
        CASE WHEN land_type = 'forest' THEN 15 
             WHEN land_type = 'field' THEN -15 
             WHEN land_type = 'urban' THEN -20 
             ELSE 0 END +
        CASE WHEN has_rapids THEN 20 ELSE 0 END
    )
END;

-- Clamp fun scores
UPDATE paddling_segments SET fun_score = 100 WHERE fun_score > 100;
UPDATE paddling_segments SET fun_score = 0 WHERE fun_score < 0;

-- Feasibility Score
UPDATE paddling_segments 
SET feasibility_score = CASE 
    WHEN is_official_route THEN 100
    WHEN type = 'lake_crossing' THEN 80
    WHEN has_poi_signal THEN 90
    ELSE 50
END;

-- Score propagation
WITH neighbor_stats AS (
    SELECT 
        a.seg_id, 
        MAX(b.feasibility_score) as max_neighbor_score
    FROM paddling_segments a
    JOIN paddling_segments b ON ST_Touches(a.geom, b.geom) AND a.seg_id != b.seg_id
    GROUP BY a.seg_id
)
UPDATE paddling_segments s
SET feasibility_score = GREATEST(
    s.feasibility_score, 
    (s.feasibility_score * 0.7 + ns.max_neighbor_score * 0.3)::int
)
FROM neighbor_stats ns
WHERE s.seg_id = ns.seg_id 
  AND s.feasibility_score < ns.max_neighbor_score;

DO $$
BEGIN
    RAISE NOTICE '  Fun Score: min=%, max=%, avg=%',
        (SELECT MIN(fun_score) FROM paddling_segments),
        (SELECT MAX(fun_score) FROM paddling_segments),
        (SELECT ROUND(AVG(fun_score)) FROM paddling_segments);
    RAISE NOTICE '  Feasibility Score: min=%, max=%, avg=%',
        (SELECT MIN(feasibility_score) FROM paddling_segments),
        (SELECT MAX(feasibility_score) FROM paddling_segments),
        (SELECT ROUND(AVG(feasibility_score)) FROM paddling_segments);
END $$;

\echo '  ✓ Scores calculated'

-- =====================================================================
-- STEP 12: AREA STATISTICS
-- =====================================================================
\echo '[12/12] Calculating Area Statistics...'

DROP TABLE IF EXISTS paddling_areas_stats CASCADE;

CREATE TABLE paddling_areas_stats AS
SELECT 
    network_id,
    (SELECT name FROM paddling_segments s2 
     WHERE s2.network_id = s.network_id 
     AND name NOT LIKE 'Unnamed%' 
     AND type = 'river' 
     ORDER BY length_m DESC LIMIT 1) as area_name,
    COUNT(*) as segment_count,
    ROUND(SUM(length_m)::numeric / 1000, 2) as total_km,
    ROUND(AVG(fun_score), 0) as avg_fun,
    MAX(feasibility_score) as max_feasibility,
    ST_Union(geom) as geom
FROM paddling_segments s
WHERE network_id IS NOT NULL
GROUP BY network_id;

\echo '  ✓ Area stats calculated'

-- =====================================================================
-- FINAL SUMMARY
-- =====================================================================
\echo ''
\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║  PIPELINE COMPLETE!                                            ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''

SELECT 
    'Total Segments' as metric,
    COUNT(*)::text as value
FROM paddling_segments
UNION ALL
SELECT 
    'River Segments',
    COUNT(*)::text
FROM paddling_segments WHERE type = 'river'
UNION ALL
SELECT 
    'Lake Crossings',
    COUNT(*)::text
FROM paddling_segments WHERE type = 'lake_crossing'
UNION ALL
SELECT 
    'Total Networks',
    COUNT(DISTINCT network_id)::text
FROM paddling_segments WHERE network_id IS NOT NULL
UNION ALL
SELECT 
    'Total Length (km)',
    ROUND(SUM(length_m)::numeric / 1000, 2)::text
FROM paddling_segments
UNION ALL
SELECT 
    'Avg Fun Score',
    ROUND(AVG(fun_score), 1)::text
FROM paddling_segments
UNION ALL
SELECT 
    'Avg Feasibility',
    ROUND(AVG(feasibility_score), 1)::text
FROM paddling_segments;

\echo ''
\echo 'Environment Distribution:'
SELECT environment, COUNT(*) as count FROM paddling_segments GROUP BY environment;

\echo ''
\echo 'Ready for QA and export!'
\echo ''
