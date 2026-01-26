-- =================================================================
-- AHTI MASTER PIPELINE: Integrated Build
-- =================================================================

-- 1. CHUNK CLEANUP
-- Clear previous work in this specific box to avoid duplicates
DELETE FROM paddling_segments WHERE geom && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);
DELETE FROM candidate_objects WHERE is_virtual = TRUE AND geom && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);

-- 2. DYNAMIC LAKE CONNECTIONS (Star Connectors)
-- Logic adapted from 05_lake_connectors.sql to run per-chunk
WITH ports AS (
    SELECT 
        l.osm_id as lake_id, 
        ST_ClosestPoint(l.way, ST_EndPoint(r.way)) as pt
    FROM planet_osm_polygon l
    JOIN planet_osm_line r ON ST_DWithin(l.way, r.way, 150) 
    WHERE l.natural = 'water' AND r.waterway IS NOT NULL
      AND l.way && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857)
)
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 
    9999, 'system', 'connector', 'Lake Star Connection', 
    ST_MakeLine(pt, ST_PointOnSurface(ST_MakeValid(l.way))), TRUE
FROM ports p
JOIN planet_osm_polygon l ON p.lake_id = l.osm_id;

-- 3. NETWORK RESEED (3m Width Filter)
-- Re-populates the source network with strict navigability rules
TRUNCATE TABLE paddling_network;
INSERT INTO paddling_network (osm_id, name, type, geom)
SELECT 
    osm_id, 
    COALESCE(name, 'Unnamed'), 
    waterway, 
    ST_Transform(way, 3857)
FROM planet_osm_line
WHERE (
    waterway = 'river' -- Always keep rivers
    OR (waterway IN ('stream', 'canal', 'ditch') AND (tags->'width')::numeric >= 3) -- Strict 3m Filter
    OR (tags->'route' = 'canoe') -- Keep official routes
)
AND way && ST_Transform(ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857), 3857);

-- Add the new lake connectors into the network
INSERT INTO paddling_network (osm_id, name, type, geom)
SELECT 0, name, 'lake_crossing', geom 
FROM candidate_objects 
WHERE is_virtual = TRUE AND geom && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);

-- 4. SEGMENTATION
-- Adapted from 07_segmentation_scoring.sql
INSERT INTO paddling_segments (parent_id, osm_id, name, type, geom)
WITH segment_gen AS (
    SELECT gid as parent_id, osm_id, name, type,
        CASE WHEN ST_Length(geom) > 1000 THEN CEIL(ST_Length(geom) / 1000.0)::int ELSE 1 END as num_chunks, geom
    FROM paddling_network
)
SELECT parent_id, osm_id, name, type,
    ST_LineSubstring(geom, (n-1)::float/num_chunks, n::float/num_chunks) as geom
FROM segment_gen CROSS JOIN LATERAL generate_series(1, num_chunks) as n;

-- 5. ENRICHMENT & SCORING
-- Precise Land Use Analysis (50% Threshold)
UPDATE paddling_segments SET length_m = ST_Length(geom);

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
UPDATE paddling_segments s SET land_type = ls.type FROM land_stats ls
WHERE s.seg_id = ls.seg_id AND ls.type_area > (ls.total_buffer_area * 0.5);

-- Apply final Scoring
UPDATE paddling_segments s SET 
    fun_score = CASE WHEN land_type = 'forest' THEN 85 WHEN land_type = 'urban' THEN 40 ELSE 60 END,
    feasibility_score = CASE WHEN type = 'river' THEN 80 WHEN type = 'lake_crossing' THEN 100 ELSE 50 END;