-- sql/detailed_chunk.sql
-- This runs inside the loop for every 50km box.

-- 1. CLEANUP OLD DATA IN THIS CHUNK
DELETE FROM paddling_segments WHERE geom && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);

-- 2. EXTRACT & SEGMENT (With STRICT filters for speed)
INSERT INTO paddling_segments (parent_id, osm_id, name, type, geom)
WITH raw_network AS (
    SELECT osm_id, COALESCE(name, 'Unnamed') as name, waterway as type, ST_Transform(way, 3857) as geom
    FROM planet_osm_line
    WHERE 
      -- *** STRICT FILTER: Rivers only. Ignore streams, ditches, canals ***
      waterway IN ('river', 'rapids')
      OR (waterway = 'stream' AND tags->'canoe' = 'yes') -- Exception: explicitly marked canoe streams
      AND way && ST_Transform(ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857), 3857)
),
segment_gen AS (
    SELECT osm_id, name, type,
        CASE WHEN ST_Length(geom) > 1000 THEN CEIL(ST_Length(geom) / 1000.0)::int ELSE 1 END as num_chunks, geom
    FROM raw_network
)
SELECT osm_id, osm_id, name, type,
    ST_LineSubstring(geom, (n-1)::float/num_chunks, n::float/num_chunks) as geom
FROM segment_gen CROSS JOIN LATERAL generate_series(1, num_chunks) as n;

-- 3. CALCULATE METRICS (The Detailed Logic)
UPDATE paddling_segments s SET length_m = ST_Length(geom) WHERE geom && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);

UPDATE paddling_segments s 
SET sinuosity = CASE WHEN ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) < 1 THEN 3.0 ELSE ROUND((length_m::numeric / ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom))::numeric), 2) END
WHERE geom && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);

-- 4. CONTEXT ANALYSIS (Using pre-calculated tables)
-- Land Use (Fast Check)
UPDATE paddling_segments s SET land_type = lc.type
FROM land_cover lc 
WHERE ST_Intersects(ST_PointOnSurface(s.geom), lc.geom)
  AND s.geom && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);

-- POIs
UPDATE paddling_segments s SET has_poi_signal = TRUE 
FROM paddling_pois p 
WHERE ST_DWithin(s.geom, p.geom, 300)
  AND s.geom && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);

-- 5. FINAL SCORING (Detailed Formula)
UPDATE paddling_segments s SET fun_score = (SELECT 
    CASE 
        WHEN s.sinuosity > 1.2 THEN 90 ELSE 70 END + 
        CASE WHEN s.land_type = 'forest' THEN 15 WHEN s.land_type = 'field' THEN -15 WHEN s.land_type = 'urban' THEN -20 ELSE 0 END
    FROM scoring_config LIMIT 1)
WHERE s.geom && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);

UPDATE paddling_segments s SET feasibility_score = (SELECT 
    CASE WHEN s.has_poi_signal THEN 90 ELSE 50 END
    FROM scoring_config LIMIT 1)
WHERE s.geom && ST_MakeEnvelope(:min_x, :min_y, :max_x, :max_y, 3857);