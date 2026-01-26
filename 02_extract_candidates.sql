-- 1. Clean slate
TRUNCATE TABLE candidate_objects RESTART IDENTITY CASCADE;

-- 2. Define the Crop Zone
DROP TABLE IF EXISTS analysis_bounds;
CREATE TEMPORARY TABLE analysis_bounds AS 
SELECT ST_Transform(ST_MakeEnvelope(
    26.50, 60.40,  -- Min Lon, Min Lat
    27.10, 61.10,  -- Max Lon, Max Lat
    4326), 3857) AS geom;

-- 3. Extract RIVERS + CANALS + STREAMS
-- We now include 'canal', 'stream', and 'drain' to catch power plant bypasses.
INSERT INTO candidate_objects (osm_id, source_type, type, name, tags, geom)
SELECT l.osm_id, 'osm', 'river', COALESCE(l.name, 'Unnamed Link'), hstore_to_jsonb(l.tags), l.way 
FROM planet_osm_line l, analysis_bounds b
WHERE l.waterway IN ('river', 'rapids', 'canal', 'stream', 'flowline') 
  AND ST_Intersects(l.way, b.geom);

-- 4. Extract LAKES (> 5 Hectares)
INSERT INTO candidate_objects (osm_id, source_type, type, name, tags, geom)
SELECT p.osm_id, 'osm', 'lake', COALESCE(p.name, 'Coastal Water'), hstore_to_jsonb(p.tags), p.way 
FROM planet_osm_polygon p, analysis_bounds b
WHERE (
    ("natural" IN ('water', 'coastline') OR "water" = 'lake')
    OR ("boundary" = 'administrative' AND "admin_level" = '2')
)
AND ST_Intersects(p.way, b.geom)
AND ST_Area(p.way) > 50000;