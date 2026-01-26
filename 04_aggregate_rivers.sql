DROP TABLE IF EXISTS paddling_areas CASCADE;

CREATE TABLE paddling_areas (
    id SERIAL PRIMARY KEY,
    name TEXT,
    geom GEOMETRY(MultiLineString, 3857),
    total_length_km REAL
);

INSERT INTO paddling_areas (name, geom, total_length_km)
SELECT 
    name,
    -- CHANGED: Reduced snap from 50m to 10m to preserve small rivers
    ST_Multi(ST_Simplify(ST_LineMerge(ST_UnaryUnion(ST_SnapToGrid(ST_Collect(geom), 10))), 5)) as geom,
    SUM(ST_Length(ST_Transform(geom, 4326)::geography)) / 1000.0 as total_length_km
FROM candidate_objects
WHERE type = 'river' 
  AND name IS NOT NULL
GROUP BY name;

CREATE INDEX idx_paddling_areas_geom ON paddling_areas USING GIST (geom);