-- 1. Candidate Water Objects (Raw Atoms)
CREATE TABLE candidate_objects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    osm_id BIGINT,
    source_type TEXT DEFAULT 'osm',
    type TEXT, -- 'river', 'lake', 'rapid'
    name TEXT,
    tags JSONB, -- Store full tags for flexibility
    geom GEOMETRY(Geometry, 3857) -- Storing in Web Mercator for easier tiling later
);
CREATE INDEX idx_cwo_geom ON candidate_objects USING GIST (geom);
CREATE INDEX idx_cwo_tags ON candidate_objects USING GIN (tags);

-- 2. Staging Tables for osm2pgsql (Standard format)
-- We let osm2pgsql create its own tables (planet_osm_line, etc) 
-- and then we migrate data to candidate_objects using SQL.
