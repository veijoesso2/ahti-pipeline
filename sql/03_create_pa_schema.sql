-- 1. Paddling Areas (The Logical Entity)
CREATE TABLE IF NOT EXISTS paddling_areas (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT,
    area_type TEXT, -- 'river_section', 'lake_area'
    
    -- Metrics
    length_m REAL,
    difficulty_grade TEXT DEFAULT 'unknown',
    
    -- The Cached Geometry (for display speed)
    geom GEOMETRY(Geometry, 3857)
);
CREATE INDEX idx_pa_geom ON paddling_areas USING GIST (geom);

-- 2. The Link Table (Many-to-Many)
-- This connects the Raw CWO (Atom) to the Paddling Area (Molecule)
CREATE TABLE IF NOT EXISTS paddling_area_cwo (
    pa_id UUID REFERENCES paddling_areas(id) ON DELETE CASCADE,
    cwo_id UUID REFERENCES candidate_objects(id) ON DELETE CASCADE,
    role TEXT DEFAULT 'main', -- 'main', 'gap_connector'
    PRIMARY KEY (pa_id, cwo_id)
);

-- 3. Area Signals (The Evidence Locker)
CREATE TABLE IF NOT EXISTS area_signals (
    pa_id UUID REFERENCES paddling_areas(id) ON DELETE CASCADE,
    signal_type TEXT,
    value_raw REAL,
    source TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
