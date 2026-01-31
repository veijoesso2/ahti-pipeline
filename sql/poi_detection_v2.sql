-- =====================================================================
-- IMPROVED POI DETECTION v2.0
-- Critical fix: Increase search radius and add buffering
-- Replace the POI detection section in Step 9 of master_pipeline_v16.sql
-- =====================================================================

\echo '[9/12] Detecting Features (IMPROVED)...'

-- Official routes (unchanged)
UPDATE paddling_segments s 
SET is_official_route = TRUE 
FROM official_routes r 
WHERE ST_Intersects(s.geom, r.geom);

-- Rapids (unchanged)
UPDATE paddling_segments s 
SET has_rapids = TRUE 
FROM rapids_features r 
WHERE ST_DWithin(s.geom, r.geom, 50);

-- POI DETECTION - IMPROVED VERSION
-- Strategy: Use larger radius + buffer around POIs for better coverage
DROP TABLE IF EXISTS paddling_pois_buffered;

-- Create buffered POI zones (200m radius around each POI)
CREATE TABLE paddling_pois_buffered AS
SELECT 
    ST_Buffer(geom, 200) as geom,
    type
FROM paddling_pois;

CREATE INDEX idx_pad_pois_buf ON paddling_pois_buffered USING GIST(geom);

-- First pass: Direct proximity (1000m) - INCREASED from 500m
UPDATE paddling_segments s 
SET has_poi_signal = TRUE 
FROM paddling_pois p 
WHERE ST_DWithin(s.geom, p.geom, 1000);

-- Second pass: Buffer intersection (catches POIs near but not within 1000m)
UPDATE paddling_segments s
SET has_poi_signal = TRUE
FROM paddling_pois_buffered pb
WHERE ST_Intersects(s.geom, pb.geom)
AND has_poi_signal = FALSE;  -- Don't double-count

-- Update environment for official routes
UPDATE paddling_segments 
SET environment = 'river_official'
WHERE is_official_route = TRUE AND environment = 'river';

-- Report detection stats
DO $$
DECLARE
    official_count INT;
    rapids_count INT;
    poi_count INT;
    poi_pct NUMERIC;
    total_segments INT;
BEGIN
    SELECT COUNT(*) INTO total_segments FROM paddling_segments;
    SELECT COUNT(*) INTO official_count FROM paddling_segments WHERE is_official_route;
    SELECT COUNT(*) INTO rapids_count FROM paddling_segments WHERE has_rapids;
    SELECT COUNT(*) INTO poi_count FROM paddling_segments WHERE has_poi_signal;
    
    poi_pct := ROUND((100.0 * poi_count / NULLIF(total_segments, 0))::numeric, 1);
    
    RAISE NOTICE '  Official: %, Rapids: %, POI Coverage: % (%.1f%%)', 
        official_count, rapids_count, poi_count, poi_pct;
END $$;

\echo '  ✓ Features detected (improved POI coverage)'
