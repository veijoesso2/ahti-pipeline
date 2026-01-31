-- =====================================================================
-- GEOMETRY VALIDATION PATCH
-- Add this to the end of Step 7 (Segmentation) in master_pipeline_v16.sql
-- =====================================================================

-- After segmentation, clean up problematic geometries immediately

\echo '  Validating geometries...'

-- Remove zero-length segments (degenerate geometries)
DELETE FROM paddling_segments 
WHERE length_m < 1;

-- Fix sinuosity for segments where it couldn't be calculated
-- (usually circles or very short segments where start ≈ end)
UPDATE paddling_segments 
SET sinuosity = CASE 
    WHEN sinuosity IS NULL THEN 1.5  -- Assume moderate winding
    WHEN sinuosity > 10 THEN 10.0    -- Cap at 10 (extremely winding)
    WHEN sinuosity < 0.9 THEN 1.0    -- Floor at 1.0 (straight line minimum)
    ELSE sinuosity
END;

-- Ensure all segments have valid environment
UPDATE paddling_segments
SET environment = 'river'
WHERE environment IS NULL AND type = 'river';

UPDATE paddling_segments
SET environment = 'lake_route'
WHERE environment IS NULL AND type = 'lake_crossing';

-- Remove any NULL geometries (shouldn't happen but safety check)
DELETE FROM paddling_segments WHERE geom IS NULL;

-- Report cleaning results
DO $$
DECLARE
    final_count INT;
BEGIN
    SELECT COUNT(*) INTO final_count FROM paddling_segments;
    RAISE NOTICE '  Geometry validation complete. Final segments: %', final_count;
END $$;
