-- =====================================================================
-- DEBUG SCRIPT: CONNECTOR LOGIC DIAGNOSTIC
-- =====================================================================
-- Run this AFTER running your master pipeline.
-- It generates a debug table to visualize why connectors are spawning.

DROP TABLE IF EXISTS debug_terminals CASCADE;

-- 1. Extract ALL endpoints from every river segment
CREATE TABLE debug_terminals AS
WITH raw_points AS (
    SELECT id as river_id, ST_StartPoint(geom) as geom, 'start' as end_type FROM candidate_objects WHERE type = 'river'
    UNION ALL
    SELECT id, ST_EndPoint(geom), 'end' FROM candidate_objects WHERE type = 'river'
),
-- 2. Cluster them to find "groups" of endpoints
clustered AS (
    SELECT 
        river_id, 
        end_type,
        geom,
        ST_ClusterDBSCAN(geom, eps := 8, minpoints := 1) OVER () as cluster_id
    FROM raw_points
),
-- 3. Calculate cluster statistics
stats AS (
    SELECT 
        cluster_id, 
        COUNT(*) as point_count,
        ST_Centroid(ST_Collect(geom)) as center_geom
    FROM clustered
    GROUP BY cluster_id
)
-- 4. Final Classification
SELECT 
    c.river_id,
    c.cluster_id,
    s.point_count,
    CASE 
        WHEN s.point_count = 1 THEN 'valid_terminal'  -- Green: True Dead End
        WHEN s.point_count = 2 THEN 'continuation'    -- Grey: Standard river flow
        ELSE 'junction_cluster'                       -- Red: 3+ lines meeting (Loop/Fork)
    END as status,
    c.geom
FROM clustered c
JOIN stats s ON c.cluster_id = s.cluster_id;

-- 5. Export for viewer
\echo '---------------------------------------------------'
\echo 'DEBUG DATA GENERATED'
\echo '---------------------------------------------------'
SELECT status, COUNT(*) FROM debug_terminals GROUP BY status;
\echo '---------------------------------------------------'