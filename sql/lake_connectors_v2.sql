-- =====================================================================
-- IMPROVED LAKE CONNECTOR LOGIC v2.0
-- Critical fixes + Important improvements
-- Replace the entire Step 5 in master_pipeline_v16.sql
-- =====================================================================

\echo '[5/12] Creating Smart Lake Connectors v2.0...'

DELETE FROM candidate_objects WHERE is_virtual = TRUE;

WITH 
-- 1. Analyze river-lake intersections
river_intersections AS (
    SELECT 
        r.id as river_id,
        COALESCE(r.name, 'Unknown River') as river_name,
        r.geom as river_geom,
        l.id as lake_id,
        l.geom as lake_geom,
        ST_Length(ST_Intersection(r.geom, l.geom)) as inside_len
    FROM candidate_objects r
    JOIN candidate_objects l ON ST_Intersects(r.geom, l.geom)
    WHERE r.type = 'river' AND l.type = 'lake' AND r.is_virtual = FALSE
),

-- 2. Lakes that are well-served by rivers (>100m of river inside)
served_lakes AS (
    SELECT DISTINCT lake_id 
    FROM river_intersections 
    WHERE inside_len > 100  -- Increased from 50m
),

-- 3. Valid river endpoints that touch lakes (potential ports)
valid_feeders AS (
    SELECT * FROM river_intersections WHERE inside_len <= 100  -- Increased from 50m
),

raw_ports AS (
    SELECT 
        r.lake_id,
        r.lake_geom,
        CASE 
            WHEN ST_DWithin(ST_StartPoint(r.river_geom), r.lake_geom, 100) THEN ST_StartPoint(r.river_geom)
            WHEN ST_DWithin(ST_EndPoint(r.river_geom), r.lake_geom, 100) THEN ST_EndPoint(r.river_geom)
        END as port_pt
    FROM valid_feeders r
),

valid_ports AS (
    SELECT * FROM raw_ports WHERE port_pt IS NOT NULL
),

-- 4. Cluster ports and find centers (with distance-based weighting)
clustered_ports AS (
    SELECT 
        lake_id,
        ST_ClusterDBSCAN(port_pt, eps := 150, minpoints := 1) OVER (PARTITION BY lake_id) as cluster_id,
        port_pt,
        lake_geom
    FROM valid_ports
),

port_centers AS (
    SELECT 
        lake_id,
        cluster_id,
        ST_PointOnSurface(MAX(lake_geom)) as lake_center,
        ST_Centroid(ST_Collect(port_pt)) as port_center
    FROM clustered_ports
    GROUP BY lake_id, cluster_id
),

-- 5. Create star connections (port to lake center)
-- Only create if connection is useful (>20m) and not too long (>3km)
star_connections AS (
    SELECT 
        'star_connector' as conn_type,
        ST_MakeLine(port_center, lake_center) as geom,
        lake_id,
        ST_Length(ST_MakeLine(port_center, lake_center)) as length
    FROM port_centers
    WHERE ST_Length(ST_MakeLine(port_center, lake_center)) BETWEEN 20 AND 3000
),

-- 6. Find nearby lake pairs for portage links (IMPROVED)
lake_pairs AS (
    SELECT 
        a.id as lake_a_id,
        b.id as lake_b_id,
        a.geom as lake_a_geom,
        b.geom as lake_b_geom,
        ST_Distance(a.geom, b.geom) as gap_distance,
        ST_Area(a.geom) as area_a,
        ST_Area(b.geom) as area_b
    FROM candidate_objects a
    JOIN candidate_objects b ON a.id < b.id
    WHERE a.type = 'lake' 
      AND b.type = 'lake'
      -- IMPROVED: Increased distance from 500m to 1000m
      AND ST_DWithin(a.geom, b.geom, 1000)
      -- IMPROVED: Lower size threshold from 50000 to 25000
      AND ST_Area(a.geom) > 25000
      AND ST_Area(b.geom) > 25000
),

-- 7. Create portage link candidates (IMPROVED filtering)
portage_candidates AS (
    SELECT 
        'portage_link' as conn_type,
        ST_ShortestLine(lake_a_geom, lake_b_geom) as geom,
        lake_a_id as lake_id,
        gap_distance as length,
        area_a,
        area_b,
        -- IMPROVED: Smarter filtering logic
        CASE 
            -- Skip if BOTH lakes are well-served by rivers
            WHEN lake_a_id IN (SELECT lake_id FROM served_lakes) 
             AND lake_b_id IN (SELECT lake_id FROM served_lakes) 
            THEN 'SKIP: Both served'
            -- Allow longer gaps for larger lakes
            WHEN gap_distance > 800 AND (area_a < 100000 OR area_b < 100000)
            THEN 'SKIP: Gap too large for lake size'
            -- Hard limit at 1200m
            WHEN gap_distance > 1200
            THEN 'SKIP: Gap > 1200m'
            ELSE 'VALID'
        END as status
    FROM lake_pairs
),

-- 8. NEAREST NEIGHBOR FALLBACK for isolated lakes
-- Find lakes that still have no connections and connect them to nearest lake
unconnected_lakes AS (
    SELECT l.id, l.geom, ST_Area(l.geom) as area
    FROM candidate_objects l
    WHERE l.type = 'lake'
    AND ST_Area(l.geom) > 25000  -- Significant lakes only
    -- Not connected by star connectors
    AND l.id NOT IN (SELECT lake_id FROM star_connections)
    -- Not connected by portage links
    AND l.id NOT IN (SELECT lake_id FROM portage_candidates WHERE status = 'VALID')
    -- Not directly served by rivers
    AND l.id NOT IN (SELECT lake_id FROM served_lakes)
),

nearest_neighbor_links AS (
    SELECT DISTINCT ON (a.id)
        'nearest_neighbor' as conn_type,
        ST_ShortestLine(a.geom, b.geom) as geom,
        a.id as lake_id,
        ST_Distance(a.geom, b.geom) as length,
        a.area,
        CASE 
            WHEN ST_Distance(a.geom, b.geom) > 1500 THEN 'SKIP: Too far'
            ELSE 'VALID'
        END as status
    FROM unconnected_lakes a
    CROSS JOIN candidate_objects b
    WHERE b.type = 'lake'
    AND a.id != b.id
    AND ST_DWithin(a.geom, b.geom, 1500)
    AND ST_Area(b.geom) > 25000
    ORDER BY a.id, ST_Distance(a.geom, b.geom)
),

-- 9. Combine all valid connectors
all_connectors AS (
    SELECT conn_type, geom, lake_id, length, 'VALID' as status
    FROM star_connections
    
    UNION ALL
    
    SELECT conn_type, geom, lake_id, length, status
    FROM portage_candidates
    WHERE status = 'VALID'
    
    UNION ALL
    
    SELECT conn_type, geom, lake_id, length, status
    FROM nearest_neighbor_links
    WHERE status = 'VALID'
),

-- 10. SMART DEDUPLICATION: Remove redundant connectors
-- If multiple connectors between same two lakes, keep only the shortest
deduplicated_connectors AS (
    SELECT DISTINCT ON (lake_id, ST_AsText(ST_StartPoint(geom)), ST_AsText(ST_EndPoint(geom)))
        conn_type, geom, lake_id, length
    FROM all_connectors
    ORDER BY lake_id, ST_AsText(ST_StartPoint(geom)), ST_AsText(ST_EndPoint(geom)), length
),

-- 11. DENSITY CONTROL: For lakes with too many connectors, keep only best ones
lake_connector_counts AS (
    SELECT 
        lake_id,
        COUNT(*) as connector_count
    FROM deduplicated_connectors
    GROUP BY lake_id
),

ranked_connectors AS (
    SELECT 
        dc.*,
        lcc.connector_count,
        ROW_NUMBER() OVER (
            PARTITION BY dc.lake_id 
            ORDER BY 
                -- Prioritize: nearest_neighbor > portage_link > star_connector
                CASE dc.conn_type 
                    WHEN 'nearest_neighbor' THEN 1
                    WHEN 'portage_link' THEN 2
                    WHEN 'star_connector' THEN 3
                END,
                -- Then by length (shorter is better)
                dc.length
        ) as rank
    FROM deduplicated_connectors dc
    JOIN lake_connector_counts lcc ON dc.lake_id = lcc.lake_id
),

final_connectors AS (
    SELECT conn_type, geom, lake_id, length
    FROM ranked_connectors
    WHERE 
        -- If lake has <= 8 connectors, keep all
        connector_count <= 8
        OR
        -- If lake has > 8 connectors, keep only top 8
        rank <= 8
)

-- 12. Insert valid connectors
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 
    9999, 
    'system', 
    'connector',
    CASE 
        WHEN conn_type = 'star_connector' THEN 'Star (' || ROUND(length::numeric) || 'm)'
        WHEN conn_type = 'portage_link' THEN 'Portage (' || ROUND(length::numeric) || 'm)'
        WHEN conn_type = 'nearest_neighbor' THEN 'Link (' || ROUND(length::numeric) || 'm)'
        ELSE 'Connector'
    END,
    geom, 
    TRUE 
FROM final_connectors;

-- Report what was created
DO $$
DECLARE
    star_count INT;
    portage_count INT;
    neighbor_count INT;
    total_count INT;
    lakes_connected INT;
    total_lakes INT;
BEGIN
    SELECT COUNT(*) INTO star_count 
    FROM candidate_objects 
    WHERE is_virtual = TRUE AND name LIKE 'Star%';
    
    SELECT COUNT(*) INTO portage_count 
    FROM candidate_objects 
    WHERE is_virtual = TRUE AND name LIKE 'Portage%';
    
    SELECT COUNT(*) INTO neighbor_count 
    FROM candidate_objects 
    WHERE is_virtual = TRUE AND name LIKE 'Link%';
    
    SELECT COUNT(*) INTO total_count
    FROM candidate_objects
    WHERE is_virtual = TRUE;
    
    SELECT COUNT(DISTINCT l.id) INTO lakes_connected
    FROM candidate_objects l
    WHERE l.type = 'lake'
    AND EXISTS (
        SELECT 1 FROM candidate_objects c
        WHERE c.is_virtual = TRUE
        AND ST_Intersects(c.geom, l.geom)
    );
    
    SELECT COUNT(*) INTO total_lakes
    FROM candidate_objects
    WHERE type = 'lake';
    
    RAISE NOTICE '  Star: %, Portage: %, Nearest: %, Total: %', 
        star_count, portage_count, neighbor_count, total_count;
    RAISE NOTICE '  Lakes connected: %/% (%.1f%%)', 
        lakes_connected, total_lakes, 
        (100.0 * lakes_connected / NULLIF(total_lakes, 0));
END $$;

\echo '  ✓ Smart connectors v2.0 created'
