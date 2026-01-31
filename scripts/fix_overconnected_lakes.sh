#!/bin/bash
# =====================================================================
# FIX OVER-CONNECTED LAKES
# Remove redundant connectors for lakes with >10 connections
# =====================================================================

DB_NAME="ahti_staging"
DB_USER="ahti_builder"
DB_PASS="ahti_secret_password"

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║  FIX OVER-CONNECTED LAKES                                     ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

echo "Step 1: Finding over-connected lakes..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
WITH lake_connections AS (
    SELECT 
        l.id as lake_id,
        l.name as lake_name,
        COUNT(c.id) as connector_count,
        ST_Area(l.geom) as lake_area
    FROM candidate_objects l
    LEFT JOIN candidate_objects c ON (
        c.is_virtual = TRUE 
        AND ST_Intersects(l.geom, c.geom)
    )
    WHERE l.type = 'lake'
    GROUP BY l.id, l.name, l.geom
    HAVING COUNT(c.id) > 10
    ORDER BY COUNT(c.id) DESC
)
SELECT 
    lake_id,
    COALESCE(lake_name, 'Unnamed') as name,
    connector_count,
    ROUND(lake_area/1000000, 2) as area_km2
FROM lake_connections
LIMIT 20;
SQL

echo ""
echo "Step 2: Analyzing connector patterns..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
-- Check what types of connectors these lakes have
WITH over_connected AS (
    SELECT l.id as lake_id
    FROM candidate_objects l
    LEFT JOIN candidate_objects c ON (
        c.is_virtual = TRUE 
        AND ST_Intersects(l.geom, c.geom)
    )
    WHERE l.type = 'lake'
    GROUP BY l.id
    HAVING COUNT(c.id) > 10
)
SELECT 
    CASE 
        WHEN c.name LIKE '%Star%' THEN 'Star Connectors'
        WHEN c.name LIKE '%Portage%' THEN 'Portage Links'
        WHEN c.name LIKE '%Culvert%' THEN 'Culvert Healing'
        ELSE 'Other'
    END as connector_type,
    COUNT(*) as count,
    ROUND(AVG(ST_Length(c.geom))) as avg_length
FROM candidate_objects c
WHERE c.is_virtual = TRUE
AND EXISTS (
    SELECT 1 FROM over_connected oc
    JOIN candidate_objects l ON l.id = oc.lake_id
    WHERE ST_Intersects(c.geom, l.geom)
)
GROUP BY 1;
SQL

echo ""
echo "Step 3: Removing redundant short connectors..."
echo "   (Keeping longest/most useful connectors for each over-connected lake)"

PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
-- Strategy: For lakes with >10 connectors, keep only the 8 longest ones
-- This preserves the most useful connections while reducing clutter

WITH over_connected_lakes AS (
    SELECT l.id as lake_id
    FROM candidate_objects l
    JOIN candidate_objects c ON (
        c.is_virtual = TRUE 
        AND ST_Intersects(l.geom, c.geom)
    )
    WHERE l.type = 'lake'
    GROUP BY l.id
    HAVING COUNT(c.id) > 10
),
ranked_connectors AS (
    SELECT 
        c.id as connector_id,
        l.id as lake_id,
        ST_Length(c.geom) as length,
        ROW_NUMBER() OVER (
            PARTITION BY l.id 
            ORDER BY ST_Length(c.geom) DESC
        ) as rank
    FROM candidate_objects c
    JOIN candidate_objects l ON ST_Intersects(c.geom, l.geom)
    WHERE c.is_virtual = TRUE
    AND l.type = 'lake'
    AND l.id IN (SELECT lake_id FROM over_connected_lakes)
),
connectors_to_remove AS (
    SELECT connector_id 
    FROM ranked_connectors 
    WHERE rank > 8  -- Keep top 8, remove rest
)
DELETE FROM candidate_objects
WHERE id IN (SELECT connector_id FROM connectors_to_remove);

-- Report what was removed
SELECT 
    'Redundant connectors removed' as action,
    (SELECT COUNT(*) FROM connectors_to_remove) as count;
SQL

echo ""
echo "Step 4: Verifying improvement..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
SELECT 
    'Lakes with >10 connectors (after)' as metric,
    COUNT(*) as count
FROM (
    SELECT l.id
    FROM candidate_objects l
    LEFT JOIN candidate_objects c ON (
        c.is_virtual = TRUE 
        AND ST_Intersects(l.geom, c.geom)
    )
    WHERE l.type = 'lake'
    GROUP BY l.id
    HAVING COUNT(c.id) > 10
) t;
SQL

echo ""
echo "Step 5: Rebuilding network with cleaned connectors..."
echo "   (This requires re-running network analysis)"

PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
-- Quick rebuild of paddling_network to reflect removed connectors
DROP TABLE IF EXISTS paddling_network CASCADE;

CREATE TABLE paddling_network AS
SELECT 
    ROW_NUMBER() OVER() as gid,
    id::text as osm_id,
    COALESCE(name, 'Unnamed Segment') as name,
    'river' as type,
    (ST_Dump(ST_Force2D(geom))).geom as geom
FROM paddling_areas
UNION ALL
SELECT 
    (ROW_NUMBER() OVER() + 1000000) as gid,
    id::text,
    name,
    CASE 
        WHEN name LIKE '%Star%' THEN 'lake_crossing'
        WHEN name LIKE '%Portage%' THEN 'portage_link'
        WHEN name LIKE '%Culvert%' THEN 'culvert_link'
        ELSE 'connector'
    END as type,
    (ST_Dump(ST_Force2D(geom))).geom as geom
FROM candidate_objects 
WHERE is_virtual = TRUE;

CREATE INDEX idx_paddling_net_geom ON paddling_network USING GIST(geom);

-- Recalculate network IDs
ALTER TABLE paddling_network ADD COLUMN network_id int;

WITH clusters AS (
    SELECT 
        gid, 
        ST_ClusterDBSCAN(geom, eps := 50, minpoints := 1) OVER () as cid
    FROM paddling_network
)
UPDATE paddling_network n
SET network_id = c.cid + 1 
FROM clusters c
WHERE n.gid = c.gid;

SELECT 'Network rebuilt' as status, COUNT(*) as segments 
FROM paddling_network;
SQL

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║  FIX COMPLETE!                                                ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "What was fixed:"
echo "  ✓ Identified over-connected lakes"
echo "  ✓ Kept 8 longest/most useful connectors per lake"
echo "  ✓ Removed redundant short connectors"
echo "  ✓ Rebuilt network with cleaned connectors"
echo ""
echo "Next steps:"
echo "  1. Re-run segmentation to update paddling_segments"
echo "  2. Or re-run full pipeline with geometry validation"
echo ""
