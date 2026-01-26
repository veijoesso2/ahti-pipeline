DELETE FROM candidate_objects WHERE is_virtual = TRUE;
DROP TABLE IF EXISTS debug_layers;

-- 1. ANALYZE RIVER-LAKE INTERSECTIONS
WITH river_intersections AS (
    SELECT 
        r.id as river_id,
        COALESCE(r.name, 'Unknown River') as river_name,
        r.geom as river_geom,
        l.id as lake_id,
        l.geom as lake_geom,
        ST_CollectionExtract(ST_Intersection(r.geom, l.geom), 2) as inside_geom,
        ST_Length(ST_Intersection(r.geom, l.geom)) as inside_len
    FROM candidate_objects r
    JOIN candidate_objects l ON ST_Intersects(r.geom, l.geom)
    WHERE r.type = 'river' AND l.type = 'lake' AND r.is_virtual = FALSE
),

-- 2. THE BLACKLIST (Served Lakes)
-- Level 1: Lakes containing > 50m of river
primary_served AS (
    SELECT DISTINCT lake_id 
    FROM river_intersections 
    WHERE inside_len > 50
),

-- Level 2: Lakes within 20m of a Served Lake (Neighbors)
extended_blacklist AS (
    SELECT lake_id FROM primary_served
    UNION
    SELECT b.id as lake_id
    FROM candidate_objects b
    JOIN candidate_objects a ON ST_DWithin(a.geom, b.geom, 20)
    WHERE a.id IN (SELECT lake_id FROM primary_served)
      AND b.type = 'lake'
),

-- 3. VALID FEEDERS (Short Tributaries)
valid_feeders AS (
    SELECT * FROM river_intersections 
    WHERE inside_len <= 50 
),

-- 4. IDENTIFY PORTS
raw_ports AS (
    SELECT 
        r.river_id::text || '_' || r.lake_id::text as port_id, 
        r.river_geom, 
        r.lake_id,
        r.lake_geom,
        CASE 
            WHEN ST_DWithin(ST_StartPoint(r.river_geom), r.lake_geom, 50) THEN ST_StartPoint(r.river_geom)
            WHEN ST_DWithin(ST_EndPoint(r.river_geom), r.lake_geom, 50) THEN ST_EndPoint(r.river_geom)
            ELSE NULL 
        END as port_pt
    FROM valid_feeders r
),
valid_ports AS (
    SELECT * FROM raw_ports WHERE port_pt IS NOT NULL
),

-- 5. CLUSTER PORTS
clustered_ports_ids AS (
    SELECT *, ST_ClusterDBSCAN(port_pt, eps := 100, minpoints := 1) OVER (PARTITION BY lake_id) as cluster_id
    FROM valid_ports
),
final_ports AS (
    SELECT 
        lake_id,
        ST_PointOnSurface(MAX(lake_geom)) as lake_center,
        ST_Centroid(ST_Collect(port_pt)) as port_center
    FROM clustered_ports_ids
    GROUP BY lake_id, cluster_id
),

-- 6. GENERATE STAR CONNECTIONS (Port -> Center)
star_connections AS (
    SELECT 
        'star' as type,
        ST_MakeLine(fp.port_center, fp.lake_center) as geom,
        fp.lake_id
    FROM final_ports fp
    WHERE fp.lake_id NOT IN (SELECT lake_id FROM extended_blacklist)
),

-- 7. LAKE-TO-LAKE CHAINING (Lake -> Lake)
lake_chains AS (
    SELECT 
        'link' as type,
        ST_MakeLine(ST_PointOnSurface(a.geom), ST_PointOnSurface(b.geom)) as geom,
        a.id as lake_id
    FROM candidate_objects a
    JOIN candidate_objects b 
      ON a.id < b.id 
      AND a.type = 'lake' AND b.type = 'lake'
      AND ST_DWithin(a.geom, b.geom, 20) 
      AND ST_Area(a.geom) > 50000 AND ST_Area(b.geom) > 50000
    
    -- CRITICAL FIX: DO NOT link if BOTH lakes are already served by rivers.
    -- If they are both served, the river connects them naturally.
    WHERE NOT (
        a.id IN (SELECT lake_id FROM extended_blacklist) 
        AND 
        b.id IN (SELECT lake_id FROM extended_blacklist)
    )
),

-- 8. COMBINE
all_connections AS (
    SELECT type, geom, lake_id FROM star_connections
    UNION ALL
    SELECT type, geom, lake_id FROM lake_chains
)

-- 9. SAVE DEBUG DATA
SELECT 'port_raw' as type, 'Port' as label, port_pt as geom INTO debug_layers FROM valid_ports
UNION ALL
SELECT 'connection_line', 'Lake ' || lake_id || ': ' || type, geom FROM all_connections
UNION ALL
SELECT 
    'debug_inside_segment' as type, 
    river_name || ' (' || round(inside_len::numeric, 0) || 'm)', 
    inside_geom 
FROM river_intersections;

-- 10. INSERT VIRTUAL CONNECTORS
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 
    9999, 
    'system', 
    'connector', 
    CASE 
        WHEN label LIKE '%star%' THEN 'Star Connector (Port->Center)'
        WHEN label LIKE '%link%' THEN 'Link Connector (Lake->Lake)'
        ELSE 'Connector'
    END, 
    geom, 
    TRUE 
FROM debug_layers 
WHERE type = 'connection_line';