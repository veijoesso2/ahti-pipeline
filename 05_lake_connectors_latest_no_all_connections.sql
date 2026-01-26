DELETE FROM candidate_objects WHERE is_virtual = TRUE;
DROP TABLE IF EXISTS debug_layers;

CREATE TABLE debug_layers AS
WITH 
-- 1. ANALYZE RIVER-LAKE INTERSECTIONS
river_intersections AS (
    SELECT 
        r.id as river_id,
        COALESCE(r.name, 'Unknown River') as river_name,
        r.geom as river_geom,
        l.id as lake_id,
        l.geom as lake_geom,
        ST_CollectionExtract(ST_Intersection(r.geom, l.geom), 2) as inside_geom
    FROM candidate_objects r
    JOIN candidate_objects l ON ST_Intersects(r.geom, l.geom)
    WHERE r.type = 'river' AND l.type = 'lake' AND r.is_virtual = FALSE
),

-- 2. RAW PORTS
raw_ports AS (
    SELECT 
        r.river_id, r.river_name, r.lake_id, r.lake_geom,
        CASE 
            WHEN ST_DWithin(ST_StartPoint(r.river_geom), r.lake_geom, 50) THEN ST_StartPoint(r.river_geom)
            WHEN ST_DWithin(ST_EndPoint(r.river_geom), r.lake_geom, 50) THEN ST_EndPoint(r.river_geom)
            ELSE NULL 
        END as port_pt
    FROM river_intersections r
),

-- 3. CLASSIFY PORTS (Keep the Splice Guard - It works well)
ports_context AS (
    SELECT 
        p.*,
        (SELECT MIN(ST_Distance(p.port_pt, other.geom))
         FROM candidate_objects other
         WHERE other.type = 'river' AND other.id != p.river_id
        ) as dist_any,
        (SELECT MIN(ST_Distance(p.port_pt, other.geom))
         FROM candidate_objects other
         WHERE other.type = 'river' AND other.id != p.river_id AND other.name = p.river_name
        ) as dist_same_name
    FROM raw_ports p
    WHERE p.port_pt IS NOT NULL
),
classified_ports AS (
    SELECT *,
        CASE 
            -- If same name is < 600m, it's a splice (continuation). Kill it.
            WHEN dist_same_name < 600 THEN 'REJECTED: Same-Name Splice'
            -- If any river is < 300m, it's a splice. Kill it.
            WHEN dist_any < 300 THEN 'REJECTED: Generic Splice' 
            ELSE 'VALID'
        END as status
    FROM ports_context
),

-- 4. CLUSTER & CALCULATE CENTERS
clustered_ports_ids AS (
    SELECT *, ST_ClusterDBSCAN(port_pt, eps := 100, minpoints := 1) OVER (PARTITION BY lake_id) as cluster_id
    FROM classified_ports WHERE status = 'VALID'
),
final_ports AS (
    SELECT 
        vp.lake_id, vp.river_name,
        -- Weighted Center: Pulls the star center towards the ports
        ST_Centroid(ST_Union(ST_PointOnSurface(MAX(vp.lake_geom)), ST_Centroid(ST_Collect(vp.port_pt)))) as lake_center,
        ST_Centroid(ST_Collect(vp.port_pt)) as port_center
    FROM clustered_ports_ids vp
    GROUP BY vp.lake_id, vp.river_name, vp.cluster_id
),

-- 5. STAR CONNECTIONS (SIMPLIFIED)
-- We removed the "Main River Exists" block. 
-- If there is a valid port, we DRAW THE LINE. 
-- It is better to have a redundant yellow line than a missing one.
star_candidates AS (
    SELECT 
        'star' as type,
        ST_MakeLine(fp.port_center, fp.lake_center) as geom,
        fp.lake_id,
        'VALID' as status
    FROM final_ports fp
),

-- 6. LAKE CHAINS (Bridging Gaps)
-- Removed "Shared River" block. If lakes are close, we bridge them.
lake_pairs AS (
    SELECT 
        a.id as lake_a, b.id as lake_b,
        ST_ShortestLine(a.geom, b.geom) as geom
    FROM candidate_objects a
    JOIN candidate_objects b 
      ON a.id < b.id 
      AND a.type = 'lake' AND b.type = 'lake'
      -- Search Radius 1.5km
      AND ST_DWithin(a.geom, b.geom, 1500) 
      AND ST_Area(a.geom) > 50000 AND ST_Area(b.geom) > 50000
),
chain_candidates AS (
    SELECT 
        'link' as type, geom, lake_a as lake_id,
        ST_Length(geom) as land_dist,
        CASE 
            -- Max Gap: 1200m
            WHEN ST_Length(geom) > 1200 THEN 'REJECTED: Gap > 1200m'
            ELSE 'VALID'
        END as status
    FROM lake_pairs lp
),

-- 7. COMBINE DEBUG DATA
all_debug AS (
    SELECT 
        CASE WHEN status LIKE 'VALID%' THEN 'port_valid' ELSE 'port_rejected' END as type,
        river_name || ' - ' || status as label,
        port_pt as geom 
    FROM classified_ports
    UNION ALL
    SELECT 
        CASE WHEN status LIKE 'VALID%' THEN 'star_valid' ELSE 'star_rejected' END as type,
        'Star: ' || status as label,
        geom 
    FROM star_candidates
    UNION ALL
    SELECT 
        CASE WHEN status LIKE 'VALID%' THEN 'link_valid' ELSE 'link_rejected' END as type,
        'Link: ' || status || ' (Gap: ' || round(land_dist::numeric,0) || 'm)' as label,
        geom 
    FROM chain_candidates
    UNION ALL 
    SELECT DISTINCT 'debug_inside_segment', r.river_name, r.inside_geom FROM river_intersections r
)
SELECT * FROM all_debug;

-- 8. INSERT VALID ITEMS
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 9999, 'system', 'connector', label, geom, TRUE 
FROM debug_layers 
WHERE type IN ('star_valid', 'link_valid');DELETE FROM candidate_objects WHERE is_virtual = TRUE;
DROP TABLE IF EXISTS debug_layers;

CREATE TABLE debug_layers AS
WITH 
-- 1. ANALYZE RIVER-LAKE INTERSECTIONS
river_intersections AS (
    SELECT 
        r.id as river_id,
        COALESCE(r.name, 'Unknown River') as river_name,
        r.geom as river_geom,
        l.id as lake_id,
        l.geom as lake_geom,
        ST_CollectionExtract(ST_Intersection(r.geom, l.geom), 2) as inside_geom
    FROM candidate_objects r
    JOIN candidate_objects l ON ST_Intersects(r.geom, l.geom)
    WHERE r.type = 'river' AND l.type = 'lake' AND r.is_virtual = FALSE
),

-- 2. RAW PORTS
raw_ports AS (
    SELECT 
        r.river_id, r.river_name, r.lake_id, r.lake_geom,
        CASE 
            WHEN ST_DWithin(ST_StartPoint(r.river_geom), r.lake_geom, 50) THEN ST_StartPoint(r.river_geom)
            WHEN ST_DWithin(ST_EndPoint(r.river_geom), r.lake_geom, 50) THEN ST_EndPoint(r.river_geom)
            ELSE NULL 
        END as port_pt
    FROM river_intersections r
),

-- 3. CLASSIFY PORTS (Keep the Splice Guard - It works well)
ports_context AS (
    SELECT 
        p.*,
        (SELECT MIN(ST_Distance(p.port_pt, other.geom))
         FROM candidate_objects other
         WHERE other.type = 'river' AND other.id != p.river_id
        ) as dist_any,
        (SELECT MIN(ST_Distance(p.port_pt, other.geom))
         FROM candidate_objects other
         WHERE other.type = 'river' AND other.id != p.river_id AND other.name = p.river_name
        ) as dist_same_name
    FROM raw_ports p
    WHERE p.port_pt IS NOT NULL
),
classified_ports AS (
    SELECT *,
        CASE 
            -- If same name is < 600m, it's a splice (continuation). Kill it.
            WHEN dist_same_name < 600 THEN 'REJECTED: Same-Name Splice'
            -- If any river is < 300m, it's a splice. Kill it.
            WHEN dist_any < 300 THEN 'REJECTED: Generic Splice' 
            ELSE 'VALID'
        END as status
    FROM ports_context
),

-- 4. CLUSTER & CALCULATE CENTERS
clustered_ports_ids AS (
    SELECT *, ST_ClusterDBSCAN(port_pt, eps := 100, minpoints := 1) OVER (PARTITION BY lake_id) as cluster_id
    FROM classified_ports WHERE status = 'VALID'
),
final_ports AS (
    SELECT 
        vp.lake_id, vp.river_name,
        -- Weighted Center: Pulls the star center towards the ports
        ST_Centroid(ST_Union(ST_PointOnSurface(MAX(vp.lake_geom)), ST_Centroid(ST_Collect(vp.port_pt)))) as lake_center,
        ST_Centroid(ST_Collect(vp.port_pt)) as port_center
    FROM clustered_ports_ids vp
    GROUP BY vp.lake_id, vp.river_name, vp.cluster_id
),

-- 5. STAR CONNECTIONS (SIMPLIFIED)
-- We removed the "Main River Exists" block. 
-- If there is a valid port, we DRAW THE LINE. 
-- It is better to have a redundant yellow line than a missing one.
star_candidates AS (
    SELECT 
        'star' as type,
        ST_MakeLine(fp.port_center, fp.lake_center) as geom,
        fp.lake_id,
        'VALID' as status
    FROM final_ports fp
),

-- 6. LAKE CHAINS (Bridging Gaps)
-- Removed "Shared River" block. If lakes are close, we bridge them.
lake_pairs AS (
    SELECT 
        a.id as lake_a, b.id as lake_b,
        ST_ShortestLine(a.geom, b.geom) as geom
    FROM candidate_objects a
    JOIN candidate_objects b 
      ON a.id < b.id 
      AND a.type = 'lake' AND b.type = 'lake'
      -- Search Radius 1.5km
      AND ST_DWithin(a.geom, b.geom, 1500) 
      AND ST_Area(a.geom) > 50000 AND ST_Area(b.geom) > 50000
),
chain_candidates AS (
    SELECT 
        'link' as type, geom, lake_a as lake_id,
        ST_Length(geom) as land_dist,
        CASE 
            -- Max Gap: 1200m
            WHEN ST_Length(geom) > 1200 THEN 'REJECTED: Gap > 1200m'
            ELSE 'VALID'
        END as status
    FROM lake_pairs lp
),

-- 7. COMBINE DEBUG DATA
all_debug AS (
    SELECT 
        CASE WHEN status LIKE 'VALID%' THEN 'port_valid' ELSE 'port_rejected' END as type,
        river_name || ' - ' || status as label,
        port_pt as geom 
    FROM classified_ports
    UNION ALL
    SELECT 
        CASE WHEN status LIKE 'VALID%' THEN 'star_valid' ELSE 'star_rejected' END as type,
        'Star: ' || status as label,
        geom 
    FROM star_candidates
    UNION ALL
    SELECT 
        CASE WHEN status LIKE 'VALID%' THEN 'link_valid' ELSE 'link_rejected' END as type,
        'Link: ' || status || ' (Gap: ' || round(land_dist::numeric,0) || 'm)' as label,
        geom 
    FROM chain_candidates
    UNION ALL 
    SELECT DISTINCT 'debug_inside_segment', r.river_name, r.inside_geom FROM river_intersections r
)
SELECT * FROM all_debug;

-- 8. INSERT VALID ITEMS
INSERT INTO candidate_objects (osm_id, source_type, type, name, geom, is_virtual)
SELECT 9999, 'system', 'connector', label, geom, TRUE 
FROM debug_layers 
WHERE type IN ('star_valid', 'link_valid');