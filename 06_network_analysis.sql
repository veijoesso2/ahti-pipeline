DROP TABLE IF EXISTS paddling_network;
DROP TABLE IF EXISTS paddling_lakes;

-- 1. UNIFY & EXPLODE THE NETWORK
-- We use ST_Dump to break MultiLineStrings into simple LineStrings.
CREATE TABLE paddling_network AS
SELECT 
    ROW_NUMBER() OVER() as gid,
    id::text as osm_id,
    COALESCE(name, 'Unnamed Segment') as name,
    'river' as type,
    (ST_Dump(ST_Force2D(geom))).geom as geom -- <--- THE FIX
FROM paddling_areas
UNION ALL
SELECT 
    (ROW_NUMBER() OVER() + 1000000) as gid,
    osm_id::text,
    name,
    CASE 
        WHEN name LIKE '%Star%' THEN 'lake_crossing'
        WHEN name LIKE '%Link%' THEN 'portage_link'
        ELSE 'connector' 
    END as type,
    (ST_Dump(ST_Force2D(geom))).geom as geom -- <--- THE FIX
FROM candidate_objects 
WHERE type = 'connector' AND is_virtual = TRUE;

CREATE INDEX idx_paddling_net_geom ON paddling_network USING GIST(geom);
CREATE INDEX idx_paddling_net_gid ON paddling_network(gid);

-- 2. CLUSTER THE LINES (Assign Network IDs)
ALTER TABLE paddling_network ADD COLUMN network_id int;

WITH clusters AS (
    SELECT 
        gid, 
        -- 50m tolerance snaps segments together
        ST_ClusterDBSCAN(geom, eps := 50, minpoints := 1) OVER () as cid
    FROM paddling_network
)
UPDATE paddling_network n
SET network_id = c.cid + 1 
FROM clusters c
WHERE n.gid = c.gid;

-- 3. CALCULATE METRICS
ALTER TABLE paddling_network ADD COLUMN segment_len float;
ALTER TABLE paddling_network ADD COLUMN sinuosity numeric;
ALTER TABLE paddling_network ADD COLUMN difficulty_score int;

UPDATE paddling_network SET segment_len = ST_Length(geom);

-- Calculate Sinuosity (Now working because geom is guaranteed LineString)
UPDATE paddling_network 
SET sinuosity = 
    CASE 
        -- Handle Loops or Tiny Lines (prevent div by zero)
        WHEN ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom)) < 1 THEN 1.0 
        -- Normal Calculation
        ELSE ROUND(
            (ST_Length(geom)::numeric / 
            ST_Distance(ST_StartPoint(geom), ST_EndPoint(geom))::numeric)
        , 2)
    END;

-- Difficulty Scoring
UPDATE paddling_network 
SET difficulty_score = CASE 
    WHEN type IN ('lake_crossing', 'portage_link') THEN 1
    WHEN sinuosity > 1.5 THEN 2 -- Winding
    WHEN sinuosity < 1.1 THEN 3 -- Straight
    ELSE 2
END;

-- 4. PREPARE LAKES
CREATE TABLE paddling_lakes AS
SELECT 
    l.id::text as osm_id,
    l.name,
    'lake_context' as type,
    l.geom,
    NULL::int as network_id
FROM candidate_objects l
WHERE l.type = 'lake' AND ST_Area(l.geom) > 50000;

CREATE INDEX idx_paddling_lakes_geom ON paddling_lakes USING GIST(geom);

-- Assign Lake ID based on intersections
UPDATE paddling_lakes l
SET network_id = n.network_id
FROM paddling_network n
WHERE ST_Intersects(l.geom, n.geom)
AND ST_Length(ST_Intersection(l.geom, n.geom)) > 50;