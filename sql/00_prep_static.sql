-- sql/00_prep_static.sql
-- Run this ONCE to prepare the static context data.

-- 1. SCORING CONFIG (The "Brain")
DROP TABLE IF EXISTS scoring_config;
CREATE TABLE scoring_config AS SELECT 
    300 as poi_proximity_dist, 50 as base_feasibility, 40 as poi_bonus, 1.2 as sinuosity_threshold, 
    90 as score_winding, 70 as score_normal, 40 as score_boring, 60 as score_lake, 10 as score_portage,
    15 as bonus_forest, -15 as penalty_field, -20 as penalty_urban, 20 as bonus_rapids;

-- 2. OFFICIAL ROUTES (Strict Canoe Only)
DROP TABLE IF EXISTS official_routes;
CREATE TABLE official_routes AS
SELECT osm_id, way as geom FROM planet_osm_line WHERE route = 'canoe';
CREATE INDEX idx_or_geom ON official_routes USING GIST(geom);

-- 3. PADDLING POIS (Shelters, Piers)
DROP TABLE IF EXISTS paddling_pois;
CREATE TABLE paddling_pois AS
SELECT way as geom, 'shelter' as type FROM planet_osm_point 
WHERE "tourism" IN ('camp_site','lean_to','picnic_site','wilderness_hut') OR "man_made" IN ('pier', 'mast') OR "leisure" = 'slipway'
UNION ALL
SELECT ST_Centroid(way), 'shelter' FROM planet_osm_polygon WHERE "tourism" IN ('camp_site','lean_to');
CREATE INDEX idx_pp_geom ON paddling_pois USING GIST(geom);

-- 4. LAND COVER (Pre-calculated for speed)
DROP TABLE IF EXISTS land_cover;
CREATE TABLE land_cover AS
SELECT 
    osm_id, 
    -- Simplify geometry to save RAM on the Mac Mini
    ST_Simplify(way, 10) as geom, 
    CASE 
        WHEN "landuse" IN ('forest', 'wood') OR "natural" IN ('wood', 'scrub', 'heath') THEN 'forest'
        WHEN "landuse" IN ('farmland', 'farm', 'meadow', 'grass', 'orchard') THEN 'field'
        WHEN "landuse" IN ('residential', 'industrial', 'commercial') THEN 'urban'
        ELSE 'other'
    END as type
FROM planet_osm_polygon
WHERE "landuse" IN ('forest', 'wood', 'farmland', 'farm', 'meadow', 'grass', 'residential', 'industrial') 
   OR "natural" IN ('wood', 'scrub');
CREATE INDEX idx_lc_geom ON land_cover USING GIST(geom);

-- 5. EMPTY TARGET TABLES (If they don't exist)
CREATE TABLE IF NOT EXISTS paddling_segments (
    seg_id SERIAL PRIMARY KEY, parent_id int, osm_id bigint, name text, type text, network_id int,
    geom geometry(LineString, 3857),
    length_m float, sinuosity numeric, environment text, land_type text DEFAULT 'mixed',
    fun_score int, feasibility_score int,
    has_poi_signal boolean DEFAULT FALSE, is_official_route boolean DEFAULT FALSE, has_rapids boolean DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_ps_geom ON paddling_segments USING GIST(geom);