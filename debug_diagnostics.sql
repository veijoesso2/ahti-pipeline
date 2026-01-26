-- 1. CHECK COORDINATE SYSTEMS (SRID)
-- If these numbers are different (e.g., 4326 vs 3857), our spatial math is broken.
SELECT 'SRID Check' as test,
       (SELECT ST_SRID(geom) FROM paddling_segments LIMIT 1) as segment_srid,
       (SELECT ST_SRID(way) FROM planet_osm_polygon LIMIT 1) as osm_srid;

-- 2. CHECK "OFFICIAL ROUTE" SOURCES
-- Let's see exactly which lines are turning purple.
SELECT 'Route Tags' as test, route, count(*) 
FROM planet_osm_line 
WHERE route IN ('canoe', 'boat', 'ferry')
GROUP BY route;

-- 3. CHECK LAND COVER INTERSECTIONS
-- Pick a random segment and see if it finds ANY forest nearby.
WITH sample_seg AS (
    SELECT geom FROM paddling_segments LIMIT 1
)
SELECT 'Forest Test' as test, 
       (SELECT COUNT(*) FROM planet_osm_polygon p 
        WHERE "landuse" = 'forest' 
        AND ST_DWithin(p.way, (SELECT geom FROM sample_seg), 100)) as forests_within_100m;

-- 4. CHECK POI AVAILABILITY
SELECT 'POI Count' as test, type, count(*) 
FROM paddling_pois 
GROUP BY type;