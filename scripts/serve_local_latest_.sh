#!/bin/bash
OUTPUT_FILE="$HOME/ahti-pipeline/output/local_data.json"

echo "1. Exporting Data with REJECTED LAYERS..."
PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder -t -A -c "
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(ST_AsGeoJSON(t.*)::json)
)
FROM (
    -- Real Rivers
    SELECT id::text, name, 'river_blue' as type, ST_Transform(geom, 4326) as geometry FROM paddling_areas
    UNION ALL
    -- Lakes
    SELECT id::text, name, 'lake_gray' as type, ST_Transform(geom, 4326) as geometry FROM candidate_objects WHERE type = 'lake' AND ST_Area(geom) > 50000
    UNION ALL
    -- Valid Connectors (Yellow)
    SELECT 'c_' || row_number() over()::text, label as name, 'connector_valid' as type, ST_Transform(geom, 4326) FROM debug_layers WHERE type IN ('star_valid', 'link_valid')
    UNION ALL
    -- REJECTED Connectors (Gray Lines)
    SELECT 'r_' || row_number() over()::text, label as name, 'connector_rejected' as type, ST_Transform(geom, 4326) FROM debug_layers WHERE type IN ('star_rejected', 'link_rejected')
    UNION ALL
    -- Valid Ports (Red Dots)
    SELECT 'p_' || row_number() over()::text, label as name, 'port_valid' as type, ST_Transform(geom, 4326) FROM debug_layers WHERE type = 'port_valid'
    UNION ALL
    -- REJECTED Ports (Gray Dots)
    SELECT 'rp_' || row_number() over()::text, label as name, 'port_rejected' as type, ST_Transform(geom, 4326) FROM debug_layers WHERE type = 'port_rejected'
    UNION ALL
    -- Pink Debug Lines
    SELECT 'di_' || row_number() over()::text, label as name, 'debug_inside_purple' as type, ST_Transform(geom, 4326) FROM debug_layers WHERE type = 'debug_inside_segment'
) t;
" > $OUTPUT_FILE

echo "2. Data ready at $OUTPUT_FILE"
echo "3. Starting Local CORS Web Server..."
cd $HOME/ahti-pipeline/output
python3 ~/ahti-pipeline/scripts/cors_server.py 8080