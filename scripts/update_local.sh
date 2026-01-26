#!/bin/bash
OUTPUT_FILE="$HOME/ahti-pipeline/output/local_data.json"

echo "1. Exporting Analyzed Network..."
PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder -t -A -c "
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(ST_AsGeoJSON(t.*)::json)
)
FROM (
    -- Export the unified network
    SELECT 
        osm_id, 
        name, 
        type,
        network_id,      -- Use this to color-code connected areas
        sinuosity,       -- Use this to judge difficulty
        ST_Transform(geom, 4326) as geometry 
    FROM paddling_network
) t;
" > $OUTPUT_FILE

echo "2. Data ready at $OUTPUT_FILE"
echo "3. Starting Local CORS Web Server..."
cd $HOME/ahti-pipeline/output
python3 ~/ahti-pipeline/scripts/cors_server.py 8080