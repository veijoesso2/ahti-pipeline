#!/bin/bash
# Note: We rely on wrangler.toml for the DB binding now.
OUTPUT_DIR="$HOME/ahti-pipeline/output/chunks"

# 1. Prep directories
mkdir -p $OUTPUT_DIR
rm -f $OUTPUT_DIR/*

echo "1. Generating Optimized SQL (Simplified Geometry)..."
# We use ST_Simplify to keep files small
PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder -t -A -F "','" -c "
-- RIVERS (Simplify 1m)
SELECT 'INSERT INTO routes_tracer (id, name, length_km, geojson, obj_type) VALUES (''' || id || ''', ''' || COALESCE(REPLACE(name, '''', ''''''), 'Unnamed') || ''', 0, ''' || REPLACE(ST_AsGeoJSON(ST_Transform(ST_Simplify(geom, 1), 4326)), '''', '''''') || ''', ''river'');'
FROM candidate_objects WHERE type='river' AND is_virtual = FALSE;

-- CONNECTORS
SELECT 'INSERT INTO routes_tracer (id, name, length_km, geojson, obj_type) VALUES (''' || id || ''', ''' || COALESCE(REPLACE(name, '''', ''''''), 'Connector') || ''', 0, ''' || REPLACE(ST_AsGeoJSON(ST_Transform(geom, 4326)), '''', '''''') || ''', ''connector'');'
FROM candidate_objects WHERE is_virtual = TRUE;

-- LAKES (Simplify 15m - slightly more aggressive simplification for background)
SELECT 'INSERT INTO routes_tracer (id, name, length_km, geojson, obj_type) VALUES (''' || id || ''', ''' || COALESCE(REPLACE(name, '''', ''''''), 'Lake') || ''', 0, ''' || REPLACE(ST_AsGeoJSON(ST_Transform(ST_Simplify(geom, 15), 4326)), '''', '''''') || ''', ''lake'');'
FROM candidate_objects WHERE type='lake';
" > $OUTPUT_DIR/full_dump.sql

echo "2. Splitting into Micro-Chunks (10 rows per chunk)..."
split -l 10 $OUTPUT_DIR/full_dump.sql $OUTPUT_DIR/chunk_

echo "3. Uploading Chunks..."
count=0
total=$(ls $OUTPUT_DIR/chunk_* | wc -l)

# Clear the DB first
wrangler d1 execute ahti-tracer --command "DELETE FROM routes_tracer;" --remote

for file in $OUTPUT_DIR/chunk_*; do
    ((count++))
    echo "   - Uploading Batch $count of $total..."
    
    # UPDATED COMMAND (Removed --database-id)
    if ! wrangler d1 execute ahti-tracer --file $file --remote > /dev/null 2>> upload_errors.log; then
        echo "     X Batch $count failed (Check upload_errors.log)"
    fi
    
    # Slight pause
    sleep 0.5
done

echo "Done! Uploaded $count batches."
