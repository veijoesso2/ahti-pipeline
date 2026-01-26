#!/bin/bash
# PASTE YOUR DATABASE ID HERE
DB_ID="25e721c4-e53f-4cec-92d8-b128f13e6a16"

echo "1. Clearing old data from Cloudflare..."
# Corrected: Run the delete command against Cloudflare, not local Postgres
wrangler d1 execute ahti-tracer --command "DELETE FROM routes_tracer;" --remote

echo "2. Exporting RAW DEBUG data (Rivers, Connectors, Lakes)..."

# We use psql ONLY to generate the text for the INSERT statements.
# Note: We removed the 'TRUNCATE' command from inside this SQL block.

PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder -t -A -F "','" -c "
-- INSERT RIVERS
SELECT 
  'INSERT INTO routes_tracer (id, name, length_km, geojson, obj_type) VALUES (''' || id || ''', ''' || COALESCE(REPLACE(name, '''', ''''''), 'Unnamed') || ''', 0, ''' || REPLACE(ST_AsGeoJSON(ST_Transform(geom, 4326)), '''', '''''') || ''', ''river'');'
FROM candidate_objects WHERE type='river' AND is_virtual = FALSE;

-- INSERT CONNECTORS
SELECT 
  'INSERT INTO routes_tracer (id, name, length_km, geojson, obj_type) VALUES (''' || id || ''', ''' || COALESCE(REPLACE(name, '''', ''''''), 'Connector') || ''', 0, ''' || REPLACE(ST_AsGeoJSON(ST_Transform(geom, 4326)), '''', '''''') || ''', ''connector'');'
FROM candidate_objects WHERE is_virtual = TRUE;

-- INSERT LAKES
SELECT 
  'INSERT INTO routes_tracer (id, name, length_km, geojson, obj_type) VALUES (''' || id || ''', ''' || COALESCE(REPLACE(name, '''', ''''''), 'Lake') || ''', 0, ''' || REPLACE(ST_AsGeoJSON(ST_Transform(geom, 4326)), '''', '''''') || ''', ''lake'');'
FROM candidate_objects WHERE type='lake';
" > ~/ahti-pipeline/output/upload.sql

echo "3. Uploading to Cloudflare D1..."
wrangler d1 execute ahti-tracer --file ~/ahti-pipeline/output/upload.sql --remote

echo "Done! Debug data is live."
