#!/bin/bash
# Export Paddling Areas to GeoJSON (Transforming 3857 back to 4326 for web use)
PGPASSWORD='ahti_secret_password' ogr2ogr -f "GeoJSON" \
  $HOME/ahti-pipeline/output/kouvola_rivers.json \
  PG:"host=localhost dbname=ahti_staging user=ahti_builder password=ahti_secret_password" \
  -sql "SELECT id, name, area_type, round((length_m/1000)::numeric, 2) as length_km, ST_Transform(geom, 4326) as geom FROM paddling_areas"
