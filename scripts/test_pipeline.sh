#!/bin/bash
# Simple test script for Espoo pipeline

DB_NAME="ahti_staging"
DB_USER="ahti_builder"  
DB_PASS="ahti_secret_password"

# Espoo bounding box
MIN_X=2726000
MAX_X=2768000
MIN_Y=8408000
MAX_Y=8481000

echo "Running pipeline with bbox: X($MIN_X-$MAX_X) Y($MIN_Y-$MAX_Y)"
echo ""

# Run pipeline with explicit variable setting
psql -h localhost -d ahti_staging -U ahti_builder   -v min_x=2726000 -v min_y=8408000 -v max_x=2768000 -v max_y=8481000   -c "SET custom.min_x = '2726000'; SET custom.min_y = '8408000'; SET custom.max_x = '2768000'; SET custom.max_y = '8481000';"   -f ~/ahti-pipeline/sql/master_pipeline_fixed.sql

echo ""
echo "Pipeline complete. Checking results..."
echo ""

# Check what was created
PGPASSWORD="$DB_PASS" psql -h localhost -d "$DB_NAME" -U "$DB_USER" << 'SQL'
SELECT 'Candidate rivers' as item, COUNT(*) as count FROM candidate_objects WHERE type='river'
UNION ALL
SELECT 'Candidate lakes', COUNT(*) FROM candidate_objects WHERE type='lake'
UNION ALL  
SELECT 'Paddling segments', COUNT(*) FROM paddling_segments
UNION ALL
SELECT 'River segments', COUNT(*) FROM paddling_segments WHERE type='river'
UNION ALL
SELECT 'Lake crossings', COUNT(*) FROM paddling_segments WHERE type='lake_crossing';
SQL
