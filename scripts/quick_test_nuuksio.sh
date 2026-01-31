#!/bin/bash
# =====================================================================
# QUICK TEST: NUUKSIO & BODOM ONLY (Small BBOX)
# =====================================================================

DB_NAME="ahti_staging"
DB_USER="ahti_builder"
DB_PASS="ahti_secret_password"

# Focused BBOX (Nuuksio + Bodom lakes)
# This is much smaller than the full Espoo area
MIN_X=2735000
MAX_X=2755000
MIN_Y=8450000
MAX_Y=8470000

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║  RUNNING QUICK TEST: NUUKSIO AREA ONLY                       ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo "   BBox: X($MIN_X - $MAX_X) Y($MIN_Y - $MAX_Y)"
echo ""

# 1. CLEANUP
echo "Step 1: Cleaning previous data..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER -q << 'SQL'
DELETE FROM paddling_segments;
DELETE FROM candidate_objects;
DELETE FROM paddling_areas;
SQL

# 2. RUN PIPELINE
echo "Step 2: Running Pipeline..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
    -v min_x="$MIN_X" -v min_y="$MIN_Y" -v max_x="$MAX_X" -v max_y="$MAX_Y" \
    -c "SET custom.min_x = '$MIN_X'; SET custom.min_y = '$MIN_Y'; SET custom.max_x = '$MAX_X'; SET custom.max_y = '$MAX_Y';" \
    -f "$HOME/ahti-pipeline/sql/master_pipeline_v18.sql"

# 3. EXPORT
echo "Step 3: Exporting Data..."
cd "$HOME/ahti-pipeline"
bash scripts/qa_export_with_debug.sh

echo ""
echo "Done! Check map at: http://localhost:8080/qa_viewer.html"