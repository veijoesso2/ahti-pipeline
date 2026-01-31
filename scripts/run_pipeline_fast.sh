#!/bin/bash
DB_NAME="ahti_staging"
DB_USER="ahti_builder"
DB_PASS="ahti_secret_password"
MIN_X=2586031
MAX_X=2960000
MIN_Y=8330000
MAX_Y=8600000

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║  RUNNING DEBUG TOPOLOGY CHECK v5 (Fix: EPSG:4326 Export)      ║"
echo "╚═══════════════════════════════════════════════════════════════╝"

# 1. RUN SQL PIPELINE
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
    -v min_x="$MIN_X" -v min_y="$MIN_Y" -v max_x="$MAX_X" -v max_y="$MAX_Y" \
    -c "SET custom.min_x = '$MIN_X'; SET custom.min_y = '$MIN_Y'; SET custom.max_x = '$MAX_X'; SET custom.max_y = '$MAX_Y';" \
    -f "$HOME/ahti-pipeline/sql/master_pipeline_v16.sql"

if [ $? -ne 0 ]; then echo "❌ SQL Failed"; exit 1; fi

# 2. EXPORT MAIN DATA
cd "$HOME/ahti-pipeline"
bash scripts/qa_export_with_debug.sh > /dev/null 2>&1

# 3. EXPORT DEBUG DATA (Now Converting 3857 -> 4326)
# 3. EXPORT DEBUG DATA
# 3. EXPORT DEBUG DATA
echo "  → Exporting Debug Layers..."
rm -f output/debug_topology.json

# FIX: Added -nln debug to force a consistent internal layer name
# Added -lco RFC7946=YES to ensure modern GeoJSON standards
ogr2ogr -f GeoJSON output/debug_topology.json \
    -t_srs EPSG:4326 \
    -nln debug \
    -lco RFC7946=YES \
    "PG:host=localhost user=$DB_USER dbname=$DB_NAME password=$DB_PASS" \
    -sql "SELECT 'raw_end'::text as debug_type, NULL::text as dist, geom FROM debug_raw_endpoints
          UNION ALL
          SELECT 'candidate'::text as debug_type, NULL::text as dist, geom FROM debug_lake_candidates
          UNION ALL
          SELECT 'missed'::text as debug_type, dist_to_lake::text as dist, geom FROM debug_missed_snaps
          UNION ALL
          SELECT 'obstacle'::text as debug_type, NULL::text as dist, geom FROM debug_obstacles"

if [ -s output/debug_topology.json ]; then
    echo "  ✓ Debug JSON created ($(stat -c%s output/debug_topology.json) bytes)"
else
    echo "  ❌ Debug JSON creation failed (File is empty or missing)"
fi
          
if [ -s output/debug_topology.json ]; then
    echo "  ✓ Debug JSON created"
else
    echo "  ❌ Debug JSON creation failed"
fi

echo ""
echo "Done! Check: http://localhost:8080/qa_viewer_topology.html"