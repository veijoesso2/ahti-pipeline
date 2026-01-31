#!/bin/bash
# =====================================================================
# CLEANUP AND RE-RUN PIPELINE WITH ESPOO BBOX AND CUSTOM VAR FIXES
# =====================================================================

DB_NAME="ahti_staging"
DB_USER="ahti_builder"
DB_PASS="ahti_secret_password"

# UPDATED: Correct Espoo bounding box from successful test_pipeline.sh
MIN_X=2726000
MAX_X=2768000
MIN_Y=8408000
MAX_Y=8481000

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║  CLEANUP AND RE-RUN WITH FIXES (ESPOO BBOX)                  ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║  CLEANUP AND RE-RUN WITH FIXES                               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

echo "This will:"
echo "  1. Clean up the broken pipeline tables"
echo "  2. Re-run with the fixed pipeline script"
echo "  3. Run QA checks"
echo "  4. Export clean data"
echo ""

read -p "Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo "Step 1: Stopping export if still running..."
# The export might be hung - kill it
pkill -f "qa_export_with_debug" 2>/dev/null && echo "  ✓ Killed hung export process" || echo "  ✓ No export running"

echo ""
echo "Step 2: Cleaning up pipeline tables..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
-- Drop all pipeline tables to start fresh
DROP TABLE IF EXISTS paddling_segments CASCADE;
DROP TABLE IF EXISTS paddling_areas_stats CASCADE;
DROP TABLE IF EXISTS paddling_network CASCADE;
DROP TABLE IF EXISTS paddling_areas CASCADE;
DROP TABLE IF EXISTS candidate_objects CASCADE;
DROP TABLE IF EXISTS official_routes CASCADE;
DROP TABLE IF EXISTS rapids_features CASCADE;
DROP TABLE IF EXISTS paddling_pois CASCADE;
DROP TABLE IF EXISTS land_cover CASCADE;
DROP TABLE IF EXISTS qa_results CASCADE;
DROP TABLE IF EXISTS qa_issues CASCADE;
DROP TABLE IF EXISTS qa_statistics CASCADE;

-- Verify cleanup
\echo 'Pipeline tables dropped. Remaining tables:'
\dt
SQL

if [ $? -eq 0 ]; then
    echo "  ✓ Cleanup complete"
else
    echo "  ❌ Cleanup failed"
    exit 1
fi

echo ""
echo "Step 3: Running FIXED pipeline..."
echo "  (Running with BBox: X($MIN_X-$MAX_X) Y($MIN_Y-$MAX_Y))"

if [ ! -f "$HOME/ahti-pipeline/sql/master_pipeline_fixed.sql" ]; then
    echo "  ❌ master_pipeline_fixed.sql not found!"
    exit 1
fi
# UPDATED: Now uses the custom.variable syntax and psql variables together
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
    -v min_x="$MIN_X" -v min_y="$MIN_Y" -v max_x="$MAX_X" -v max_y="$MAX_Y" \
    -c "SET custom.min_x = '$MIN_X'; SET custom.min_y = '$MIN_Y'; SET custom.max_x = '$MAX_X'; SET custom.max_y = '$MAX_Y';" \
    -f "$HOME/ahti-pipeline/sql/master_pipeline_fixed.sql" 2>&1 | tee /tmp/pipeline_fixed.log
    
if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo ""
    echo "  ✓ Pipeline complete!"
else
    echo ""
    echo "  ❌ Pipeline failed - check /tmp/pipeline_fixed.log"
    exit 1
fi

echo ""
echo "Step 4: Quick validation check..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
\echo 'Checking data quality:'

SELECT 
    'Segments with NULL environment' as check,
    COUNT(*) as count,
    CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END as status
FROM paddling_segments WHERE environment IS NULL
UNION ALL
SELECT 
    'Segments with NULL sinuosity',
    COUNT(*),
    CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE '⚠ WARNING' END
FROM paddling_segments WHERE sinuosity IS NULL
UNION ALL
SELECT 
    'Lake crossing segments',
    COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✓ HAS DATA' ELSE '⚠ NO DATA' END
FROM paddling_segments WHERE type = 'lake_crossing';

\echo ''
\echo 'Score distribution:'
SELECT 
    environment,
    COUNT(*) as segments,
    ROUND(AVG(fun_score), 1) as avg_fun,
    ROUND(AVG(feasibility_score), 1) as avg_feas
FROM paddling_segments
GROUP BY environment
ORDER BY segments DESC;
SQL

echo ""
echo "Step 5: Running QA checks..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
    -f "$HOME/ahti-pipeline/sql/qa_comprehensive.sql" 2>&1 | tee /tmp/qa_fixed.log | tail -30

echo ""
echo "Step 6: Exporting data..."
cd "$HOME/ahti-pipeline"
bash scripts/qa_export_with_debug.sh

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║  FIXED PIPELINE COMPLETE!                                     ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "Results:"
echo "  • Fixed pipeline log: /tmp/pipeline_fixed.log"
echo "  • QA report: /tmp/qa_report.html"
echo "  • Data: ~/ahti-pipeline/output/local_data_qa.json"
echo ""
echo "Check results:"
echo "  psql -h localhost -U ahti_builder -d ahti_staging -c 'SELECT * FROM qa_results;'"
echo ""
echo "View data:"
echo "  http://localhost:8080/qa_viewer.html"
echo ""
