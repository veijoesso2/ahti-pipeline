#!/bin/bash
# =====================================================================
# QUICK FIX: Lake Connectors + Map Visibility
# =====================================================================

DB_NAME="ahti_staging"
DB_USER="ahti_builder"
DB_PASS="ahti_secret_password"

# Uusimaa Region (approximate)
MIN_X=2586031
MAX_X=2960000
MIN_Y=8330000
MAX_Y=8600000

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║  QUICK FIX: Improved Lake Connectors + Visibility            ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "This will:"
echo "  1. Clean up existing pipeline"
echo "  2. Re-run with improved lake connector logic"
echo "  3. Create proper portage links between lakes"
echo "  4. Update viewer with light background"
echo "  5. Export and view"
echo ""

read -p "Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo "Step 1: Cleaning up..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER -q << 'SQL'
DROP TABLE IF EXISTS paddling_segments CASCADE;
DROP TABLE IF EXISTS paddling_areas_stats CASCADE;
DROP TABLE IF EXISTS paddling_network CASCADE;
DROP TABLE IF EXISTS candidate_objects CASCADE;
DROP TABLE IF EXISTS paddling_areas CASCADE;
DROP TABLE IF EXISTS official_routes CASCADE;
DROP TABLE IF EXISTS rapids_features CASCADE;
DROP TABLE IF EXISTS paddling_pois CASCADE;
DROP TABLE IF EXISTS land_cover CASCADE;
SQL

echo "  ✓ Cleanup complete"

echo ""
echo "Step 2: Running improved pipeline..."

if [ ! -f "$HOME/ahti-pipeline/sql/master_pipeline_v16.sql" ]; then
    echo "  ❌ master_pipeline_v16.sql not found!"
    echo "  Please copy it to ~/ahti-pipeline/sql/"
    exit 1
fi

PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
    -v min_x="$MIN_X" -v min_y="$MIN_Y" -v max_x="$MAX_X" -v max_y="$MAX_Y" \
    -c "SET custom.min_x = '$MIN_X'; SET custom.min_y = '$MIN_Y'; SET custom.max_x = '$MAX_X'; SET custom.max_y = '$MAX_Y';" \
    -f "$HOME/ahti-pipeline/sql/master_pipeline_v16.sql" 2>&1 | tee /tmp/pipeline_v16.log

if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo ""
    echo "  ✓ Pipeline complete!"
else
    echo ""
    echo "  ❌ Pipeline failed"
    exit 1
fi

echo ""
echo "Step 3: Checking lake connectors..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
\echo 'Connector Summary:'
SELECT 
    CASE 
        WHEN name LIKE 'Star%' THEN 'Star Connectors'
        WHEN name LIKE 'Portage%' THEN 'Portage Links'
        ELSE 'Other'
    END as type,
    COUNT(*) as count,
    ROUND(AVG(ST_Length(geom))::numeric) as avg_length_m,
    ROUND(MIN(ST_Length(geom))::numeric) as min_length_m,
    ROUND(MAX(ST_Length(geom))::numeric) as max_length_m
FROM candidate_objects
WHERE is_virtual = TRUE
GROUP BY 
    CASE 
        WHEN name LIKE 'Star%' THEN 'Star Connectors'
        WHEN name LIKE 'Portage%' THEN 'Portage Links'
        ELSE 'Other'
    END;

\echo ''
\echo 'Lake Network Coverage:'
SELECT 
    'Total lakes' as metric,
    COUNT(*) as value
FROM candidate_objects WHERE type = 'lake'
UNION ALL
SELECT 
    'Lakes with connectors',
    COUNT(DISTINCT l.id)
FROM candidate_objects l
WHERE l.type = 'lake'
AND EXISTS (
    SELECT 1 FROM candidate_objects c
    WHERE c.is_virtual = TRUE
    AND ST_Intersects(c.geom, l.geom)
);
SQL

echo ""
echo "Step 4: Running QA..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
    -f "$HOME/ahti-pipeline/sql/qa_comprehensive.sql" 2>&1 | grep -A 20 "CRITICAL FAILURES\|OVERALL QA"

echo ""
echo "Step 5: Exporting with updated viewer..."

# Copy updated viewer
if [ -f "$HOME/ahti-pipeline/output/qa_viewer.html" ]; then
    cp "$HOME/ahti-pipeline/output/qa_viewer.html" "$HOME/ahti-pipeline/output/qa_viewer_old.html"
    echo "  ✓ Backed up old viewer"
fi

# Export data
cd "$HOME/ahti-pipeline"
bash scripts/qa_export_with_debug.sh 2>&1 | tail -20

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║  FIX COMPLETE!                                                ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "Changes made:"
echo "  ✓ Improved lake connector logic (creates portage links)"
echo "  ✓ Increased POI detection radius (300m → 500m)"
echo "  ✓ Light grayscale background map"
echo "  ✓ Better contrast colors (easier to see)"
echo ""
echo "View results:"
echo "  http://localhost:8080/qa_viewer.html"
echo ""
echo "Compare QA results:"
echo "  psql -h localhost -U ahti_builder -d ahti_staging -c \"
echo "    SELECT check_name, status, count 
echo "    FROM qa_results 
echo "    WHERE status IN ('FAIL', 'WARNING')
echo "    ORDER BY severity DESC, count DESC;
echo "  \""
echo ""
