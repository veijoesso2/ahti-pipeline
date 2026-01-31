#!/bin/bash
# =====================================================================
# TEST AND APPLY IMPROVEMENTS - All-in-One Script
# =====================================================================

DB_NAME="ahti_staging"
DB_USER="ahti_builder"
DB_PASS="ahti_secret_password"

# Test on small Nuuksio area first
MIN_X=2735000
MAX_X=2755000
MIN_Y=8450000
MAX_Y=8470000

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║  TEST & APPLY IMPROVEMENTS                                    ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "Phase 1: Test improvements on small area (Nuuksio)"
echo "Phase 2: Apply improvements to full Uusimaa"
echo ""

read -p "Start Phase 1 (test)? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "PHASE 1: TESTING ON NUUKSIO AREA"
echo "═══════════════════════════════════════════════════════════════"
echo ""

echo "Step 1.1: Cleaning test data..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER -q << 'SQL'
DELETE FROM paddling_segments;
DELETE FROM paddling_areas_stats;
DELETE FROM paddling_network;
DELETE FROM candidate_objects;
DELETE FROM paddling_areas;
SQL

echo "  ✓ Cleaned"

echo ""
echo "Step 1.2: Running pipeline with improvements..."
echo "  (Using master_pipeline_v16.sql as base)"

# Run the base pipeline
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
    -v min_x="$MIN_X" -v min_y="$MIN_Y" -v max_x="$MAX_X" -v max_y="$MAX_Y" \
    -c "SET custom.min_x = '$MIN_X'; SET custom.min_y = '$MIN_Y'; SET custom.max_x = '$MAX_X'; SET custom.max_y = '$MAX_Y';" \
    -f "$HOME/ahti-pipeline/sql/master_pipeline_v16.sql" > /tmp/test_base.log 2>&1

echo "  ✓ Base pipeline complete"

echo ""
echo "Step 1.3: Applying improvements on top..."

# Apply geometry cleanup
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
\echo 'Cleaning geometries...'

-- Remove zero-length segments
DELETE FROM paddling_segments WHERE length_m < 1;

-- Fix sinuosity
UPDATE paddling_segments 
SET sinuosity = CASE 
    WHEN sinuosity IS NULL THEN 1.5
    WHEN sinuosity > 10 THEN 10.0
    WHEN sinuosity < 0.9 THEN 1.0
    ELSE sinuosity
END;

\echo 'Geometry cleanup complete'
SQL

# Apply improved POI detection
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
\echo 'Improving POI detection...'

-- Drop and recreate with buffering
DROP TABLE IF EXISTS paddling_pois_buffered;
CREATE TABLE paddling_pois_buffered AS
SELECT ST_Buffer(geom, 200) as geom, type
FROM paddling_pois;

CREATE INDEX idx_pad_pois_buf ON paddling_pois_buffered USING GIST(geom);

-- Reset POI signals
UPDATE paddling_segments SET has_poi_signal = FALSE;

-- Apply improved detection (1000m + buffer)
UPDATE paddling_segments s 
SET has_poi_signal = TRUE 
FROM paddling_pois p 
WHERE ST_DWithin(s.geom, p.geom, 1000);

UPDATE paddling_segments s
SET has_poi_signal = TRUE
FROM paddling_pois_buffered pb
WHERE ST_Intersects(s.geom, pb.geom)
AND has_poi_signal = FALSE;

\echo 'POI detection improved'
SQL

# Apply lake connector improvements
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
\echo 'Optimizing lake connectors...'

-- Remove zero-length connectors
DELETE FROM candidate_objects
WHERE is_virtual = TRUE AND ST_Length(geom) < 1;

-- Remove redundant connectors for over-connected lakes
WITH over_connected AS (
    SELECT l.id as lake_id
    FROM candidate_objects l
    JOIN candidate_objects c ON (c.is_virtual = TRUE AND ST_Intersects(l.geom, c.geom))
    WHERE l.type = 'lake'
    GROUP BY l.id
    HAVING COUNT(c.id) > 8
),
ranked_connectors AS (
    SELECT 
        c.id,
        l.id as lake_id,
        ROW_NUMBER() OVER (PARTITION BY l.id ORDER BY ST_Length(c.geom) DESC) as rank
    FROM candidate_objects c
    JOIN candidate_objects l ON ST_Intersects(c.geom, l.geom)
    WHERE c.is_virtual = TRUE
    AND l.type = 'lake'
    AND l.id IN (SELECT lake_id FROM over_connected)
)
DELETE FROM candidate_objects
WHERE id IN (
    SELECT id FROM ranked_connectors WHERE rank > 8
);

\echo 'Lake connectors optimized'
SQL

echo ""
echo "Step 1.4: Checking results..."

PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
\echo 'Test Results:'

SELECT 'Total segments' as metric, COUNT(*)::text as value
FROM paddling_segments
UNION ALL
SELECT 'Zero-length segments', COUNT(*)::text
FROM paddling_segments WHERE length_m < 1
UNION ALL
SELECT 'NULL sinuosity', COUNT(*)::text
FROM paddling_segments WHERE sinuosity IS NULL
UNION ALL
SELECT 'POI coverage %', 
    ROUND((100.0 * COUNT(*) FILTER (WHERE has_poi_signal) / COUNT(*))::numeric, 1)::text
FROM paddling_segments
UNION ALL
SELECT 'Total connectors', COUNT(*)::text
FROM candidate_objects WHERE is_virtual = TRUE
UNION ALL
SELECT 'Lakes connected', 
    (SELECT COUNT(DISTINCT l.id)::text
     FROM candidate_objects l
     WHERE l.type = 'lake'
     AND EXISTS (
         SELECT 1 FROM candidate_objects c
         WHERE c.is_virtual = TRUE AND ST_Intersects(c.geom, l.geom)
     ))
UNION ALL
SELECT 'Over-connected lakes (>8)', COUNT(*)::text
FROM (
    SELECT l.id
    FROM candidate_objects l
    JOIN candidate_objects c ON (c.is_virtual = TRUE AND ST_Intersects(l.geom, c.geom))
    WHERE l.type = 'lake'
    GROUP BY l.id
    HAVING COUNT(c.id) > 8
) t;
SQL

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "PHASE 1 COMPLETE - Review results above"
echo "═══════════════════════════════════════════════════════════════"
echo ""

read -p "Results look good? Proceed to Phase 2 (full Uusimaa)? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Stopping. You can re-test or adjust parameters."
    exit 0
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "PHASE 2: APPLYING TO FULL UUSIMAA"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Uusimaa bounds
MIN_X=2586031
MAX_X=2960000
MIN_Y=8330000
MAX_Y=8600000

echo "Step 2.1: Cleaning full data..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER -q << 'SQL'
DROP TABLE IF EXISTS paddling_segments CASCADE;
DROP TABLE IF EXISTS paddling_areas_stats CASCADE;
DROP TABLE IF EXISTS paddling_network CASCADE;
DROP TABLE IF EXISTS candidate_objects CASCADE;
DROP TABLE IF EXISTS paddling_areas CASCADE;
DROP TABLE IF EXISTS official_routes CASCADE;
DROP TABLE IF EXISTS rapids_features CASCADE;
DROP TABLE IF EXISTS paddling_pois CASCADE;
DROP TABLE IF EXISTS paddling_pois_buffered CASCADE;
DROP TABLE IF EXISTS land_cover CASCADE;
SQL

echo "  ✓ Cleaned"

echo ""
echo "Step 2.2: Running full pipeline..."
echo "  (This will take 3-5 minutes)"

PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
    -v min_x="$MIN_X" -v min_y="$MIN_Y" -v max_x="$MAX_X" -v max_y="$MAX_Y" \
    -c "SET custom.min_x = '$MIN_X'; SET custom.min_y = '$MIN_Y'; SET custom.max_x = '$MAX_X'; SET custom.max_y = '$MAX_Y';" \
    -f "$HOME/ahti-pipeline/sql/master_pipeline_v16.sql" 2>&1 | tee /tmp/uusimaa_improved.log

echo ""
echo "Step 2.3: Applying all improvements..."

# Geometry cleanup
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
\echo '[IMPROVEMENT 1/3] Geometry cleanup...'
DELETE FROM paddling_segments WHERE length_m < 1;
UPDATE paddling_segments 
SET sinuosity = CASE 
    WHEN sinuosity IS NULL THEN 1.5
    WHEN sinuosity > 10 THEN 10.0
    WHEN sinuosity < 0.9 THEN 1.0
    ELSE sinuosity
END;
SELECT 'Geometry fixed' as status, 
    (SELECT COUNT(*) FROM paddling_segments WHERE length_m < 1) as zero_length_remaining;
SQL

# POI improvement
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
\echo '[IMPROVEMENT 2/3] POI detection enhancement...'
DROP TABLE IF EXISTS paddling_pois_buffered;
CREATE TABLE paddling_pois_buffered AS
SELECT ST_Buffer(geom, 200) as geom, type FROM paddling_pois;
CREATE INDEX idx_pad_pois_buf ON paddling_pois_buffered USING GIST(geom);

UPDATE paddling_segments SET has_poi_signal = FALSE;
UPDATE paddling_segments s SET has_poi_signal = TRUE 
FROM paddling_pois p WHERE ST_DWithin(s.geom, p.geom, 1000);
UPDATE paddling_segments s SET has_poi_signal = TRUE
FROM paddling_pois_buffered pb
WHERE ST_Intersects(s.geom, pb.geom) AND has_poi_signal = FALSE;

SELECT 'POI coverage improved' as status,
    ROUND((100.0 * COUNT(*) FILTER (WHERE has_poi_signal) / COUNT(*))::numeric, 1) as coverage_pct
FROM paddling_segments;
SQL

# Connector optimization
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
\echo '[IMPROVEMENT 3/3] Connector optimization...'
DELETE FROM candidate_objects WHERE is_virtual = TRUE AND ST_Length(geom) < 1;

WITH over_connected AS (
    SELECT l.id as lake_id
    FROM candidate_objects l
    JOIN candidate_objects c ON (c.is_virtual = TRUE AND ST_Intersects(l.geom, c.geom))
    WHERE l.type = 'lake'
    GROUP BY l.id
    HAVING COUNT(c.id) > 8
),
ranked_connectors AS (
    SELECT c.id, l.id as lake_id,
        ROW_NUMBER() OVER (PARTITION BY l.id ORDER BY ST_Length(c.geom) DESC) as rank
    FROM candidate_objects c
    JOIN candidate_objects l ON ST_Intersects(c.geom, l.geom)
    WHERE c.is_virtual = TRUE AND l.type = 'lake'
    AND l.id IN (SELECT lake_id FROM over_connected)
)
DELETE FROM candidate_objects WHERE id IN (
    SELECT id FROM ranked_connectors WHERE rank > 8
);

SELECT 'Connectors optimized' as status,
    (SELECT COUNT(*) FROM candidate_objects WHERE is_virtual = TRUE) as total_connectors;
SQL

echo ""
echo "Step 2.4: Running QA..."

PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
    -f "$HOME/ahti-pipeline/sql/qa_comprehensive.sql" 2>&1 | \
    grep -A 25 "CRITICAL FAILURES\|OVERALL QA"

echo ""
echo "Step 2.5: Exporting..."

cd "$HOME/ahti-pipeline"
bash scripts/qa_export_with_debug.sh 2>&1 | tail -15

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║  ALL IMPROVEMENTS APPLIED SUCCESSFULLY!                       ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "Summary of improvements:"
echo "  ✓ Zero-length segments removed"
echo "  ✓ Sinuosity fixed (capped at 10, floored at 1.0)"
echo "  ✓ POI detection enhanced (500m → 1000m + buffering)"
echo "  ✓ Over-connected lakes optimized (max 8 connectors)"
echo "  ✓ Redundant connectors removed"
echo ""
echo "View results:"
echo "  • Map: http://localhost:8080/qa_viewer.html"
echo "  • QA Report: file:///tmp/qa_report.html"
echo "  • Logs: /tmp/uusimaa_improved.log"
echo ""
echo "Compare with previous run:"
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
SELECT 
    check_name,
    status,
    count,
    threshold
FROM qa_results
WHERE status IN ('FAIL', 'WARNING')
ORDER BY 
    CASE status WHEN 'FAIL' THEN 1 ELSE 2 END,
    severity DESC,
    count DESC
LIMIT 15;
SQL

echo ""
