#!/bin/bash
# =====================================================================
# AHTI PADDLE MAP - QA-ENHANCED EXPORT SCRIPT
# =====================================================================
# Purpose: Export data with additional QA/debug layers for visual inspection
# Usage: bash qa_export_with_debug.sh
# Output: local_data_qa.json with multiple debug layers
# =====================================================================

OUTPUT_DIR="$HOME/ahti-pipeline/output"
OUTPUT_FILE="$OUTPUT_DIR/local_data_qa.json"
mkdir -p $OUTPUT_DIR

echo "=================================================="
echo "  Ahti QA-Enhanced Export"
echo "=================================================="
echo ""
echo "1. Running QA Checks..."

# Run the QA script first
PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder \
    -f ~/ahti-pipeline/sql/qa_comprehensive.sql > /tmp/qa_output.log 2>&1

if [ $? -eq 0 ]; then
    echo "   ✓ QA checks complete"
else
    echo "   ⚠ QA checks had warnings (see /tmp/qa_output.log)"
fi

echo ""
echo "2. Exporting Data with Debug Layers..."

# Capture output to check for SQL errors
JSON_DATA=$(PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder -t -A -c "
SELECT json_build_object(
    'type', 'FeatureCollection',
    'metadata', json_build_object(
        'export_timestamp', NOW()::TEXT,
        'qa_status', (
            SELECT json_build_object(
                'total_checks', COUNT(*),
                'passed', COUNT(*) FILTER (WHERE status = 'PASS'),
                'warnings', COUNT(*) FILTER (WHERE status = 'WARNING'),
                'failures', COUNT(*) FILTER (WHERE status = 'FAIL')
            )
            FROM qa_results
        ),
        'network_stats', (
            SELECT json_object_agg(stat_name, stat_value)
            FROM qa_statistics
        )
    ),
    'features', COALESCE(json_agg(ST_AsGeoJSON(t.*)::json), '[]'::json)
)
FROM (
    -- ============================================
    -- LAYER 1: SCORED SEGMENTS (Primary Data)
    -- ============================================
    SELECT 
        seg_id::text as id,
        COALESCE(name, 'Unnamed') as name, 
        environment,
        feasibility_score, 
        fun_score,         
        has_poi_signal,
        is_official_route,
        has_rapids,
        land_type,
        sinuosity,
        network_id,
        length_m::int as length_m,
        'segment' as layer_type,
        'primary' as layer_category,
        ST_Transform(geom, 4326) as geometry 
    FROM paddling_segments

    UNION ALL

    -- ============================================
    -- LAYER 2: AREA LABELS
    -- ============================================
    SELECT 
        'area_' || network_id as id,
        area_name || ' (' || total_km || 'km)' as name,
        'area_label' as environment,
        max_feasibility as feasibility_score,
        avg_fun::int as fun_score,
        NULL::boolean as has_poi_signal,
        NULL::boolean as is_official_route,
        NULL::boolean as has_rapids,
        NULL as land_type,
        NULL::numeric as sinuosity,
        network_id,
        (total_km * 1000)::int as length_m,
        'label_point' as layer_type,
        'primary' as layer_category,
        ST_Transform(ST_PointOnSurface(geom), 4326) as geometry
    FROM paddling_areas_stats

    UNION ALL

    -- ============================================
    -- LAYER 3: LAKES (Background Context)
    -- ============================================
    SELECT 
        osm_id::text as id,
        COALESCE(name, 'Lake') as name, 
        'lake_route' as environment, 
        NULL::int as feasibility_score,
        NULL::int as fun_score,
        NULL::boolean as has_poi_signal,
        NULL::boolean as is_official_route,
        NULL::boolean as has_rapids,
        NULL as land_type,
        NULL::numeric as sinuosity,
        NULL::int as network_id,
        ST_Area(geom)::int as length_m,
        'lake_gray' as layer_type,
        'background' as layer_category,
        ST_Transform(geom, 4326) as geometry 
    FROM candidate_objects 
    WHERE type = 'lake' AND ST_Area(geom) > 50000

    UNION ALL

    -- ============================================
    -- QA DEBUG LAYER 4: DANGLING ENDPOINTS
    -- ============================================
    SELECT 
        'dangling_' || i.seg_id as id,
        'Dangling Endpoint' as name,
        'qa_dangling' as environment,
        NULL::int, NULL::int, NULL::boolean, NULL::boolean, NULL::boolean,
        NULL, NULL::numeric,
        i.network_id,
        0 as length_m,
        'qa_dangling' as layer_type,
        'qa_critical' as layer_category,
        ST_Transform(i.geom, 4326) as geometry
    FROM qa_issues i
    WHERE i.issue_type = 'dangling_endpoint'

    UNION ALL

    -- ============================================
    -- QA DEBUG LAYER 5: EXTREME SCORES
    -- ============================================
    SELECT 
        'extreme_' || seg_id as id,
        'Extreme Score: ' || description as name,
        'qa_extreme_score' as environment,
        NULL::int, NULL::int, NULL::boolean, NULL::boolean, NULL::boolean,
        NULL, NULL::numeric,
        network_id,
        0 as length_m,
        'qa_extreme_score' as layer_type,
        'qa_warning' as layer_category,
        ST_Transform(ST_Centroid(geom), 4326) as geometry
    FROM qa_issues
    WHERE issue_type = 'extreme_score'

    UNION ALL

    -- ============================================
    -- QA DEBUG LAYER 6: SINUOSITY ANOMALIES
    -- ============================================
    SELECT 
        'sinuosity_' || seg_id as id,
        'Sinuosity Issue: ' || description as name,
        'qa_sinuosity' as environment,
        NULL::int, NULL::int, NULL::boolean, NULL::boolean, NULL::boolean,
        NULL, NULL::numeric,
        network_id,
        0 as length_m,
        'qa_sinuosity' as layer_type,
        'qa_warning' as layer_category,
        ST_Transform(geom, 4326) as geometry
    FROM qa_issues
    WHERE issue_type = 'sinuosity_anomaly'

    UNION ALL

    -- ============================================
    -- QA DEBUG LAYER 7: POIS (Validation)
    -- ============================================
    SELECT 
        'poi_' || ROW_NUMBER() OVER() as id,
        type || ' POI' as name,
        'qa_poi' as environment,
        NULL::int, NULL::int, NULL::boolean, NULL::boolean, NULL::boolean,
        NULL, NULL::numeric,
        NULL::int as network_id,
        0 as length_m,
        'qa_poi' as layer_type,
        'qa_reference' as layer_category,
        ST_Transform(geom, 4326) as geometry
    FROM paddling_pois

    UNION ALL

    -- ============================================
    -- QA DEBUG LAYER 8: RAPIDS FEATURES
    -- ============================================
    SELECT 
        'rapids_' || osm_id as id,
        'Rapids Feature' as name,
        'qa_rapids' as environment,
        NULL::int, NULL::int, NULL::boolean, NULL::boolean, NULL::boolean,
        NULL, NULL::numeric,
        NULL::int as network_id,
        0 as length_m,
        'qa_rapids' as layer_type,
        'qa_reference' as layer_category,
        ST_Transform(geom, 4326) as geometry
    FROM rapids_features

    UNION ALL

    -- ============================================
    -- QA DEBUG LAYER 9: OFFICIAL ROUTES (Reference)
    -- ============================================
    SELECT 
        'official_' || osm_id as id,
        'Official Canoe Route' as name,
        'qa_official' as environment,
        NULL::int, NULL::int, NULL::boolean, NULL::boolean, NULL::boolean,
        NULL, NULL::numeric,
        NULL::int as network_id,
        ST_Length(geom)::int as length_m,
        'qa_official_route' as layer_type,
        'qa_reference' as layer_category,
        ST_Transform(geom, 4326) as geometry
    FROM official_routes

) t;
")

# Basic validation: JSON must start with {
if [[ $JSON_DATA == \{* ]]; then
    echo "$JSON_DATA" > $OUTPUT_FILE
    
    FILE_SIZE=$(du -h $OUTPUT_FILE | cut -f1)
    FEATURE_COUNT=$(echo "$JSON_DATA" | jq '.features | length' 2>/dev/null || echo "unknown")
    
    echo "   ✓ Export Successful!"
    echo "     File size: $FILE_SIZE"
    echo "     Features: $FEATURE_COUNT"
    echo ""
    
    echo "3. Generating Layer Summary..."
    echo "$JSON_DATA" | jq -r '
        .features 
        | group_by(.properties.layer_category) 
        | map({
            category: .[0].properties.layer_category,
            count: length
        }) 
        | .[] 
        | "   " + .category + ": " + (.count|tostring) + " features"
    ' 2>/dev/null || echo "   (jq not available for summary)"
    
    echo ""
    echo "4. QA Summary from Database..."
    PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder -t -A -c "
    SELECT 
        '   ' || status || ': ' || COUNT(*) || ' checks'
    FROM qa_results
    GROUP BY status
    ORDER BY 
        CASE status 
            WHEN 'FAIL' THEN 1 
            WHEN 'WARNING' THEN 2 
            WHEN 'PASS' THEN 3 
            ELSE 4 
        END;
    "
    
    echo ""
    echo "5. Starting Web Server..."
    cd $OUTPUT_DIR
    if pgrep -f "cors_server.py" > /dev/null; then
        echo "   (Server already running on port 8080)"
    else
        python3 ~/ahti-pipeline/scripts/cors_server.py 8080 &
        echo "   ✓ Server started on http://localhost:8080"
    fi
    
    echo ""
    echo "=================================================="
    echo "  Export Complete!"
    echo "=================================================="
    echo ""
    echo "Files generated:"
    echo "  • Data: $OUTPUT_FILE"
    echo "  • QA Report: /tmp/qa_report.html"
    echo "  • QA Log: /tmp/qa_output.log"
    echo ""
    echo "View in browser:"
    echo "  • Data viewer: http://localhost:8080"
    echo "  • QA Report: file:///tmp/qa_report.html"
    echo ""
    echo "Database QA tables available:"
    echo "  • qa_results (check summaries)"
    echo "  • qa_issues (detailed problems)"
    echo "  • qa_statistics (network metrics)"
    echo ""
    
else
    echo "❌ EXPORT FAILED! Database returned:"
    echo "=================================================="
    echo "$JSON_DATA"
    echo "=================================================="
    exit 1
fi
