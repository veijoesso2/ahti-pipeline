#!/bin/bash
OUTPUT_DIR="$HOME/ahti-pipeline/output"
OUTPUT_FILE="$OUTPUT_DIR/local_data.json"
mkdir -p $OUTPUT_DIR

echo "1. Exporting Analyzed Data..."

# Capture output to check for SQL errors
JSON_DATA=$(PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder -t -A -c "
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', COALESCE(json_agg(ST_AsGeoJSON(t.*)::json), '[]'::json)
)
FROM (
    -- 1. SCORED SEGMENTS
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
        'segment' as type,
        ST_Transform(geom, 4326) as geometry 
    FROM paddling_segments

    UNION ALL

    -- 2. AREA LABELS (Now includes total_km)
    SELECT 
        'area_' || network_id as id,
        area_name || ' (' || total_km || 'km)' as name,
        'area_label' as environment,
        NULL::int as feasibility_score,
        NULL::int as fun_score,
        NULL::boolean as has_poi_signal,
        NULL::boolean as is_official_route,
        NULL::boolean as has_rapids,
        NULL as land_type,
        NULL::numeric as sinuosity,
        network_id,
        'label_point' as type,
        ST_Transform(ST_PointOnSurface(geom), 4326) as geometry
    FROM paddling_areas_stats

    UNION ALL

    -- 3. LAKES (Background)
    SELECT 
        osm_id::text as id, -- Cast to Text to match other tables
        COALESCE(name, 'Lake'), 
        'lake_route' as environment, 
        NULL::int, NULL::int, NULL::boolean, NULL::boolean, NULL::boolean, NULL, NULL::numeric,
        NULL::int as network_id,
        'lake_gray' as type, 
        ST_Transform(geom, 4326) as geometry 
    FROM candidate_objects 
    WHERE type = 'lake' AND ST_Area(geom) > 50000
) t;
")

# Basic validation: JSON must start with {
if [[ $JSON_DATA == \{* ]]; then
    echo "$JSON_DATA" > $OUTPUT_FILE
    echo "✅ Export Successful! File size: $(du -h $OUTPUT_FILE | cut -f1)"
    
    echo "2. Starting Web Server..."
    cd $OUTPUT_DIR
    if pgrep -f "cors_server.py" > /dev/null; then
        echo "   (Server is already running on port 8080)"
    else
        # Run in background so script can finish
        python3 ~/ahti-pipeline/scripts/cors_server.py 8080 &
        echo "   Server started on http://localhost:8080"
    fi
else
    echo "❌ EXPORT FAILED! Database returned:"
    echo "------------------------------------------------"
    echo "$JSON_DATA"
    echo "------------------------------------------------"
fi