#!/bin/bash
# =====================================================================
# RUN PIPELINE BY REGION (CHUNKING STRATEGY)
# =====================================================================
# Usage: ./run_finland_regions.sh [region_name]
# Regions: south, west, east, central, lapland_w, lapland_e
# =====================================================================

DB_NAME="ahti_staging"
DB_USER="ahti_builder"
DB_PASS="ahti_secret_password"

# 1. CHECK INPUT
REGION=$1

if [ -z "$REGION" ]; then
    echo "Usage: ./run_finland_regions.sh [region]"
    echo "Available regions:"
    echo "  south      (Helsinki, Turku, Kotka)"
    echo "  west       (Tampere, Vaasa, Pori)"
    echo "  east       (Lakeland, Kuopio, Joensuu)"
    echo "  central    (Oulu, Kajaani)"
    echo "  lapland_w  (Rovaniemi, Western Lapland)"
    echo "  lapland_e  (Kuusamo, Eastern Lapland)"
    exit 1
fi

# 2. SET BOUNDING BOXES (EPSG:3857)
# Note: Coordinates overlap slightly to ensure connections aren't cut

case $REGION in
  "south")
    echo "📍 Selected Region: SOUTH (Uusimaa, Varsinais-Suomi, Kymenlaakso)"
    MIN_X=2100000; MAX_X=3100000
    MIN_Y=8200000; MAX_Y=8600000
    ;;
    
  "west")
    echo "📍 Selected Region: WEST (Pirkanmaa, Satakunta, Ostrobothnia)"
    MIN_X=2100000; MAX_X=2750000
    MIN_Y=8600000; MAX_Y=9200000
    ;;

  "east")
    echo "📍 Selected Region: EAST (Lakeland, Savo, Karelia)"
    MIN_X=2750000; MAX_X=3600000
    MIN_Y=8600000; MAX_Y=9200000
    ;;

  "central")
    echo "📍 Selected Region: CENTRAL (Oulu, Kainuu)"
    MIN_X=2300000; MAX_X=3400000
    MIN_Y=9200000; MAX_Y=9800000
    ;;

  "lapland_w")
    echo "📍 Selected Region: LAPLAND WEST"
    MIN_X=2200000; MAX_X=2850000
    MIN_Y=9800000; MAX_Y=11150000
    ;;

  "lapland_e")
    echo "📍 Selected Region: LAPLAND EAST"
    MIN_X=2850000; MAX_X=3600000
    MIN_Y=9800000; MAX_Y=11150000
    ;;

  *)
    echo "❌ Unknown region: $REGION"
    exit 1
    ;;
esac

echo "   BBox: X($MIN_X - $MAX_X) Y($MIN_Y - $MAX_Y)"
echo ""

# 3. CLEAN DATABASE
echo "Step 1: Cleaning database..."
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

# 4. RUN PIPELINE
echo "Step 2: Running pipeline for $REGION..."
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
    -v min_x="$MIN_X" -v min_y="$MIN_Y" -v max_x="$MAX_X" -v max_y="$MAX_Y" \
    -c "SET custom.min_x = '$MIN_X'; SET custom.min_y = '$MIN_Y'; SET custom.max_x = '$MAX_X'; SET custom.max_y = '$MAX_Y';" \
    -f "$HOME/ahti-pipeline/sql/master_pipeline_v3.sql" > /tmp/pipeline_${REGION}.log 2>&1

if [ $? -eq 0 ]; then
    echo "  ✓ Pipeline finished successfully"
else
    echo "  ❌ Pipeline failed! Check /tmp/pipeline_${REGION}.log"
    exit 1
fi

# 5. EXPORT DATA
echo "Step 3: Exporting JSON..."
cd "$HOME/ahti-pipeline"
# Run the standard export script
bash scripts/qa_export_with_debug.sh > /dev/null 2>&1

# 6. RENAME OUTPUT
# This prevents the next run from overwriting this one
if [ -f "output/local_data_qa.json" ]; then
    mv "output/local_data_qa.json" "output/finland_${REGION}.json"
    echo "  ✓ Data saved to: output/finland_${REGION}.json"
else
    echo "  ⚠ Warning: JSON export file not found."
fi

echo ""
echo "Region $REGION complete!"