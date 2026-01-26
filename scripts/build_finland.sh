#!/bin/bash
# =================================================================
# AHTI PIPELINE: FINLAND NATIONAL BUILD (DETAILED SCORING)
# Optimized for: Mac Mini 2010 (2GB RAM)
# =================================================================

# DATABASE CONFIGURATION
DB_NAME="ahti_staging"
DB_USER="ahti_builder"

# FINLAND BOUNDING BOX (EPSG:3857 meters)
MIN_X=2100000  # West (Turku/Aland)
MAX_X=3600000  # East (Russian border)
MIN_Y=8300000  # South (Hanko)
MAX_Y=11000000 # North (Lapland)

# 50km chunks are efficient for your Mac Mini's limited RAM
CHUNK_SIZE=50000 

echo "🚀 Starting Detailed Finland Build..."

# 1. RUN PREP ONCE (Crucial for 2GB RAM)
# This calculates Land Cover and POIs once so the loop doesn't have to.
echo "📦 Step 1: Pre-calculating static data (Land Cover, POIs)..."
# psql -h localhost -d $DB_NAME -U $DB_USER -f ~/ahti-pipeline/sql/00_prep_static.sql

echo "📡 Step 2: Beginning Spatial Loop..."

# Loop through Latitude (South to North)
for (( y=$MIN_Y; y<$MAX_Y; y+=$CHUNK_SIZE )); do
    # Loop through Longitude (West to East)
    for (( x=$MIN_X; x<$MAX_X; x+=$CHUNK_SIZE )); do
        
        NEXT_X=$((x + CHUNK_SIZE))
        NEXT_Y=$((y + CHUNK_SIZE))
        
        echo "📡 Processing Chunk: X[$x to $NEXT_X] Y[$y to $NEXT_Y]"
        
        # 2. RUN THE DETAILED CHUNK SCRIPT
        # This script filters for rivers/rapids and applies detailed scoring logic.
        psql -h localhost -d $DB_NAME -U $DB_USER \
             -v min_x=$x -v min_y=$y -v max_x=$NEXT_X -v max_y=$NEXT_Y \
             -f ~/ahti-pipeline/sql/detailed_chunk.sql
             
        # 3. EXPORT DATA INCREMENTALLY
        # This updates your local_data.json so you can monitor progress in real-time.
        echo "📦 Chunk complete. Updating viewer data..."
        bash ~/ahti-pipeline/scripts/pipeline_export.sh         
        
    done
done

echo "✅ Whole Finland Build Complete!"