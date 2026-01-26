#!/bin/bash
# CONFIGURATION
DB_NAME="ahti_staging"
DB_USER="ahti_builder"

# Finland Bounding Box (Approx in EPSG:3857 meters)
# NEW COORDINATES based on successful Mankinvirta test
MIN_X=2100000  # West (Turku/Aland)
MAX_X=3600000  # East (Russian border)
MIN_Y=8300000  # South (Hanko)
MAX_Y=11000000 # North (Lapland)
CHUNK_SIZE=50000 # 50km chunks are efficient for your Mac Mini

echo "🚀 Starting Gradual Finland Build..."

# Loop through Latitude (South to North)
for (( y=$MIN_Y; y<$MAX_Y; y+=$CHUNK_SIZE )); do
    # Loop through Longitude (West to East)
    for (( x=$MIN_X; x<$MAX_X; x+=$CHUNK_SIZE )); do
        
        NEXT_X=$((x + CHUNK_SIZE))
        NEXT_Y=$((y + CHUNK_SIZE))
        
        echo "📡 Processing Chunk: X[$x to $NEXT_X] Y[$y to $NEXT_Y]"
        
        # Run the SQL for the current spatial chunk
        psql -h localhost -d $DB_NAME -U $DB_USER \
             -v min_x=$x -v min_y=$y -v max_x=$NEXT_X -v max_y=$NEXT_Y \
             -f ~/ahti-pipeline/sql/master_pipeline.sql
        echo "📦 Chunk complete. Updating viewer data..."
        bash ~/ahti-pipeline/scripts/pipeline_export.sh         
        # Optional: Export a status JSON after every few chunks to monitor progress
        # bash ~/ahti-pipeline/scripts/pipeline_export.sh
    done
done

echo "✅ Whole Finland Build Complete!"
