#!/bin/bash
DB_NAME="ahti_staging"
DB_USER="ahti_builder"

# 1. RUN PREP ONCE (Crucial for 2GB RAM)
echo "📦 Pre-calculating static data (Land Cover, POIs)..."
psql -h localhost -d $DB_NAME -U $DB_USER -f ~/ahti-pipeline/sql/00_prep_static.sql

# 2. START LOOP
# (Keep your existing coordinates configuration)
MIN_X=2100000; MAX_X=3600000; MIN_Y=8300000; MAX_Y=11000000
CHUNK_SIZE=50000 

echo "🚀 Starting Detailed Build (Rivers Only)..."

for (( y=$MIN_Y; y<$MAX_Y; y+=$CHUNK_SIZE )); do
    for (( x=$MIN_X; x<$MAX_X; x+=$CHUNK_SIZE )); do
        NEXT_X=$((x + CHUNK_SIZE))
        NEXT_Y=$((y + CHUNK_SIZE))
        
        echo "📡 Processing Chunk: X[$x] Y[$y]"
        
        # USE THE NEW DETAILED CHUNK SCRIPT
        psql -h localhost -d $DB_NAME -U $DB_USER \
             -v min_x=$x -v min_y=$y -v max_x=$NEXT_X -v max_y=$NEXT_Y \
             -f ~/ahti-pipeline/sql/detailed_chunk.sql
    done
done
echo "✅ Build Complete!"