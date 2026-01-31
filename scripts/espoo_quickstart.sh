#!/bin/bash
# =====================================================================
# AHTI PADDLE MAP - ESPOO QUICK START
# =====================================================================
# Automated setup and processing for Espoo test area
# Run this on your Debian 12 Mac Mini
# =====================================================================

set -e  # Exit on error

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                                                               ║"
echo "║     AHTI PADDLE MAP - ESPOO QUICK START                      ║"
echo "║                                                               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

# Configuration
DB_NAME="ahti_staging"
DB_USER="ahti_builder"
DB_PASS="ahti_secret_password"
PIPELINE_DIR="$HOME/ahti-pipeline"

# Espoo Bounding Box
# Geographic (WGS84): 24.6-25.1°E, 60.1-60.3°N
# Web Mercator (EPSG:3857) - in meters
MIN_X=2739000
MAX_X=2795000
MIN_Y=8406000
MAX_Y=8429000

# =====================================================================
# STEP 1: CHECK PREREQUISITES
# =====================================================================
echo "Step 1: Checking prerequisites..."
echo ""

check_command() {
    if ! command -v $1 &> /dev/null; then
        echo "  ❌ $1 not found"
        echo "  Install with: sudo apt-get install $2"
        exit 1
    else
        echo "  ✓ $1 found"
    fi
}

check_command "psql" "postgresql postgresql-contrib"
check_command "osm2pgsql" "osm2pgsql"
check_command "python3" "python3"
check_command "wget" "wget"

echo ""

# =====================================================================
# STEP 2: CREATE DIRECTORY STRUCTURE
# =====================================================================
echo "Step 2: Creating directory structure..."
echo ""

mkdir -p "$PIPELINE_DIR"/{sql,scripts,output,data}
echo "  ✓ Directories created at $PIPELINE_DIR"
echo ""

# =====================================================================
# STEP 3: SETUP DATABASE
# =====================================================================
echo "Step 3: Setting up database..."
echo ""

# Check if database exists
if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "  ⚠ Database $DB_NAME already exists"
    read -p "  Drop and recreate? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        sudo -u postgres psql -c "DROP DATABASE IF EXISTS $DB_NAME;"
        sudo -u postgres psql -c "DROP USER IF EXISTS $DB_USER;"
    else
        echo "  Using existing database"
    fi
fi

# Create database and user if needed
if ! sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "  Creating database and user..."
    sudo -u postgres psql << SQL
CREATE DATABASE $DB_NAME;
\c $DB_NAME
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE USER $DB_USER WITH PASSWORD '$DB_PASS';
GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $DB_USER;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $DB_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $DB_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $DB_USER;
SQL
    echo "  ✓ Database created"
else
    echo "  ✓ Database exists"
fi

# Test connection
echo "  Testing connection..."
PGPASSWORD=$DB_PASS psql -h localhost -U $DB_USER -d $DB_NAME -c "SELECT PostGIS_Version();" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "  ✓ Connection successful"
else
    echo "  ❌ Connection failed"
    exit 1
fi

echo ""

# =====================================================================
# STEP 4: DOWNLOAD OSM DATA
# =====================================================================
echo "Step 4: Downloading OSM data..."
echo ""

OSM_FILE="$PIPELINE_DIR/input/finland-latest.osm.pbf"

if [ -f "$OSM_FILE" ]; then
    echo "  ✓ OSM file already exists: $OSM_FILE"
    read -p "  Re-download? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm "$OSM_FILE"
    fi
fi

if [ ! -f "$OSM_FILE" ]; then
    echo "  Downloading Finland OSM data (~400MB)..."
    echo "  This may take 5-15 minutes depending on your connection..."
    wget -c -O "$OSM_FILE" http://download.geofabrik.de/europe/finland-latest.osm.pbf
    echo "  ✓ Download complete"
else
    echo "  ✓ Using existing file"
fi

echo ""

# =====================================================================
# STEP 5: IMPORT OSM DATA
# =====================================================================
echo "Step 5: Importing OSM data..."
echo ""

# Check if already imported
TABLE_COUNT=$(PGPASSWORD=$DB_PASS psql -h localhost -U $DB_USER -d $DB_NAME -t -A -c \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'planet_osm_line';")

if [ "$TABLE_COUNT" -gt 0 ]; then
    echo "  ⚠ OSM tables already exist"
    read -p "  Re-import? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "  ✓ Using existing OSM data"
        echo ""
        # Skip to next step
        TABLE_COUNT=1
    else
        TABLE_COUNT=0
    fi
fi

if [ "$TABLE_COUNT" -eq 0 ]; then
    echo "  Importing OSM data (this will take 5-15 minutes)..."
    echo "  You can monitor progress in another terminal with: htop"
    echo ""
    
    # Import with conservative settings for 2010 Mac Mini
    PGPASSWORD=$DB_PASS osm2pgsql \
        --create \
        --slim \
        --drop \
        --cache 512 \
        --number-processes 1 \
        --hstore \
        --multi-geometry \
        --database $DB_NAME \
        --username $DB_USER \
        --host localhost \
        --port 5432 \
        "$OSM_FILE"
    
    echo ""
    echo "  ✓ Import complete"
    echo ""
    
    # Verify import
    echo "  Verifying import..."
    PGPASSWORD=$DB_PASS psql -h localhost -U $DB_USER -d $DB_NAME << 'SQL'
\echo '  Checking data in Espoo area:'
SELECT 
    'Waterway lines' as type, 
    COUNT(*) as count 
FROM planet_osm_line 
WHERE waterway IS NOT NULL 
AND way && ST_Transform(ST_MakeEnvelope(24.6, 60.1, 25.1, 60.3, 4326), 3857)
UNION ALL
SELECT 
    'Water polygons', 
    COUNT(*) 
FROM planet_osm_polygon 
WHERE (natural = 'water' OR waterway = 'riverbank')
AND way && ST_Transform(ST_MakeEnvelope(24.6, 60.1, 25.1, 60.3, 4326), 3857)
UNION ALL
SELECT 
    'POI points', 
    COUNT(*) 
FROM planet_osm_point 
WHERE (tourism IS NOT NULL OR amenity IS NOT NULL OR leisure IS NOT NULL)
AND way && ST_Transform(ST_MakeEnvelope(24.6, 60.1, 25.1, 60.3, 4326), 3857);
SQL
fi

echo ""

# =====================================================================
# STEP 6: COPY PIPELINE SCRIPTS
# =====================================================================
echo "Step 6: Setting up pipeline scripts..."
echo ""

# Copy the master pipeline script
if [ ! -f "$PIPELINE_DIR/sql/master_pipeline.sql" ]; then
    echo "  ⚠ master_pipeline.sql not found in $PIPELINE_DIR/sql/"
    echo "  Please copy it from the outputs directory"
    echo ""
    read -p "  Press Enter when ready to continue..."
fi

# Copy QA scripts
if [ ! -f "$PIPELINE_DIR/sql/qa_comprehensive.sql" ]; then
    echo "  ⚠ qa_comprehensive.sql not found in $PIPELINE_DIR/sql/"
    echo "  Please copy it from the outputs directory"
    echo ""
    read -p "  Press Enter when ready to continue..."
fi

# Copy export script
if [ ! -f "$PIPELINE_DIR/scripts/qa_export_with_debug.sh" ]; then
    echo "  ⚠ qa_export_with_debug.sh not found in $PIPELINE_DIR/scripts/"
    echo "  Please copy it from the outputs directory"
    echo ""
    read -p "  Press Enter when ready to continue..."
fi

# Copy CORS server
if [ ! -f "$PIPELINE_DIR/scripts/cors_server.py" ]; then
    cat > "$PIPELINE_DIR/scripts/cors_server.py" << 'CORS'
from http.server import HTTPServer, SimpleHTTPRequestHandler
import sys

class CORSRequestHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()

if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    server_address = ('', port)
    httpd = HTTPServer(server_address, CORSRequestHandler)
    print(f"Serving with CORS enabled on port {port}...")
    httpd.serve_forever()
CORS
    echo "  ✓ CORS server created"
fi

echo ""

# =====================================================================
# STEP 7: RUN PIPELINE
# =====================================================================
echo "Step 7: Running pipeline for Espoo area..."
echo ""
echo "  Bounding Box:"
echo "    Geographic: 24.6-25.1°E, 60.1-60.3°N"
echo "    Web Mercator: X($MIN_X-$MAX_X) Y($MIN_Y-$MAX_Y)"
echo ""

if [ ! -f "$PIPELINE_DIR/sql/master_pipeline.sql" ]; then
    echo "  ❌ master_pipeline.sql not found!"
    echo "  Cannot proceed without pipeline script"
    exit 1
fi

read -p "  Start pipeline processing? (Y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Nn]$ ]]; then
    echo "  Running pipeline (this may take 5-15 minutes)..."
    echo "  Logs will be saved to /tmp/pipeline_espoo.log"
    echo ""
    
    PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
        -v min_x=$MIN_X -v min_y=$MIN_Y -v max_x=$MAX_X -v max_y=$MAX_Y \
        -f "$PIPELINE_DIR/sql/master_pipeline.sql" 2>&1 | tee /tmp/pipeline_espoo.log
    
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo ""
        echo "  ✓ Pipeline complete!"
    else
        echo ""
        echo "  ❌ Pipeline failed - check /tmp/pipeline_espoo.log"
        exit 1
    fi
else
    echo "  Skipping pipeline execution"
fi

echo ""

# =====================================================================
# STEP 8: RUN QA CHECKS
# =====================================================================
echo "Step 8: Running QA checks..."
echo ""

if [ ! -f "$PIPELINE_DIR/sql/qa_comprehensive.sql" ]; then
    echo "  ⚠ qa_comprehensive.sql not found, skipping QA"
else
    PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
        -f "$PIPELINE_DIR/sql/qa_comprehensive.sql" 2>&1 | tee /tmp/qa_espoo.log
    
    echo ""
    echo "  ✓ QA checks complete"
    echo "  HTML report available at: /tmp/qa_report.html"
fi

echo ""

# =====================================================================
# STEP 9: EXPORT DATA
# =====================================================================
echo "Step 9: Exporting data..."
echo ""

if [ ! -f "$PIPELINE_DIR/scripts/qa_export_with_debug.sh" ]; then
    echo "  ⚠ qa_export_with_debug.sh not found, skipping export"
else
    cd "$PIPELINE_DIR"
    bash scripts/qa_export_with_debug.sh
    echo "  ✓ Data exported to $PIPELINE_DIR/output/local_data_qa.json"
fi

echo ""

# =====================================================================
# STEP 10: START SERVER
# =====================================================================
echo "Step 10: Starting web server..."
echo ""

# Kill any existing server
pkill -f "cors_server.py" 2>/dev/null || true

# Start server
cd "$PIPELINE_DIR/output"
python3 "$PIPELINE_DIR/scripts/cors_server.py" 8080 > /dev/null 2>&1 &
SERVER_PID=$!

echo "  ✓ Server started on port 8080 (PID: $SERVER_PID)"
echo ""

# =====================================================================
# COMPLETION SUMMARY
# =====================================================================
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                                                               ║"
echo "║     SETUP COMPLETE!                                           ║"
echo "║                                                               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "Results:"
echo "  • Pipeline directory: $PIPELINE_DIR"
echo "  • Database: $DB_NAME"
echo "  • Output file: $PIPELINE_DIR/output/local_data_qa.json"
echo "  • QA report: /tmp/qa_report.html"
echo "  • Pipeline log: /tmp/pipeline_espoo.log"
echo ""
echo "Access the data:"
echo "  • Web server: http://localhost:8080"
echo "  • Viewer: http://localhost:8080/qa_viewer.html"
echo ""
echo "If you're on a headless server, copy files to view locally:"
echo "  scp user@your-server:$PIPELINE_DIR/output/* ."
echo "  scp user@your-server:/tmp/qa_report.html ."
echo ""
echo "Useful commands:"
echo "  • View segments: psql -h localhost -U $DB_USER -d $DB_NAME -c 'SELECT COUNT(*) FROM paddling_segments;'"
echo "  • Stop server: kill $SERVER_PID"
echo "  • Re-run pipeline: cd $PIPELINE_DIR && bash scripts/espoo_quickstart.sh"
echo ""
echo "Enjoy exploring Espoo's waterways! 🚣‍♂️"
echo ""
