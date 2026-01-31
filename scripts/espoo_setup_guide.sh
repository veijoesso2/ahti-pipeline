#!/bin/bash
# =====================================================================
# AHTI PADDLE MAP - ESPOO TEST AREA SETUP & PROCESSING GUIDE
# =====================================================================
# For: Mac Mini C2D 2010 running Debian 12 headless server
# Target: Espoo area, Finland (24.6-25.1°E, 60.1-60.3°N)
# =====================================================================

cat << 'EOF'
╔═══════════════════════════════════════════════════════════════════╗
║                                                                   ║
║     AHTI PADDLE MAP - ESPOO TEST AREA PROCESSING                 ║
║     Complete Setup & Execution Guide                             ║
║                                                                   ║
╚═══════════════════════════════════════════════════════════════════╝

SYSTEM INFO
-----------
Hardware: Mac Mini Core 2 Duo 2010
OS: Debian 12 (headless)
Target Area: Espoo, Finland
Bounding Box: 24.6-25.1°E, 60.1-60.3°N
Expected Processing Time: 5-15 minutes

═══════════════════════════════════════════════════════════════════

STEP 1: INSTALL PREREQUISITES
═══════════════════════════════════════════════════════════════════
EOF

echo "Checking prerequisites..."

# Check PostgreSQL
if ! command -v psql &> /dev/null; then
    echo "❌ PostgreSQL not found. Installing..."
    echo "sudo apt-get update"
    echo "sudo apt-get install -y postgresql postgresql-contrib"
else
    echo "✓ PostgreSQL found: $(psql --version)"
fi

# Check PostGIS
if ! psql -U postgres -c "SELECT PostGIS_Version();" 2>/dev/null | grep -q "POSTGIS"; then
    echo "❌ PostGIS not found. Installing..."
    echo "sudo apt-get install -y postgis postgresql-15-postgis-3"
else
    echo "✓ PostGIS installed"
fi

# Check osm2pgsql
if ! command -v osm2pgsql &> /dev/null; then
    echo "❌ osm2pgsql not found. Installing..."
    echo "sudo apt-get install -y osm2pgsql"
else
    echo "✓ osm2pgsql found: $(osm2pgsql --version | head -1)"
fi

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 not found. Installing..."
    echo "sudo apt-get install -y python3"
else
    echo "✓ Python3 found: $(python3 --version)"
fi

# Check wget or curl
if ! command -v wget &> /dev/null && ! command -v curl &> /dev/null; then
    echo "❌ wget/curl not found. Installing..."
    echo "sudo apt-get install -y wget"
else
    echo "✓ Download tool available"
fi

cat << 'EOF'

═══════════════════════════════════════════════════════════════════
STEP 2: CREATE DATABASE & USER
═══════════════════════════════════════════════════════════════════
EOF

cat << 'DBSETUP'
# Run these commands as postgres user:
sudo -u postgres psql << 'SQL'

-- Create database
DROP DATABASE IF EXISTS ahti_staging;
CREATE DATABASE ahti_staging;

-- Connect to database
\c ahti_staging

-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Create user
DROP USER IF EXISTS ahti_builder;
CREATE USER ahti_builder WITH PASSWORD 'ahti_secret_password';

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE ahti_staging TO ahti_builder;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ahti_builder;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ahti_builder;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO ahti_builder;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO ahti_builder;

-- Verify
\l ahti_staging
\du ahti_builder
SELECT PostGIS_Version();

SQL

# Test connection
echo "Testing database connection..."
PGPASSWORD='ahti_secret_password' psql -h localhost -U ahti_builder -d ahti_staging -c "SELECT version();"

if [ $? -eq 0 ]; then
    echo "✓ Database connection successful"
else
    echo "❌ Database connection failed"
    exit 1
fi

DBSETUP

cat << 'EOF'

═══════════════════════════════════════════════════════════════════
STEP 3: DOWNLOAD OSM DATA FOR ESPOO AREA
═══════════════════════════════════════════════════════════════════
EOF

cat << 'DOWNLOAD'
# Create working directory
mkdir -p ~/ahti-pipeline/data
cd ~/ahti-pipeline/data

# Option A: Download Finland extract (recommended for testing multiple areas)
echo "Downloading Finland OSM data (~400MB)..."
wget -c http://download.geofabrik.de/europe/finland-latest.osm.pbf

# Option B: Use Overpass API to get only Espoo area (smaller, but may timeout)
# echo "Downloading Espoo area from Overpass API..."
# wget -O espoo.osm "https://overpass-api.de/api/map?bbox=24.6,60.1,25.1,60.3"

echo "✓ OSM data downloaded: $(ls -lh *.osm.pbf)"

DOWNLOAD

cat << 'EOF'

═══════════════════════════════════════════════════════════════════
STEP 4: IMPORT OSM DATA WITH osm2pgsql
═══════════════════════════════════════════════════════════════════
EOF

cat << 'IMPORT'
# Import OSM data
# Note: Using slim mode for better performance on constrained hardware
echo "Importing OSM data (this will take 5-10 minutes)..."

osm2pgsql \
    --create \
    --slim \
    --drop \
    --cache 512 \
    --number-processes 1 \
    --hstore \
    --multi-geometry \
    --database ahti_staging \
    --username ahti_builder \
    --host localhost \
    --port 5432 \
    --password \
    ~/ahti-pipeline/data/finland-latest.osm.pbf

# Password: ahti_secret_password

# Verify import
echo "Verifying import..."
PGPASSWORD='ahti_secret_password' psql -h localhost -U ahti_builder -d ahti_staging << 'SQL'

SELECT 'Lines (waterways):', COUNT(*) FROM planet_osm_line WHERE waterway IS NOT NULL;
SELECT 'Polygons (lakes):', COUNT(*) FROM planet_osm_polygon WHERE natural = 'water' OR waterway = 'riverbank';
SELECT 'Points (POIs):', COUNT(*) FROM planet_osm_point WHERE tourism IS NOT NULL OR amenity IS NOT NULL;

-- Check Espoo area specifically
SELECT 'Espoo waterways:', COUNT(*) 
FROM planet_osm_line 
WHERE waterway IS NOT NULL 
AND way && ST_Transform(
    ST_MakeEnvelope(24.6, 60.1, 25.1, 60.3, 4326), 
    3857
);

SQL

IMPORT

cat << 'EOF'

═══════════════════════════════════════════════════════════════════
STEP 5: SET UP PIPELINE DIRECTORY STRUCTURE
═══════════════════════════════════════════════════════════════════
EOF

cat << 'SETUP'
# Create directory structure
mkdir -p ~/ahti-pipeline/{sql,scripts,output}

echo "✓ Directory structure created:"
tree -L 2 ~/ahti-pipeline/ 2>/dev/null || ls -R ~/ahti-pipeline/

SETUP

cat << 'EOF'

═══════════════════════════════════════════════════════════════════
STEP 6: CREATE SQL SCRIPTS
═══════════════════════════════════════════════════════════════════
EOF

echo "Creating SQL scripts..."
echo "You'll need to copy these from the pipeline repository:"
echo "  - 01_schema.sql"
echo "  - 02_extract_candidates.sql"
echo "  - 03_create_pa_schema.sql"
echo "  - 04_aggregate_rivers.sql"
echo "  - 05_lake_connectors.sql"
echo "  - 06_network_analysis.sql"
echo "  - 07_pipeline_analysis.sql (or 07_segmentation_scoring.sql)"
echo "  - 08_enrich_amenities.sql"
echo "  - 09_advanced_scoring.sql"
echo "  - 10_refine_scoring.sql"
echo "  - 11_final_scoring.sql"
echo "  - 12_precise_landuse.sql"
echo ""
echo "Place these in ~/ahti-pipeline/sql/"

cat << 'EOF'

═══════════════════════════════════════════════════════════════════
STEP 7: CREATE ESPOO-SPECIFIC PROCESSING SCRIPT
═══════════════════════════════════════════════════════════════════
EOF

# Create Espoo processing script
cat > ~/ahti-pipeline/scripts/process_espoo.sh << 'ESPOO_SCRIPT'
#!/bin/bash
# =====================================================================
# AHTI PADDLE MAP - ESPOO AREA PROCESSOR
# =====================================================================

DB_NAME="ahti_staging"
DB_USER="ahti_builder"
DB_PASS="ahti_secret_password"

# Espoo Bounding Box in EPSG:3857 (Web Mercator meters)
# Converted from 24.6-25.1°E, 60.1-60.3°N
MIN_X=2739000
MAX_X=2795000
MIN_Y=8406000
MAX_Y=8429000

echo "╔════════════════════════════════════════════════════════════╗"
echo "║  Processing Espoo Area Paddle Routes                      ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Bounding Box (EPSG:3857):"
echo "  X: $MIN_X to $MAX_X (East-West)"
echo "  Y: $MIN_Y to $MAX_Y (South-North)"
echo ""

# Function to run SQL and check for errors
run_sql() {
    local step_num=$1
    local step_name=$2
    local sql_file=$3
    
    echo "[$step_num] $step_name..."
    
    PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER \
        -v min_x=$MIN_X -v min_y=$MIN_Y -v max_x=$MAX_X -v max_y=$MAX_Y \
        -f "$sql_file" 2>&1 | tee "/tmp/pipeline_step_${step_num}.log"
    
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo "    ✓ Success"
    else
        echo "    ❌ Failed - check /tmp/pipeline_step_${step_num}.log"
        exit 1
    fi
    echo ""
}

# Start processing
echo "Starting pipeline at $(date)"
start_time=$(date +%s)

# Execute pipeline steps
run_sql 1 "Creating Schema" ~/ahti-pipeline/sql/01_schema.sql
run_sql 2 "Extracting Candidates" ~/ahti-pipeline/sql/02_extract_candidates.sql
run_sql 3 "Creating Paddling Area Schema" ~/ahti-pipeline/sql/03_create_pa_schema.sql
run_sql 4 "Aggregating Rivers" ~/ahti-pipeline/sql/04_aggregate_rivers.sql
run_sql 5 "Creating Lake Connectors" ~/ahti-pipeline/sql/05_lake_connectors.sql
run_sql 6 "Network Analysis" ~/ahti-pipeline/sql/06_network_analysis.sql
run_sql 7 "Segmentation & Scoring" ~/ahti-pipeline/sql/07_segmentation_scoring.sql
run_sql 8 "Enriching Amenities" ~/ahti-pipeline/sql/08_enrich_amenities.sql
run_sql 9 "Advanced Scoring" ~/ahti-pipeline/sql/09_advanced_scoring.sql
run_sql 10 "Refining Scores" ~/ahti-pipeline/sql/10_refine_scoring.sql
run_sql 11 "Final Scoring" ~/ahti-pipeline/sql/11_final_scoring.sql
run_sql 12 "Precise Landuse" ~/ahti-pipeline/sql/12_precise_landuse.sql

# Calculate processing time
end_time=$(date +%s)
duration=$((end_time - start_time))
minutes=$((duration / 60))
seconds=$((duration % 60))

echo "╔════════════════════════════════════════════════════════════╗"
echo "║  Pipeline Complete!                                        ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Processing time: ${minutes}m ${seconds}s"
echo ""

# Show results summary
echo "Results Summary:"
PGPASSWORD=$DB_PASS psql -h localhost -d $DB_NAME -U $DB_USER << 'SQL'
SELECT 
    '  Total Segments' as metric, 
    COUNT(*)::text as value 
FROM paddling_segments
UNION ALL
SELECT 
    '  Total Networks', 
    COUNT(DISTINCT network_id)::text 
FROM paddling_segments 
WHERE network_id IS NOT NULL
UNION ALL
SELECT 
    '  Total Length (km)', 
    ROUND(SUM(length_m)::numeric / 1000, 2)::text 
FROM paddling_segments
UNION ALL
SELECT 
    '  Avg Fun Score', 
    ROUND(AVG(fun_score), 1)::text 
FROM paddling_segments
UNION ALL
SELECT 
    '  Avg Feasibility', 
    ROUND(AVG(feasibility_score), 1)::text 
FROM paddling_segments;
SQL

echo ""
echo "Next steps:"
echo "  1. Run QA: psql -h localhost -d $DB_NAME -U $DB_USER -f ~/ahti-pipeline/sql/qa_comprehensive.sql"
echo "  2. Export: bash ~/ahti-pipeline/scripts/qa_export_with_debug.sh"
echo "  3. View: http://localhost:8080/qa_viewer.html"
echo ""
ESPOO_SCRIPT

chmod +x ~/ahti-pipeline/scripts/process_espoo.sh

echo "✓ Espoo processing script created"

cat << 'EOF'

═══════════════════════════════════════════════════════════════════
STEP 8: RUN THE PIPELINE
═══════════════════════════════════════════════════════════════════
EOF

cat << 'RUN'
# Execute the Espoo processing pipeline
cd ~/ahti-pipeline
bash scripts/process_espoo.sh

RUN

cat << 'EOF'

═══════════════════════════════════════════════════════════════════
STEP 9: RUN QUALITY ASSURANCE
═══════════════════════════════════════════════════════════════════
EOF

cat << 'QA'
# Run QA checks
PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder \
    -f ~/ahti-pipeline/sql/qa_comprehensive.sql

# View HTML report
echo "QA Report available at: file:///tmp/qa_report.html"
echo "You can copy this to your local machine to view in a browser:"
echo "  scp user@your-mac-mini:/tmp/qa_report.html ."

QA

cat << 'EOF'

═══════════════════════════════════════════════════════════════════
STEP 10: EXPORT & VIEW RESULTS
═══════════════════════════════════════════════════════════════════
EOF

cat << 'EXPORT'
# Export with debug layers
bash ~/ahti-pipeline/scripts/qa_export_with_debug.sh

# Data is now available at:
# ~/ahti-pipeline/output/local_data_qa.json

# Start web server (if you have a GUI/browser on the server)
cd ~/ahti-pipeline/output
python3 ~/ahti-pipeline/scripts/cors_server.py 8080 &

# Access viewer:
# http://localhost:8080/qa_viewer.html

# OR copy files to your local machine for viewing:
echo "To view on another machine, copy these files:"
echo "  scp user@your-mac-mini:~/ahti-pipeline/output/* ."
echo "Then open qa_viewer.html in your browser"

EXPORT

cat << 'EOF'

═══════════════════════════════════════════════════════════════════
TROUBLESHOOTING
═══════════════════════════════════════════════════════════════════

ISSUE: "osm2pgsql: command not found"
FIX: sudo apt-get install osm2pgsql

ISSUE: "FATAL: password authentication failed"
FIX: Edit /etc/postgresql/15/main/pg_hba.conf
     Change: local all all peer
     To:     local all all md5
     Restart: sudo systemctl restart postgresql

ISSUE: "out of memory" during import
FIX: Reduce cache size: osm2pgsql --cache 256 ...
     Or swap file: sudo fallocate -l 2G /swapfile
                   sudo chmod 600 /swapfile
                   sudo mkswap /swapfile
                   sudo swapon /swapfile

ISSUE: "No such file or directory: sql/01_schema.sql"
FIX: Make sure all SQL files are in ~/ahti-pipeline/sql/
     You need to copy them from the repository

ISSUE: Pipeline runs but no segments created
FIX: Check bounding box coordinates are correct
     Verify OSM data has waterways in that area:
     psql -h localhost -U ahti_builder -d ahti_staging -c \
       "SELECT COUNT(*) FROM planet_osm_line WHERE waterway IS NOT NULL 
        AND way && ST_Transform(ST_MakeEnvelope(24.6,60.1,25.1,60.3,4326),3857);"

ISSUE: "relation 'planet_osm_line' does not exist"
FIX: OSM import failed or incomplete. Re-run osm2pgsql

═══════════════════════════════════════════════════════════════════
PERFORMANCE TIPS FOR 2010 MAC MINI
═══════════════════════════════════════════════════════════════════

1. Use minimal cache: osm2pgsql --cache 256
2. Process one area at a time (don't run full Finland)
3. Add swap space if RAM < 4GB
4. Use postgresql.conf tuning:
   shared_buffers = 256MB
   work_mem = 16MB
   maintenance_work_mem = 128MB
   effective_cache_size = 1GB

5. Monitor resources:
   htop  # CPU/memory usage
   iotop # Disk I/O
   
6. Expected times for Espoo area:
   - OSM Import: 5-10 minutes
   - Pipeline Processing: 5-15 minutes
   - QA Checks: 1-3 minutes
   - Export: < 1 minute

═══════════════════════════════════════════════════════════════════
QUICK REFERENCE COMMANDS
═══════════════════════════════════════════════════════════════════

# Check database status
PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder -c "\dt"

# See segment count
PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder -c \
  "SELECT COUNT(*) FROM paddling_segments;"

# View sample data
PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder -c \
  "SELECT name, environment, fun_score, feasibility_score FROM paddling_segments LIMIT 10;"

# Clear and restart
PGPASSWORD='ahti_secret_password' psql -h localhost -d ahti_staging -U ahti_builder -c \
  "DROP TABLE IF EXISTS paddling_segments CASCADE;"

# Re-run pipeline
bash ~/ahti-pipeline/scripts/process_espoo.sh

═══════════════════════════════════════════════════════════════════
EOF

echo ""
echo "Setup guide complete! This script has been created as reference."
echo "Follow the steps above to process your Espoo test area."
echo ""
echo "Save this guide to: ~/ahti-pipeline/ESPOO_SETUP_GUIDE.sh"
echo ""
