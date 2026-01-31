# AHTI PADDLE MAP - QA SYSTEM USAGE GUIDE

## Overview
Comprehensive quality assurance system for validating paddle map generation pipeline output.

## Files Created

### 1. `qa_comprehensive.sql`
Main QA script that checks data quality across 7 categories:
- Topology & Connectivity
- Scoring Validation
- Geometry & Segmentation
- Lake Connectors
- Naming & Metadata
- Enrichment Data
- Network Statistics

### 2. `qa_export_with_debug.sh`
Enhanced export script that includes QA debug layers for visual verification.

### 3. `qa_viewer.html`
Interactive map viewer with QA layer controls and segment inspection tools.

---

## Quick Start

### Step 1: Run QA Checks
```bash
cd ~/ahti-pipeline
psql -h localhost -d ahti_staging -U ahti_builder -f sql/qa_comprehensive.sql
```

This will:
- Run all quality checks
- Store results in `qa_results`, `qa_issues`, `qa_statistics` tables
- Generate HTML report at `/tmp/qa_report.html`
- Display console summary

### Step 2: Export Data with Debug Layers
```bash
chmod +x scripts/qa_export_with_debug.sh
bash scripts/qa_export_with_debug.sh
```

This will:
- Run QA checks automatically
- Export GeoJSON with QA debug layers
- Start local web server on port 8080
- Generate summary of layers and issues

### Step 3: Visual Inspection
```bash
# Copy the QA viewer to your output directory
cp ~/ahti-pipeline/qa_viewer.html ~/ahti-pipeline/output/

# Open in browser
open http://localhost:8080/qa_viewer.html
# or
firefox http://localhost:8080/qa_viewer.html
```

---

## Understanding QA Results

### Status Levels

**PASS** ✅ - Check passed, no issues
**WARNING** ⚠️ - Minor issues detected, review recommended
**FAIL** ❌ - Critical issues found, requires action
**INFO** ℹ️ - Informational metric, not a problem

### Severity Levels

1. **Info** - Statistical information, no action needed
2. **Warning** - Potential quality issue, should review
3. **Critical** - Data integrity problem, must fix

---

## QA Check Categories

### 1. Topology & Connectivity
Validates network structure and connectivity.

**Key Checks:**
- **Dangling Endpoints** - River segments not connected to network
- **Isolated Networks** - Networks with < 3 segments
- **Self-Intersecting** - Geometry errors
- **Duplicate Geometries** - Same geometry multiple times

**Common Issues:**
- Rivers that should connect but don't (snapping tolerance)
- Fragment networks from chunked processing
- Invalid geometry from OSM data

**How to Fix:**
```sql
-- Find dangling endpoints
SELECT * FROM qa_issues WHERE issue_type = 'dangling_endpoint';

-- Check if they should connect
SELECT seg_id, network_id, ST_AsText(geom) 
FROM paddling_segments 
WHERE seg_id IN (SELECT seg_id FROM qa_issues WHERE issue_type = 'dangling_endpoint');
```

### 2. Scoring Validation
Ensures scoring algorithms produce reasonable results.

**Key Checks:**
- **Extreme Scores** - Too many segments at 0 or 100
- **Score Distribution** - Standard deviation too low
- **Inconsistent Logic** - Attributes don't match scores

**Common Issues:**
- All segments scoring 100 (logic error)
- Rapids not increasing fun score
- Official routes not getting high feasibility

**How to Fix:**
```sql
-- Review extreme scores
SELECT seg_id, fun_score, feasibility_score, environment, land_type, 
       has_rapids, is_official_route, sinuosity
FROM paddling_segments
WHERE fun_score IN (0, 100)
LIMIT 20;

-- Check scoring logic
SELECT environment, land_type, 
       AVG(fun_score) as avg_fun,
       COUNT(*) as count
FROM paddling_segments
GROUP BY environment, land_type
ORDER BY avg_fun;
```

### 3. Geometry & Segmentation
Validates segment lengths and geometry calculations.

**Key Checks:**
- **Oversized Segments** - Segments > 1500m (should be ~1000m)
- **Sinuosity Anomalies** - Impossible values (< 1.0 or > 4.0)
- **Zero-Length** - Degenerate geometries

**Common Issues:**
- Chopping algorithm didn't run on some segments
- Circular geometries causing sinuosity errors
- Invalid geometries from OSM

**How to Fix:**
```sql
-- Find oversized segments
SELECT seg_id, length_m, type, ST_NumPoints(geom) as points
FROM paddling_segments
WHERE length_m > 1500 AND type = 'river'
ORDER BY length_m DESC;

-- Re-chunk if needed
-- (Re-run the segmentation step of pipeline)
```

### 4. Lake Connectors
Validates lake connection logic.

**Key Checks:**
- **Over-Connected Lakes** - Lakes with > 10 connectors
- **Unrealistic Lengths** - Connectors > 2km
- **Unconnected Lakes** - Significant lakes with no connections

**Common Issues:**
- Star connector logic creating too many connections
- Large lakes getting connectors when they shouldn't
- Missing connections in lake-heavy areas

**How to Fix:**
```sql
-- Find over-connected lakes
WITH lake_stats AS (
    SELECT l.id, l.name, COUNT(s.seg_id) as connector_count
    FROM candidate_objects l
    LEFT JOIN paddling_segments s ON (
        s.type IN ('lake_crossing', 'star_connector')
        AND ST_Intersects(l.geom, s.geom)
    )
    WHERE l.type = 'lake'
    GROUP BY l.id, l.name
)
SELECT * FROM lake_stats WHERE connector_count > 10;

-- Review lake connector logic in 05_lake_connectors.sql
```

### 5. Naming & Metadata
Validates naming and metadata completeness.

**Key Checks:**
- **Unnamed Networks** - Networks > 5km without names
- **Missing Stats** - Networks without area stats
- **Environment Classification** - Segments without environment

**How to Fix:**
```sql
-- Find unnamed networks
SELECT network_id, COUNT(*) as segments, 
       SUM(length_m)/1000 as total_km
FROM paddling_segments
WHERE network_id IS NOT NULL
GROUP BY network_id
HAVING MAX(CASE WHEN name NOT LIKE 'Unnamed%' THEN 1 ELSE 0 END) = 0
AND SUM(length_m)/1000 > 5;

-- Check OSM data for names
-- May need to improve name extraction logic
```

### 6. Enrichment Data
Validates POI detection, land classification, rapids.

**Key Checks:**
- **POI Coverage** - % of segments near POIs
- **Land Classification** - % classified vs "mixed"
- **Rapids Detection** - Segments vs OSM rapids features

**Common Issues:**
- POI proximity distance too small/large
- Land cover polygons missing in OSM
- Rapids features not properly detected

**How to Fix:**
```sql
-- Check POI distribution
SELECT 
    has_poi_signal,
    COUNT(*) as segments,
    ROUND(AVG(feasibility_score), 1) as avg_feasibility
FROM paddling_segments
GROUP BY has_poi_signal;

-- Review land type coverage
SELECT land_type, COUNT(*) as count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
FROM paddling_segments
WHERE type = 'river'
GROUP BY land_type;
```

---

## Using the QA Viewer

### Layer Controls

**Primary Layers** (always useful):
- ✓ River Segments - Main paddling routes colored by fun score
- ✓ Area Labels - Network names and distances
- ✓ Lakes - Background context

**QA Debug Layers** (enable when investigating):
- Dangling Endpoints - Shows connectivity problems (red circles)
- Extreme Scores - Segments at min/max scores (orange circles)
- Sinuosity Issues - Geometry calculation problems (purple dashed)

**Reference Layers** (for validation):
- POIs - Shows all POI locations (green circles)
- Rapids Features - OSM rapids/waterfalls (cyan circles)
- Official Routes - OSM canoe routes (purple lines)

### Inspector Panel

Click any segment to see:
- Name and environment classification
- Fun and feasibility scores with visual bars
- Length and sinuosity
- Land type
- Features (rapids, official status, nearby POIs)

Use this to:
- Verify scores make sense for the location
- Check if official routes are properly detected
- Validate POI proximity logic
- Spot-check land type classification

### Keyboard Shortcuts

- Click segment → Open inspector
- Click background → Close inspector
- "Reset View" button → Return to Finland view
- "View QA Report" → Open HTML report in new tab

---

## Interpreting Results

### Good Results
```
✅ QA Status: PASSED
- Passed: 18/21 checks
- Warnings: 3 (all < threshold)
- Failures: 0
```

**Action:** Data quality is acceptable, proceed with deployment.

### Needs Review
```
⚠️ QA Status: NEEDS REVIEW
- Passed: 15/21 checks
- Warnings: 6
- Failures: 0
```

**Action:** Review warnings, most are likely acceptable but verify.

### Failed
```
❌ QA Status: FAILED
- Passed: 12/21 checks
- Warnings: 4
- Failures: 5
```

**Action:** Review failures table, fix critical issues, re-run pipeline.

---

## Common Workflows

### Workflow 1: Post-Pipeline Validation
```bash
# 1. Run pipeline on a chunk
bash scripts/build_finland.sh

# 2. Run QA immediately
psql -h localhost -d ahti_staging -U ahti_builder -f sql/qa_comprehensive.sql

# 3. Check console output
# If PASS → Continue to next chunk
# If FAIL → Fix issues before continuing

# 4. Export with debug layers
bash scripts/qa_export_with_debug.sh

# 5. Spot-check in viewer
open http://localhost:8080/qa_viewer.html
```

### Workflow 2: Investigating Specific Issues
```bash
# 1. Enable relevant debug layers in viewer
# Example: Enable "Dangling Endpoints" layer

# 2. Zoom to problem area

# 3. Query database for details
psql -h localhost -d ahti_staging -U ahti_builder -c "
  SELECT * FROM qa_issues 
  WHERE issue_type = 'dangling_endpoint' 
  AND ST_Intersects(
    geom, 
    ST_Transform(ST_MakeEnvelope(25.0, 61.0, 26.0, 62.0, 4326), 3857)
  );
"

# 4. Fix in SQL or adjust pipeline logic
```

### Workflow 3: Score Calibration
```bash
# 1. Export sample area with QA layers
bash scripts/qa_export_with_debug.sh

# 2. In viewer, inspect 10-20 diverse segments
#    - High sinuosity forest rivers
#    - Straight field rivers
#    - Lake crossings
#    - Urban sections

# 3. Note if scores seem too high/low

# 4. Adjust scoring formulas in SQL:
#    - Edit sql/11_final_scoring.sql
#    - Modify bonus/penalty values
#    - Re-run pipeline on test area
#    - Repeat until scores feel right
```

---

## Troubleshooting

### "No QA tables found"
```bash
# QA script hasn't been run yet
psql -h localhost -d ahti_staging -U ahti_builder -f sql/qa_comprehensive.sql
```

### "Cannot load local_data_qa.json"
```bash
# Export script hasn't been run
bash scripts/qa_export_with_debug.sh

# Or server isn't running
python3 ~/ahti-pipeline/scripts/cors_server.py 8080 &
```

### "Too many failures"
```sql
-- Get details on what's failing
SELECT category, check_name, count, notes 
FROM qa_results 
WHERE status = 'FAIL'
ORDER BY severity DESC;

-- Focus on highest severity first
SELECT * FROM qa_issues 
WHERE check_id IN (
    SELECT check_id FROM qa_results WHERE severity = 3
)
LIMIT 20;
```

### "Scores seem wrong everywhere"
```sql
-- Check if scoring ran at all
SELECT 
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE fun_score IS NOT NULL) as has_fun,
    COUNT(*) FILTER (WHERE feasibility_score IS NOT NULL) as has_feas
FROM paddling_segments;

-- If NULL, scoring didn't run - check pipeline logs

-- Check scoring formulas
SELECT environment, land_type, has_rapids, is_official_route,
       MIN(fun_score) as min_fun, 
       MAX(fun_score) as max_fun,
       AVG(fun_score) as avg_fun,
       COUNT(*) as count
FROM paddling_segments
GROUP BY environment, land_type, has_rapids, is_official_route
ORDER BY avg_fun DESC;
```

---

## Tips for Best Results

1. **Run QA after every pipeline change** - Catch regressions early

2. **Focus on failures first** - Warnings can often be ignored if < threshold

3. **Use visual verification** - Numbers don't lie, but maps show context

4. **Compare to OSM** - Enable reference layers to validate against source

5. **Test on known areas** - Pick places you know for sanity checks

6. **Document patterns** - If certain warnings always appear, adjust thresholds

7. **Keep QA report history** - Track quality over time as pipeline evolves

---

## Integration with Pipeline

### Add to build_finland.sh
```bash
# After each chunk processes
psql -h localhost -d ahti_staging -U ahti_builder -f ~/ahti-pipeline/sql/qa_comprehensive.sql

# Check for failures
FAIL_COUNT=$(psql -h localhost -d ahti_staging -U ahti_builder -t -A -c \
    "SELECT COUNT(*) FROM qa_results WHERE status = 'FAIL';")

if [ "$FAIL_COUNT" -gt "0" ]; then
    echo "⚠️  QA FAILURES DETECTED - Review before continuing"
    # Optional: pause for review
    # read -p "Press enter to continue..."
fi
```

### Scheduled QA Runs
```bash
# Add to crontab for nightly validation
0 2 * * * cd /home/ahti/ahti-pipeline && psql -h localhost -d ahti_staging -U ahti_builder -f sql/qa_comprehensive.sql && bash scripts/qa_export_with_debug.sh
```

---

## Customization

### Adjust Thresholds

Edit `qa_comprehensive.sql` to change what counts as a failure:

```sql
-- Example: More strict on dangling endpoints
INSERT INTO qa_results (...)
SELECT ...
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN COUNT(*) < 5 THEN 'WARNING'  -- was 10
        ELSE 'FAIL'
    END,
    ...
```

### Add Custom Checks

```sql
-- Add your own check to qa_comprehensive.sql
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH my_check AS (
    SELECT ... -- your check logic
)
SELECT 
    'Custom',
    'My Check Name',
    CASE ... END,
    COUNT(*),
    threshold_value,
    'Description of what this checks',
    2
FROM my_check;
```

### Add Debug Layers

```javascript
// In qa_viewer.html, add new layer
map.addLayer({
    id: 'my-debug-layer',
    type: 'circle', // or 'line', 'fill'
    source: 'paddle-data',
    filter: ['==', ['get', 'layer_type'], 'my_debug_type'],
    paint: { ... }
});
```

---

## Support

For issues or questions:
1. Check `/tmp/qa_output.log` for error messages
2. Review HTML report at `/tmp/qa_report.html`
3. Query `qa_issues` table for specific problem details
4. Enable debug layers in viewer for visual investigation

---

**Remember:** QA is a tool to help you, not a blocker. Use judgment when interpreting results!
