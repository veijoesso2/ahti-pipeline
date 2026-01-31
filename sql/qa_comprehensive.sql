-- =====================================================================
-- AHTI PADDLE MAP PIPELINE - COMPREHENSIVE QA SCRIPT
-- =====================================================================
-- Purpose: Automated data quality verification for paddling network
-- Usage: psql -h localhost -d ahti_staging -U ahti_builder -f qa_comprehensive.sql
-- Output: HTML report and console summary
-- =====================================================================

\timing on
\set QUIET on

-- Drop existing QA infrastructure
DROP TABLE IF EXISTS qa_results CASCADE;
DROP TABLE IF EXISTS qa_issues CASCADE;
DROP TABLE IF EXISTS qa_statistics CASCADE;

-- Create QA results table
CREATE TABLE qa_results (
    check_id SERIAL PRIMARY KEY,
    category TEXT,
    check_name TEXT,
    status TEXT, -- PASS, WARNING, FAIL, INFO
    count INTEGER,
    threshold INTEGER,
    notes TEXT,
    severity INTEGER, -- 1=Info, 2=Warning, 3=Critical
    timestamp TIMESTAMP DEFAULT NOW()
);

-- Create detailed issues table for failed checks
CREATE TABLE qa_issues (
    issue_id SERIAL PRIMARY KEY,
    check_id INTEGER REFERENCES qa_results(check_id),
    seg_id INTEGER,
    network_id INTEGER,
    issue_type TEXT,
    description TEXT,
    geom GEOMETRY(Geometry, 3857),
    suggested_fix TEXT
);

-- Create statistics table
CREATE TABLE qa_statistics (
    stat_name TEXT PRIMARY KEY,
    stat_value NUMERIC,
    stat_text TEXT,
    category TEXT
);

\set QUIET off
\echo '============================================================='
\echo '  AHTI PADDLE MAP QA - Starting Quality Checks'
\echo '============================================================='
\echo ''

-- =====================================================================
-- SECTION 1: TOPOLOGY & CONNECTIVITY
-- =====================================================================
\echo '[1/7] Running Topology & Connectivity Checks...'

-- 1.1: Dangling Endpoints (segments not connected to network)
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH dangling AS (
    SELECT s1.seg_id, s1.network_id, s1.type, s1.geom
    FROM paddling_segments s1
    WHERE s1.type NOT IN ('portage_link', 'lake_crossing') -- These can dangle
    AND NOT EXISTS (
        SELECT 1 
        FROM paddling_segments s2 
        WHERE s1.seg_id != s2.seg_id 
        AND (ST_DWithin(ST_StartPoint(s1.geom), s2.geom, 5) 
             OR ST_DWithin(ST_EndPoint(s1.geom), s2.geom, 5))
    )
)
SELECT 
    'Topology',
    'Dangling River Endpoints',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN COUNT(*) < 10 THEN 'WARNING'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    10,
    'River segments with unconnected endpoints (excludes portages/lake crossings)',
    CASE WHEN COUNT(*) < 10 THEN 2 ELSE 3 END
FROM dangling;

-- Store dangling segment details
INSERT INTO qa_issues (check_id, seg_id, network_id, issue_type, description, geom, suggested_fix)
SELECT 
    (SELECT check_id FROM qa_results WHERE check_name = 'Dangling River Endpoints'),
    s1.seg_id,
    s1.network_id,
    'dangling_endpoint',
    'Segment has no neighbors within 5m',
    ST_Union(ST_StartPoint(s1.geom), ST_EndPoint(s1.geom)),
    'Check if this should connect to network_id ' || s1.network_id
FROM paddling_segments s1
WHERE s1.type NOT IN ('portage_link', 'lake_crossing')
AND NOT EXISTS (
    SELECT 1 FROM paddling_segments s2 
    WHERE s1.seg_id != s2.seg_id 
    AND (ST_DWithin(ST_StartPoint(s1.geom), s2.geom, 5) 
         OR ST_DWithin(ST_EndPoint(s1.geom), s2.geom, 5))
);

-- 1.2: Isolated Networks (networks with < 3 segments)
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH network_sizes AS (
    SELECT network_id, COUNT(*) as seg_count
    FROM paddling_segments
    WHERE network_id IS NOT NULL
    GROUP BY network_id
    HAVING COUNT(*) < 3
)
SELECT 
    'Topology',
    'Isolated/Tiny Networks',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN COUNT(*) < 20 THEN 'WARNING'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    20,
    'Networks with fewer than 3 segments (may be fragments)',
    2
FROM network_sizes;

-- 1.3: Self-intersecting segments
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH self_intersecting AS (
    SELECT seg_id, network_id, geom
    FROM paddling_segments
    WHERE NOT ST_IsSimple(geom)
)
SELECT 
    'Topology',
    'Self-Intersecting Segments',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    0,
    'Segments with self-intersections (geometry errors)',
    3
FROM self_intersecting;

-- 1.4: Duplicate segments (same geometry)
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH duplicates AS (
    SELECT ST_AsText(geom) as geom_text, COUNT(*) as dup_count
    FROM paddling_segments
    GROUP BY geom
    HAVING COUNT(*) > 1
)
SELECT 
    'Topology',
    'Duplicate Geometries',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN COUNT(*) < 5 THEN 'WARNING'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    5,
    'Segments with identical geometries',
    2
FROM duplicates;

-- =====================================================================
-- SECTION 2: SCORING VALIDATION
-- =====================================================================
\echo '[2/7] Running Scoring Validation Checks...'

-- 2.1: Extreme Fun Scores
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH extreme_scores AS (
    SELECT seg_id, fun_score, feasibility_score
    FROM paddling_segments
    WHERE fun_score IN (0, 100)
)
SELECT 
    'Scoring',
    'Extreme Fun Scores (0 or 100)',
    CASE 
        WHEN COUNT(*) < (SELECT COUNT(*) * 0.05 FROM paddling_segments) THEN 'PASS'
        WHEN COUNT(*) < (SELECT COUNT(*) * 0.15 FROM paddling_segments) THEN 'WARNING'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    (SELECT COUNT(*) * 0.15 FROM paddling_segments)::INTEGER,
    'Segments at min/max fun score (>15% suspicious)',
    2
FROM extreme_scores;

-- Store extreme score details
INSERT INTO qa_issues (check_id, seg_id, network_id, issue_type, description, geom, suggested_fix)
SELECT 
    (SELECT check_id FROM qa_results WHERE check_name = 'Extreme Fun Scores (0 or 100)'),
    seg_id,
    network_id,
    'extreme_score',
    'Fun=' || fun_score || ', Feas=' || feasibility_score || 
    ', Env=' || environment || ', Land=' || land_type,
    geom,
    'Review scoring logic for this environment/land_type combination'
FROM paddling_segments
WHERE fun_score IN (0, 100);

-- 2.2: Score Distribution Analysis
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH score_stats AS (
    SELECT 
        STDDEV(fun_score) as fun_stddev,
        STDDEV(feasibility_score) as feas_stddev
    FROM paddling_segments
)
SELECT 
    'Scoring',
    'Score Distribution Variance',
    CASE 
        WHEN fun_stddev > 15 AND feas_stddev > 15 THEN 'PASS'
        WHEN fun_stddev > 10 OR feas_stddev > 10 THEN 'WARNING'
        ELSE 'FAIL'
    END,
    ROUND(fun_stddev)::INTEGER,
    15,
    'Fun StdDev=' || ROUND(fun_stddev,1) || ', Feas StdDev=' || ROUND(feas_stddev,1) || 
    ' (low variance indicates poor differentiation)',
    2
FROM score_stats;

-- 2.3: Inconsistent Score Components
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH inconsistent AS (
    SELECT seg_id, fun_score, feasibility_score, has_rapids, is_official_route, 
           land_type, environment, sinuosity
    FROM paddling_segments
    WHERE (
        -- Has rapids but low fun score
        (has_rapids = TRUE AND fun_score < 70)
        OR
        -- Official route but low feasibility
        (is_official_route = TRUE AND feasibility_score < 90)
        OR
        -- High sinuosity but not reflected in score
        (sinuosity > 1.5 AND fun_score < 80 AND type = 'river')
    )
)
SELECT 
    'Scoring',
    'Inconsistent Score Logic',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN COUNT(*) < (SELECT COUNT(*) * 0.05 FROM paddling_segments) THEN 'WARNING'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    (SELECT COUNT(*) * 0.05 FROM paddling_segments)::INTEGER,
    'Segments where attributes dont match scores',
    2
FROM inconsistent;

-- =====================================================================
-- SECTION 3: GEOMETRY & SEGMENTATION
-- =====================================================================
\echo '[3/7] Running Geometry & Segmentation Checks...'

-- 3.1: Segment Length Outliers
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH length_outliers AS (
    SELECT seg_id, length_m, type
    FROM paddling_segments
    WHERE length_m > 1500 AND type = 'river' -- Should be max 1000m after chopping
)
SELECT 
    'Geometry',
    'Oversized Segments (>1500m)',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN COUNT(*) < 10 THEN 'WARNING'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    10,
    'River segments exceeding expected 1km chunks',
    2
FROM length_outliers;

-- 3.2: Sinuosity Anomalies
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH sinuosity_issues AS (
    SELECT seg_id, sinuosity, length_m, type
    FROM paddling_segments
    WHERE (
        (sinuosity > 4.0 AND type = 'river') -- Extremely winding
        OR (sinuosity < 1.0) -- Impossible (would mean shorter than straight line)
        OR (sinuosity IS NULL)
    )
)
SELECT 
    'Geometry',
    'Sinuosity Anomalies',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN COUNT(*) < 5 THEN 'WARNING'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    5,
    'Segments with impossible or extreme sinuosity values',
    2
FROM sinuosity_issues;

-- Store sinuosity issues
INSERT INTO qa_issues (check_id, seg_id, network_id, issue_type, description, geom, suggested_fix)
SELECT 
    (SELECT check_id FROM qa_results WHERE check_name = 'Sinuosity Anomalies'),
    seg_id,
    network_id,
    'sinuosity_anomaly',
    'Sinuosity=' || COALESCE(sinuosity::TEXT, 'NULL') || ', Length=' || ROUND(length_m) || 'm',
    geom,
    'Recalculate sinuosity or check for circular geometry'
FROM paddling_segments
WHERE (sinuosity > 4.0 AND type = 'river') OR (sinuosity < 1.0) OR (sinuosity IS NULL);

-- 3.3: Zero-length segments
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH zero_length AS (
    SELECT seg_id
    FROM paddling_segments
    WHERE length_m < 1.0
)
SELECT 
    'Geometry',
    'Zero-Length Segments',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    0,
    'Segments with length < 1m (degenerate geometry)',
    3
FROM zero_length;

-- =====================================================================
-- SECTION 4: LAKE CONNECTORS
-- =====================================================================
\echo '[4/7] Running Lake Connector Validation...'

-- 4.1: Over-connected Lakes
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH lake_connections AS (
    SELECT 
        l.id as lake_id,
        l.name as lake_name,
        COUNT(s.seg_id) as connector_count
    FROM candidate_objects l
    LEFT JOIN paddling_segments s ON (
        s.type IN ('lake_crossing', 'star_connector')
        AND ST_Intersects(l.geom, s.geom)
    )
    WHERE l.type = 'lake'
    GROUP BY l.id, l.name
    HAVING COUNT(s.seg_id) > 10
)
SELECT 
    'Lake Connectors',
    'Over-Connected Lakes',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN COUNT(*) < 5 THEN 'WARNING'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    5,
    'Lakes with >10 star connectors (likely over-segmented)',
    2
FROM lake_connections;

-- 4.2: Unrealistic Connector Lengths
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH long_connectors AS (
    SELECT seg_id, length_m, type, name
    FROM paddling_segments
    WHERE type IN ('lake_crossing', 'portage_link', 'star_connector')
    AND length_m > 2000 -- Connectors shouldn't be > 2km
)
SELECT 
    'Lake Connectors',
    'Unrealistic Connector Length',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN COUNT(*) < 10 THEN 'WARNING'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    10,
    'Lake/portage connectors longer than 2km',
    2
FROM long_connectors;

-- 4.3: Lakes Without Connections
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH unconnected_lakes AS (
    SELECT l.id, l.name, ST_Area(l.geom) as area
    FROM candidate_objects l
    WHERE l.type = 'lake'
    AND ST_Area(l.geom) > 50000 -- Significant lakes only
    AND NOT EXISTS (
        SELECT 1 FROM paddling_segments s
        WHERE ST_Intersects(l.geom, s.geom)
    )
)
SELECT 
    'Lake Connectors',
    'Unconnected Significant Lakes',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN COUNT(*) < (SELECT COUNT(*) * 0.2 FROM candidate_objects WHERE type='lake') THEN 'WARNING'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    (SELECT COUNT(*) * 0.2 FROM candidate_objects WHERE type='lake')::INTEGER,
    'Large lakes with no river or connector segments',
    2
FROM unconnected_lakes;

-- =====================================================================
-- SECTION 5: NAMING & METADATA
-- =====================================================================
\echo '[5/7] Running Naming & Metadata Checks...'

-- 5.1: Unnamed Networks
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH unnamed_networks AS (
    SELECT network_id, COUNT(*) as segment_count,
           SUM(length_m)/1000 as total_km
    FROM paddling_segments
    WHERE network_id IS NOT NULL
    GROUP BY network_id
    HAVING MAX(CASE WHEN name NOT LIKE 'Unnamed%' THEN 1 ELSE 0 END) = 0
    AND SUM(length_m)/1000 > 5 -- Only care about networks > 5km
)
SELECT 
    'Metadata',
    'Unnamed Significant Networks',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        WHEN COUNT(*) < (SELECT COUNT(DISTINCT network_id) * 0.2 FROM paddling_segments) THEN 'WARNING'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    (SELECT COUNT(DISTINCT network_id) * 0.2 FROM paddling_segments)::INTEGER,
    'Networks >5km with no named segments',
    2
FROM unnamed_networks;

-- 5.2: Missing Area Stats
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH networks_without_stats AS (
    SELECT DISTINCT network_id
    FROM paddling_segments
    WHERE network_id IS NOT NULL
    AND network_id NOT IN (SELECT network_id FROM paddling_areas_stats WHERE network_id IS NOT NULL)
)
SELECT 
    'Metadata',
    'Networks Missing Area Stats',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'WARNING'
    END,
    COUNT(*)::INTEGER,
    0,
    'Networks without corresponding paddling_areas_stats records',
    2
FROM networks_without_stats;

-- 5.3: Environment Classification Coverage
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH unclassified AS (
    SELECT seg_id
    FROM paddling_segments
    WHERE environment IS NULL OR environment = ''
)
SELECT 
    'Metadata',
    'Missing Environment Classification',
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END,
    COUNT(*)::INTEGER,
    0,
    'Segments without environment classification',
    3
FROM unclassified;

-- =====================================================================
-- SECTION 6: ENRICHMENT DATA
-- =====================================================================
\echo '[6/7] Running Enrichment Data Checks...'

-- 6.1: POI Detection Coverage
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH poi_stats AS (
    SELECT 
        COUNT(*) as total_segments,
        SUM(CASE WHEN has_poi_signal THEN 1 ELSE 0 END) as segments_with_poi,
        (SELECT COUNT(*) FROM paddling_pois) as total_pois
    FROM paddling_segments
)
SELECT 
    'Enrichment',
    'POI Signal Coverage',
    CASE 
        WHEN segments_with_poi::FLOAT / total_segments > 0.15 THEN 'PASS'
        WHEN segments_with_poi::FLOAT / total_segments > 0.05 THEN 'WARNING'
        ELSE 'FAIL'
    END,
    segments_with_poi,
    (total_segments * 0.15)::INTEGER,
    ROUND((segments_with_poi::FLOAT / total_segments * 100)::NUMERIC, 1)::TEXT || 
    '% of segments near POIs (' || total_pois || ' total POIs)',
    2
FROM poi_stats;

-- 6.2: Land Type Classification Coverage
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH land_coverage AS (
    SELECT 
        COUNT(*) as total_segments,
        SUM(CASE WHEN land_type = 'mixed' THEN 1 ELSE 0 END) as mixed_segments,
        SUM(CASE WHEN land_type = 'forest' THEN 1 ELSE 0 END) as forest_segments,
        SUM(CASE WHEN land_type = 'field' THEN 1 ELSE 0 END) as field_segments,
        SUM(CASE WHEN land_type = 'urban' THEN 1 ELSE 0 END) as urban_segments
    FROM paddling_segments
    WHERE type = 'river' -- Only rivers should have land classification
)
SELECT 
    'Enrichment',
    'Land Type Classification Rate',
    CASE 
        WHEN mixed_segments::FLOAT / total_segments < 0.5 THEN 'PASS'
        WHEN mixed_segments::FLOAT / total_segments < 0.7 THEN 'WARNING'
        ELSE 'FAIL'
    END,
    mixed_segments,
    (total_segments * 0.5)::INTEGER,
    ROUND((mixed_segments::FLOAT / total_segments * 100)::NUMERIC, 1)::TEXT || 
    '% as mixed (F:' || forest_segments || ', Fld:' || field_segments || ', U:' || urban_segments || ')',
    2
FROM land_coverage;

-- 6.3: Rapids Detection vs POI Data
INSERT INTO qa_results (category, check_name, status, count, threshold, notes, severity)
WITH rapids_stats AS (
    SELECT 
        COUNT(*) as segments_with_rapids,
        (SELECT COUNT(*) FROM rapids_features) as rapids_features_count
    FROM paddling_segments
    WHERE has_rapids = TRUE
)
SELECT 
    'Enrichment',
    'Rapids Detection Quality',
    CASE 
        WHEN rapids_features_count > 0 AND segments_with_rapids > rapids_features_count * 0.5 THEN 'PASS'
        WHEN rapids_features_count > 0 THEN 'WARNING'
        ELSE 'INFO'
    END,
    segments_with_rapids,
    rapids_features_count,
    segments_with_rapids::TEXT || ' segments marked with rapids from ' || 
    rapids_features_count::TEXT || ' OSM rapids features',
    1
FROM rapids_stats;

-- =====================================================================
-- SECTION 7: NETWORK STATISTICS
-- =====================================================================
\echo '[7/7] Generating Network Statistics...'

-- Overall network metrics
INSERT INTO qa_statistics (stat_name, stat_value, stat_text, category)
SELECT 
    'Total Networks',
    COUNT(DISTINCT network_id),
    COUNT(DISTINCT network_id)::TEXT || ' connected networks',
    'Network'
FROM paddling_segments WHERE network_id IS NOT NULL;

INSERT INTO qa_statistics (stat_name, stat_value, stat_text, category)
SELECT 
    'Total Segments',
    COUNT(*),
    COUNT(*)::TEXT || ' total segments',
    'Network'
FROM paddling_segments;

INSERT INTO qa_statistics (stat_name, stat_value, stat_text, category)
SELECT 
    'Total Network Length (km)',
    ROUND(SUM(length_m)::NUMERIC / 1000, 2),
    ROUND(SUM(length_m)::NUMERIC / 1000, 2)::TEXT || ' km total navigable waterways',
    'Network'
FROM paddling_segments;

INSERT INTO qa_statistics (stat_name, stat_value, stat_text, category)
SELECT 
    'Avg Segments per Network',
    ROUND(AVG(segment_count), 1),
    ROUND(AVG(segment_count), 1)::TEXT || ' segments per network (median: ' || 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY segment_count)::TEXT || ')',
    'Network'
FROM (SELECT network_id, COUNT(*) as segment_count 
      FROM paddling_segments WHERE network_id IS NOT NULL GROUP BY network_id) t;

-- Score statistics
INSERT INTO qa_statistics (stat_name, stat_value, stat_text, category)
SELECT 
    'Fun Score - Mean',
    ROUND(AVG(fun_score), 1),
    'μ=' || ROUND(AVG(fun_score), 1) || ', σ=' || ROUND(STDDEV(fun_score), 1) || 
    ', min=' || MIN(fun_score) || ', max=' || MAX(fun_score),
    'Scoring'
FROM paddling_segments;

INSERT INTO qa_statistics (stat_name, stat_value, stat_text, category)
SELECT 
    'Feasibility Score - Mean',
    ROUND(AVG(feasibility_score), 1),
    'μ=' || ROUND(AVG(feasibility_score), 1) || ', σ=' || ROUND(STDDEV(feasibility_score), 1) ||
    ', min=' || MIN(feasibility_score) || ', max=' || MAX(feasibility_score),
    'Scoring'
FROM paddling_segments;

-- Type distribution
INSERT INTO qa_statistics (stat_name, stat_value, stat_text, category)
SELECT 
    'Type Distribution',
    NULL,
    'River: ' || SUM(CASE WHEN type='river' THEN 1 ELSE 0 END) ||
    ', Lake: ' || SUM(CASE WHEN type IN ('lake_crossing','star_connector') THEN 1 ELSE 0 END) ||
    ', Portage: ' || SUM(CASE WHEN type='portage_link' THEN 1 ELSE 0 END),
    'Network'
FROM paddling_segments;

-- Environment distribution
INSERT INTO qa_statistics (stat_name, stat_value, stat_text, category)
SELECT 
    'Environment Distribution',
    NULL,
    'River: ' || SUM(CASE WHEN environment='river' THEN 1 ELSE 0 END) ||
    ', Official: ' || SUM(CASE WHEN environment='river_official' THEN 1 ELSE 0 END) ||
    ', Lake: ' || SUM(CASE WHEN environment='lake_route' THEN 1 ELSE 0 END) ||
    ', Portage: ' || SUM(CASE WHEN environment='portage' THEN 1 ELSE 0 END),
    'Network'
FROM paddling_segments;

-- =====================================================================
-- GENERATE REPORTS
-- =====================================================================
\echo ''
\echo '============================================================='
\echo '  QA RESULTS SUMMARY'
\echo '============================================================='

-- Console summary
\echo ''
\echo '--- CRITICAL FAILURES ---'
SELECT 
    category || ' > ' || check_name as check,
    count || ' issues (threshold: ' || threshold || ')' as result,
    notes
FROM qa_results 
WHERE status = 'FAIL'
ORDER BY severity DESC, category;

\echo ''
\echo '--- WARNINGS ---'
SELECT 
    category || ' > ' || check_name as check,
    count || ' issues (threshold: ' || threshold || ')' as result,
    notes
FROM qa_results 
WHERE status = 'WARNING'
ORDER BY category;

\echo ''
\echo '--- PASSED CHECKS ---'
SELECT 
    category || ' > ' || check_name as check,
    'PASSED' as result
FROM qa_results 
WHERE status = 'PASS'
ORDER BY category;

\echo ''
\echo '--- NETWORK STATISTICS ---'
SELECT 
    stat_name || ': ' || COALESCE(stat_value::TEXT, stat_text) as statistic
FROM qa_statistics
ORDER BY category, stat_name;

\echo ''
\echo '============================================================='
\echo '  OVERALL QA STATUS'
\echo '============================================================='

DO $$
DECLARE
    fail_count INTEGER;
    warn_count INTEGER;
    pass_count INTEGER;
    total_issues INTEGER;
BEGIN
    SELECT 
        COUNT(*) FILTER (WHERE status = 'FAIL'),
        COUNT(*) FILTER (WHERE status = 'WARNING'),
        COUNT(*) FILTER (WHERE status = 'PASS')
    INTO fail_count, warn_count, pass_count
    FROM qa_results;
    
    SELECT COUNT(*) INTO total_issues FROM qa_issues;
    
    RAISE NOTICE '';
    RAISE NOTICE 'Total Checks: %', (fail_count + warn_count + pass_count);
    RAISE NOTICE 'Passed: % ✓', pass_count;
    RAISE NOTICE 'Warnings: % ⚠', warn_count;
    RAISE NOTICE 'Failures: % ✗', fail_count;
    RAISE NOTICE 'Total Issues Logged: %', total_issues;
    RAISE NOTICE '';
    
    IF fail_count > 0 THEN
        RAISE NOTICE '❌ QA Status: FAILED - Critical issues require attention';
    ELSIF warn_count > 5 THEN
        RAISE NOTICE '⚠️  QA Status: NEEDS REVIEW - Multiple warnings detected';
    ELSE
        RAISE NOTICE '✅ QA Status: PASSED - Data quality acceptable';
    END IF;
END $$;

-- =====================================================================
-- EXPORT HTML REPORT
-- =====================================================================
\echo ''
\echo 'Generating HTML report...'

\o /tmp/qa_report.html
SELECT '<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Ahti Paddle Map - QA Report</title>
    <style>
        body { 
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            margin: 40px auto;
            max-width: 1200px;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }
        .header h1 { margin: 0; font-size: 2.5em; }
        .header p { margin: 10px 0 0 0; opacity: 0.9; }
        
        .status-card {
            background: white;
            padding: 25px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }
        .stat-box {
            background: white;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }
        .stat-value {
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }
        .stat-label {
            color: #666;
            font-size: 0.9em;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        th {
            background: #667eea;
            color: white;
            padding: 15px;
            text-align: left;
            font-weight: 600;
        }
        td {
            padding: 12px 15px;
            border-bottom: 1px solid #eee;
        }
        tr:last-child td { border-bottom: none; }
        tr:hover { background: #f9f9f9; }
        
        .status-PASS { color: #10b981; font-weight: bold; }
        .status-WARNING { color: #f59e0b; font-weight: bold; }
        .status-FAIL { color: #ef4444; font-weight: bold; }
        .status-INFO { color: #3b82f6; font-weight: bold; }
        
        .category-badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 0.85em;
            font-weight: 500;
            background: #e5e7eb;
            color: #374151;
        }
        
        .severity-1 { background: #dbeafe; color: #1e40af; }
        .severity-2 { background: #fef3c7; color: #92400e; }
        .severity-3 { background: #fee2e2; color: #991b1b; }
        
        h2 {
            color: #1f2937;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
            margin-top: 40px;
        }
        
        .timestamp {
            text-align: right;
            color: #6b7280;
            font-size: 0.9em;
            margin-top: 40px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🌊 Ahti Paddle Map - QA Report</h1>
        <p>Comprehensive Data Quality Assessment</p>
    </div>';

-- Overall Status
SELECT '<div class="status-card">
    <h2>Overall QA Status</h2>
    <div class="stats-grid">';
    
SELECT 
    '<div class="stat-box">
        <div class="stat-value">' || COUNT(*) || '</div>
        <div class="stat-label">Total Checks</div>
    </div>'
FROM qa_results;

SELECT 
    '<div class="stat-box">
        <div class="stat-value" style="color: #10b981;">' || COUNT(*) || '</div>
        <div class="stat-label">Passed</div>
    </div>'
FROM qa_results WHERE status = 'PASS';

SELECT 
    '<div class="stat-box">
        <div class="stat-value" style="color: #f59e0b;">' || COUNT(*) || '</div>
        <div class="stat-label">Warnings</div>
    </div>'
FROM qa_results WHERE status = 'WARNING';

SELECT 
    '<div class="stat-box">
        <div class="stat-value" style="color: #ef4444;">' || COUNT(*) || '</div>
        <div class="stat-label">Failures</div>
    </div>'
FROM qa_results WHERE status = 'FAIL';

SELECT '</div></div>';

-- Network Statistics
SELECT '<h2>📊 Network Statistics</h2>
    <div class="status-card">
    <table>
        <thead>
            <tr>
                <th>Metric</th>
                <th>Value</th>
                <th>Details</th>
            </tr>
        </thead>
        <tbody>';

SELECT 
    '<tr>
        <td><span class="category-badge">' || category || '</span> ' || stat_name || '</td>
        <td><strong>' || COALESCE(stat_value::TEXT, '-') || '</strong></td>
        <td>' || stat_text || '</td>
    </tr>'
FROM qa_statistics
ORDER BY category, stat_name;

SELECT '</tbody></table></div>';

-- QA Results by Category
SELECT '<h2>🔍 Quality Check Results</h2>';

SELECT '<div class="status-card">
    <h3>' || category || '</h3>
    <table>
        <thead>
            <tr>
                <th>Check Name</th>
                <th>Status</th>
                <th>Count</th>
                <th>Threshold</th>
                <th>Notes</th>
            </tr>
        </thead>
        <tbody>' ||
    string_agg(
        '<tr>
            <td>' || check_name || '</td>
            <td class="status-' || status || '">' || status || '</td>
            <td>' || count || '</td>
            <td>' || threshold || '</td>
            <td>' || notes || '</td>
        </tr>',
        ''
    ) || '</tbody></table></div>'
FROM qa_results
GROUP BY category
ORDER BY category;

-- Issue Details (if any critical issues exist)
SELECT '<h2>⚠️ Detailed Issue Log</h2>
    <div class="status-card">
    <p>Showing detailed information for segments requiring attention...</p>
    <table>
        <thead>
            <tr>
                <th>Issue Type</th>
                <th>Segment ID</th>
                <th>Network ID</th>
                <th>Description</th>
                <th>Suggested Fix</th>
            </tr>
        </thead>
        <tbody>'
WHERE EXISTS (SELECT 1 FROM qa_issues);

SELECT 
    '<tr>
        <td><span class="category-badge">' || issue_type || '</span></td>
        <td>' || COALESCE(seg_id::TEXT, '-') || '</td>
        <td>' || COALESCE(network_id::TEXT, '-') || '</td>
        <td>' || description || '</td>
        <td><em>' || suggested_fix || '</em></td>
    </tr>'
FROM qa_issues
ORDER BY check_id, seg_id
LIMIT 100; -- Limit to first 100 issues in report

SELECT '</tbody></table>
    </div>'
WHERE EXISTS (SELECT 1 FROM qa_issues);

-- Footer
SELECT 
    '<div class="timestamp">
        Report generated: ' || NOW()::TEXT || '<br>
        Database: ahti_staging
    </div>
</body>
</html>';

\o

\echo ''
\echo '============================================================='
\echo '  QA Complete!'
\echo '============================================================='
\echo ''
\echo 'Results saved to:'
\echo '  - Tables: qa_results, qa_issues, qa_statistics'
\echo '  - HTML Report: /tmp/qa_report.html'
\echo ''
\echo 'To view detailed issues:'
\echo '  SELECT * FROM qa_issues WHERE check_id IN'
\echo '    (SELECT check_id FROM qa_results WHERE status = ''FAIL'');'
\echo ''
\echo 'To export issues as GeoJSON for mapping:'
\echo '  SELECT json_build_object('
\echo '    ''type'', ''FeatureCollection'','
\echo '    ''features'', json_agg(ST_AsGeoJSON(qa_issues.*)::json)'
\echo '  ) FROM qa_issues;'
\echo ''
