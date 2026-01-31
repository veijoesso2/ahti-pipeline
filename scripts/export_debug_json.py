import json
import psycopg2
import os

# Configuration
DB_HOST = "localhost"
DB_NAME = "ahti_staging"
DB_USER = "ahti_builder"
DB_PASS = "ahti_secret_password"
OUTPUT_FILE = os.path.expanduser("~/ahti-pipeline/output/debug_data.json")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def fetch_features():
    conn = get_db_connection()
    cur = conn.cursor()

    # CRITICAL FIX: ST_Transform(geom, 4326) converts meters to Lat/Lon
    # We also explicitly build the Feature object to ensure valid GeoJSON properties.
    query = """
    SELECT json_build_object(
        'type', 'FeatureCollection',
        'features', json_agg(
            json_build_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(ST_Transform(t.geom, 4326))::json,
                'properties', json_build_object(
                    'debug_type', t.debug_type,
                    'status', t.status
                )
            )
        )
    )
    FROM (
        -- 1. RIVERS (Background)
        SELECT 
            'river' as debug_type, 
            'none' as status, 
            geom 
        FROM candidate_objects 
        WHERE type='river'
        
        UNION ALL
        
        -- 2. CONNECTORS (The Problem Lines)
        SELECT 
            'connector' as debug_type, 
            'none' as status, 
            geom 
        FROM candidate_objects 
        WHERE is_virtual=true
        
        UNION ALL
        
        -- 3. DEBUG POINTS (The Logic Decisions)
        SELECT 
            'point' as debug_type, 
            status, 
            geom 
        FROM debug_terminals
    ) AS t;
    """
    
    print("Executing query (converting to EPSG:4326)...")
    cur.execute(query)
    
    # Fetch result
    row = cur.fetchone()
    if row and row[0]:
        geojson_data = row[0]
    else:
        geojson_data = {"type": "FeatureCollection", "features": []}
        print("⚠️ Warning: No data returned from query.")
    
    cur.close()
    conn.close()
    return geojson_data

if __name__ == "__main__":
    try:
        data = fetch_features()
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(data, f)
            
        print(f"✅ Success! Fixed debug data (EPSG:4326) saved to: {OUTPUT_FILE}")
        
    except Exception as e:
        print(f"❌ Error: {e}")