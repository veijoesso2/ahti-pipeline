import json
import psycopg2
import os

# Configuration
DB_HOST = "localhost"
DB_NAME = "ahti_staging"
DB_USER = "ahti_builder"
DB_PASS = "ahti_secret_password"
OUTPUT_FILE = os.path.expanduser("~/ahti-pipeline/output/debug_data_v15.json")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
    )

def fetch_features():
    conn = get_db_connection()
    cur = conn.cursor()

    query = """
    SELECT json_build_object(
        'type', 'FeatureCollection',
        'features', json_agg(
            json_build_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(ST_Transform(t.geom, 4326))::json,
                'properties', json_build_object(
                    'type', t.type,
                    'name', t.name
                )
            )
        )
    )
    FROM (
        -- 1. RIVERS (Background)
        SELECT 'river' as type, name, geom FROM candidate_objects WHERE type='river'
        UNION ALL
        -- 2. CONNECTORS (Portages & Culverts)
        SELECT 'connector' as type, name, geom FROM candidate_objects WHERE is_virtual=true
    ) AS t;
    """
    
    print("Executing v15 export...")
    cur.execute(query)
    geojson_data = cur.fetchone()[0]
    cur.close()
    conn.close()
    return geojson_data

if __name__ == "__main__":
    try:
        data = fetch_features()
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(data, f)
        print(f"✅ Debug data exported: {OUTPUT_FILE}")
    except Exception as e:
        print(f"❌ Error: {e}")