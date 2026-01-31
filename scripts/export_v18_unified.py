import json
import psycopg2
import os

# Configuration
DB_HOST = "localhost"
DB_NAME = "ahti_staging"
DB_USER = "ahti_builder"
DB_PASS = "ahti_secret_password"
OUTPUT_FILE = os.path.expanduser("~/ahti-pipeline/output/local_data_qa.json")

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
    )

def fetch_data():
    conn = get_db_connection()
    cur = conn.cursor()

    print("⏳ Exporting v18 Data (Lakes + Segments)...")

    # This query combines TWO sources:
    # 1. Lake Polygons (Background)
    # 2. Paddling Segments (Rivers, Dams, Culverts, Routes)
    query = """
    SELECT json_build_object(
        'type', 'FeatureCollection',
        'features', json_agg(fc.feature)
    )
    FROM (
        -- 1. LAKES (Polygons from candidate_objects)
        SELECT json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(ST_Transform(geom, 4326))::json,
            'properties', json_build_object(
                'type', 'lake_gray', 
                'name', name
            )
        ) as feature
        FROM candidate_objects 
        WHERE type = 'lake'
        
        UNION ALL
        
        -- 2. SEGMENTS (Rivers, Dams, Culverts from paddling_segments)
        SELECT json_build_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(ST_Transform(geom, 4326))::json,
            'properties', json_build_object(
                'type', type,                 -- river, lake_crossing, dam_crossing
                'name', name,                 -- 'Culvert', 'Dam Portage', etc.
                'environment', environment,   -- obstacle, river_official
                'fun_score', fun_score,
                'feasibility_score', feasibility_score,
                'rapid_class', rapid_class    -- I, II, III...
            )
        ) as feature
        FROM paddling_segments
    ) as fc;
    """
    
    cur.execute(query)
    row = cur.fetchone()
    
    # Handle empty results safely
    if row and row[0]:
        geojson_data = row[0]
    else:
        geojson_data = {"type": "FeatureCollection", "features": []}
        print("⚠️ Warning: Database returned no features.")

    cur.close()
    conn.close()
    return geojson_data

if __name__ == "__main__":
    try:
        data = fetch_data()
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(data, f)
            
        print(f"✅ Success! Data exported to: {OUTPUT_FILE}")
        print(f"📊 Total Features: {len(data['features'])}")
        
    except Exception as e:
        print(f"❌ Error: {e}")