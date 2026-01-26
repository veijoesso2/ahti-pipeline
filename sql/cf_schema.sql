DROP TABLE IF EXISTS routes_tracer;
CREATE TABLE routes_tracer (
    id TEXT PRIMARY KEY,
    name TEXT,
    length_km REAL,
    geojson TEXT
);
