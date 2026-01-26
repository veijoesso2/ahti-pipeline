🌊 Ahti Tracer: Finnish Paddling Network Pipeline
Ahti Tracer is a high-performance spatial ETL (Extract, Transform, Load) pipeline designed to identify, analyze, and score navigable paddling routes across Finland. Using OpenStreetMap data and PostGIS, it transforms raw waterway lines into a segmented, scored network for canoeists and kayakers.

🚀 Key Features
Strict Navigability Filtering: Automatically filters out unnavigable forest ditches and small streams by enforcing a 3-meter minimum width constraint for secondary waterways.

Automated Lake Connections: Uses "Star Connector" logic to bridge the gap between river mouths and lake centers, creating a continuous traversable network.

Precise Land-Use Enrichment: Analyzes the environment within a 50m buffer of every segment, assigning scores based on a 50% area threshold for forest, field, or urban surroundings.

Chunk-Based National Build: Designed to run on resource-constrained hardware (like a 2010 Mac Mini) by processing Finland in manageable spatial chunks.

Dynamic Scoring Engine: Calculates "Fun" and "Feasibility" scores based on sinuosity, rapids, proximity to points of interest (shelters/piers), and official canoe route status.

🛠 Tech Stack
Database: PostgreSQL 15+ with PostGIS 3.3

Ingestion: osm2pgsql (Slim mode)

Frontend: MapLibre GL JS for real-time GeoJSON/PMTiles visualization.

Server: Node.js http-server or Nginx with Gzip compression for handling large national datasets.

📁 Pipeline Architecture
The pipeline is executed via a master SQL script that handles the lifecycle of a spatial chunk:

Cleanup: Wipes existing data in the target bounding box to allow for idempotent re-runs.

Lake Connectivity: Generates virtual connectors to ensure routing through Finland's thousands of lakes.

The "Chopper": Segments long OSM ways into uniform 1km pieces for granular analysis.

Enrichment: Spatial joins against land cover, official routes, and rapids features.

Scoring: Final calculation of paddling quality metrics.

🏁 Getting Started
Prerequisites
A PostGIS-enabled database named ahti_staging.

Finland OSM data (finland-latest.osm.pbf) imported via osm2pgsql.

Running the Build
To process the whole of Finland, run the build script from the root directory:

Bash

bash ./scripts/build_finland.sh
Viewing the Data
Start the local server to view the results in viewer.html:

Bash

http-server ./output -p 8080 --cors -c-1 --gzip
📜 License
This project is licensed under the MIT License. Data is © OpenStreetMap contributors.
