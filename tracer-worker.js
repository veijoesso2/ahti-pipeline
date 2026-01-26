export default {
  async fetch(request, env) {
    // 1. Handle CORS (Allow your browser to access this API)
    const corsHeaders = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, HEAD, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      // 2. Query D1 (Get all rows)
      const { results } = await env.DB.prepare(
        "SELECT * FROM routes_tracer"
      ).all();

      // 3. Convert SQL Rows -> GeoJSON FeatureCollection
      const features = results.map(row => {
        // Parse the geometry string back into an object
        let geometry = null;
        try { geometry = JSON.parse(row.geojson); } catch(e) {}

        return {
          type: "Feature",
          properties: {
            id: row.id,
            name: row.name,
            length_km: row.length_km
          },
          geometry: geometry
        };
      }).filter(f => f.geometry !== null);

      const geojson = {
        type: "FeatureCollection",
        features: features
      };

      // 4. Return JSON
      return new Response(JSON.stringify(geojson), {
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      });

    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), {
        status: 500,
        headers: corsHeaders
      });
    }
  }
};
