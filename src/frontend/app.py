"""
Flask web application for NC Elections Transparency Project data visualization.
Improved version with better routing, error handling, and data freshness tracking.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from flask import Flask, render_template, send_from_directory, send_file, jsonify
import logging
from datetime import datetime
from src.database.connection import get_engine, test_connection
from src.database.queries import get_registration_by_party, get_registration_by_county
from config.settings import CHARTS_DIR, PROJECT_ROOT

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
template_dir = PROJECT_ROOT / "src" / "frontend" / "templates"
static_dir = PROJECT_ROOT / "src" / "frontend" / "static"

app = Flask(
    __name__,
    template_folder=str(template_dir),
    static_folder=str(static_dir)
)

@app.route("/")
def index():
    """Homepage with all visualizations."""
    try:
        # Check if charts exist
        charts = {
            'trends': (CHARTS_DIR / "registration_trends.png").exists(),
            'county': (CHARTS_DIR / "county_choropleth.png").exists(),
            'party': (CHARTS_DIR / "party_breakdown.png").exists(),
        }
        
        # Get data freshness info
        last_updated = None
        if CHARTS_DIR.exists():
            chart_files = list(CHARTS_DIR.glob("*.png"))
            if chart_files:
                latest_file = max(chart_files, key=lambda p: p.stat().st_mtime)
                last_updated = datetime.fromtimestamp(latest_file.stat().st_mtime)
        
        return render_template(
            "index.html",
            charts=charts,
            last_updated=last_updated
        )
    except Exception as e:
        logger.error(f"Error rendering index: {e}")
        return f"Error: {e}", 500

@app.route("/party")
def party_page():
    """Dedicated page for party registration visualization."""
    try:
        chart_exists = (CHARTS_DIR / "party_breakdown.png").exists()
        
        # Get party statistics
        stats = {}
        if chart_exists:
            engine = get_engine()
            df = get_registration_by_party(engine)
            if not df.empty:
                stats = {
                    'total': int(df['total'].sum()),
                    'parties': df.to_dict('records'),
                    'top_party': df.iloc[0]['party'] if len(df) > 0 else None,
                    'top_count': int(df.iloc[0]['total']) if len(df) > 0 else 0
                }
        
        return render_template(
            "party.html",
            chart_exists=chart_exists,
            stats=stats
        )
    except Exception as e:
        logger.error(f"Error rendering party page: {e}")
        return f"Error: {e}", 500

@app.route("/trends")
def trends_page():
    """Dedicated page for registration trends visualization."""
    try:
        chart_exists = (CHARTS_DIR / "registration_trends.png").exists()
        return render_template(
            "trends.html",
            chart_exists=chart_exists
        )
    except Exception as e:
        logger.error(f"Error rendering trends page: {e}")
        return f"Error: {e}", 500

@app.route("/county")
def county_page():
    """Dedicated page for county distribution visualization."""
    try:
        chart_exists = (CHARTS_DIR / "county_choropleth.png").exists()
        
        # Get county statistics
        stats = {}
        if chart_exists:
            engine = get_engine()
            df = get_registration_by_county(engine)
            if not df.empty:
                stats = {
                    'total_counties': len(df),
                    'total_registered': int(df['registered'].sum()),
                    'top_county': df.iloc[0]['county'] if len(df) > 0 else None,
                    'top_count': int(df.iloc[0]['registered']) if len(df) > 0 else 0
                }
        
        return render_template(
            "county.html",
            chart_exists=chart_exists,
            stats=stats
        )
    except Exception as e:
        logger.error(f"Error rendering county page: {e}")
        return f"Error: {e}", 500

@app.route("/interactive-map")
def interactive_map():
    """Interactive choropleth map with multiple data layers."""
    try:
        from flask import request
        # Get requested layer (default to 'total')
        layer = request.args.get('layer', 'total')
        
        # Validate layer - only 4 main maps now
        valid_layers = ['total', 'unregistered', 'party', 'race', 'gender']
        if layer not in valid_layers:
            layer = 'total'
        
        # Check if map exists
        from config.settings import OUTPUT_DIR
        maps_dir = OUTPUT_DIR / 'maps'
        map_filename = f'interactive_map_{layer}.html'
        map_path = maps_dir / map_filename
        map_exists = map_path.exists()
        
        return render_template(
            "interactive_map.html",
            layer=layer,
            map_exists=map_exists,
            map_filename=map_filename
        )
    except Exception as e:
        logger.error(f"Error rendering interactive map: {e}")
        return f"Error: {e}", 500
    
    

@app.route("/maps/<filename>")
def serve_map(filename):
    """Serve interactive map HTML files."""
    try:
        from config.settings import OUTPUT_DIR
        maps_dir = OUTPUT_DIR / 'maps'
        return send_from_directory(maps_dir, filename)
    except FileNotFoundError:
        logger.warning(f"Map not found: {filename}")
        return "Map not found", 404

@app.route("/charts/<filename>")
def serve_chart(filename):
    """Serve chart files (PNG or HTML)."""
    try:
        file_path = CHARTS_DIR / filename
        
        if not file_path.exists():
            logger.warning(f"Chart not found: {filename}")
            return "Chart not found", 404
        
        # Determine MIME type based on extension
        if filename.endswith('.html'):
            return send_file(file_path, mimetype='text/html')
        elif filename.endswith('.png'):
            return send_file(file_path, mimetype='image/png')
        else:
            return send_from_directory(CHARTS_DIR, filename)
            
    except Exception as e:
        logger.error(f"Error serving chart {filename}: {e}")
        return f"Error: {e}", 500

@app.route("/api/health")
def health_check():
    """API endpoint to check database connectivity and data status."""
    try:
        db_connected = test_connection()
        
        # Get basic stats if DB is connected
        stats = {}
        if db_connected:
            engine = get_engine()
            df = get_registration_by_party(engine)
            stats = {
                'total_registered': int(df['total'].sum()),
                'parties': len(df),
                'top_party': df.iloc[0]['party'] if not df.empty else None
            }
        
        return jsonify({
            'status': 'healthy' if db_connected else 'degraded',
            'database': 'connected' if db_connected else 'disconnected',
            'timestamp': datetime.utcnow().isoformat(),
            'stats': stats
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500

@app.route("/api/stats")
def get_stats():
    """API endpoint to get basic registration statistics."""
    try:
        engine = get_engine()
        df = get_registration_by_party(engine)
        
        return jsonify({
            'total_registered': int(df['total'].sum()),
            'by_party': df.to_dict('records'),
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found(e):
    """Handle 404 errors."""
    return "Page not found", 404

@app.errorhandler(500)
def server_error(e):
    """Handle 500 errors."""
    logger.error(f"Server error: {e}")
    return "Internal server error", 500

def main():
    """Run the Flask development server."""
    app.run(debug=True, host='0.0.0.0', port=5000)

if __name__ == "__main__":
    main()