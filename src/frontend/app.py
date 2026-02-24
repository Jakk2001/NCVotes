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
import yaml
import markdown

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Initialize Flask app
template_dir = PROJECT_ROOT / "src" / "frontend" / "templates"
static_dir = PROJECT_ROOT / "src" / "frontend" / "static"

_trends_stats_cache = {
    'data': None,
    'timestamp': None,
    'ttl_minutes': 60  # Cache for 1 hour
}

app = Flask(
    __name__,
    template_folder=str(template_dir),
    static_folder=str(static_dir)
)

import os

# Production settings
if os.getenv('ENVIRONMENT') == 'production':
    app.config['DEBUG'] = False
    app.config['TESTING'] = False

# Add this route to src/frontend/app.py
# Place it before the main() function, after the other route definitions

@app.route("/voting-info")
def voting_info():
    """Voting and elections information page."""
    try:
        import json
        from pathlib import Path
        
        # Load content from JSON file
        # Try multiple possible locations
        possible_paths = [
            PROJECT_ROOT / "data" / "voting_info_content.json",
            Path(__file__).parent.parent.parent / "data" / "voting_info_content.json",
        ]
        
        content = None
        for content_path in possible_paths:
            if content_path.exists():
                logger.info(f"Loading voting info from: {content_path}")
                with open(content_path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                break
        
        if content is None:
            # Fallback minimal content if file doesn't exist
            logger.warning("Voting info content file not found, using fallback")
            content = {
                "page_title": "Voting & Elections in North Carolina",
                "page_tagline": "Your guide to understanding how elections work in NC",
                "sections": []
            }
        
        # Ensure content has the expected structure
        if not isinstance(content, dict):
            raise ValueError(f"Content must be a dict, got {type(content)}")
        if 'sections' not in content:
            content['sections'] = []
        
        return render_template("voting_info.html", content=content)
        
    except Exception as e:
        logger.error(f"Error rendering voting info page: {e}", exc_info=True)
        return f"Error loading voting info page: {str(e)}", 500
    
    return render_template("voting_info.html", content=content)



@app.route("/trends")
def trends_page():
    """Dedicated page for registration trends visualization with pre-generated stats."""
    try:
        import json
        
        chart_exists = (CHARTS_DIR / "registration_trends.png").exists()

        # Get data freshness info
        last_updated = None
        if CHARTS_DIR.exists():
            chart_files = list(CHARTS_DIR.glob("*.png"))
            if chart_files:
                latest_file = max(chart_files, key=lambda p: p.stat().st_mtime)
                last_updated = datetime.fromtimestamp(latest_file.stat().st_mtime)
        
        # Read pre-generated stats from JSON file (instant load)
        stats_file = CHARTS_DIR / 'trends_key_stats.json'
        stats = {}
        
        if stats_file.exists():
            try:
                with open(stats_file, 'r') as f:
                    stats = json.load(f)
                logger.info("Loaded pre-generated trends stats from JSON")
            except Exception as e:
                logger.warning(f"Could not load stats JSON: {e}")
                # Fallback: get current total only (fast query)
                engine = get_engine()
                from src.database.queries import get_registration_by_party
                df = get_registration_by_party(engine)
                stats = {'current_total': int(df['total'].sum())}
        else:
            logger.warning("Stats JSON not found, using minimal stats")
            # Fallback: get current total only
            engine = get_engine()
            from src.database.queries import get_registration_by_party
            df = get_registration_by_party(engine)
            stats = {'current_total': int(df['total'].sum())}
        
        return render_template(
            "trends.html",
            chart_exists=chart_exists,
            last_updated=last_updated,
            stats=stats
        )
    except Exception as e:
        logger.error(f"Error rendering trends page: {e}")
        return f"Error: {e}", 500



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

        # Get latest blog post
        latest_post = None
        try:
            posts = load_blog_posts()
            if posts:
                latest_post = posts[0]
        except Exception as e:
            logger.warning(f"Could not load latest blog post: {e}")
        
        return render_template(
            "index.html",
            charts=charts,
            last_updated=last_updated,
            latest_post=latest_post,

        )
    except Exception as e:
        logger.error(f"Error rendering index: {e}")
        return f"Error: {e}", 500

@app.route("/party")
def party_page():
    """Dedicated page for party registration visualization."""
    try:
        import json
        
        chart_exists = (CHARTS_DIR / "party_breakdown.png").exists()
        
        # Load stats from pre-generated JSON (no database query)
        stats_file = CHARTS_DIR / 'trends_key_stats.json'
        stats = {}
        
        if stats_file.exists():
            try:
                with open(stats_file, 'r') as f:
                    data = json.load(f)
                    # Extract what we need for the party page
                    stats = {
                        'total': data.get('current_total', 0),
                        'parties': data.get('party_breakdown', []),
                        'top_party': data.get('party_breakdown', [{}])[0].get('party') if data.get('party_breakdown') else None,
                        'top_count': data.get('party_breakdown', [{}])[0].get('total') if data.get('party_breakdown') else 0
                    }
            except Exception as e:
                logger.warning(f"Could not load stats JSON: {e}")
                stats = {}
        
        return render_template(
            "party.html",
            chart_exists=chart_exists,
            stats=stats
        )
    except Exception as e:
        logger.error(f"Error rendering party page: {e}")
        return render_template("party.html", chart_exists=False, stats={})

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
    
    """
Blog routes - paste into src/frontend/app.py before the main() function.
Also add these imports at the top of app.py:

    import yaml
    import markdown

And add to requirements.txt:
    markdown==3.7
    pyyaml==6.0.2
    pygments==2.18.0
"""

# --- Add to app.py BEFORE main() ---

def load_blog_posts():
    """Load all blog posts from data/blog/ directory, sorted by date descending."""
    import yaml
    import markdown
    
    blog_dir = PROJECT_ROOT / "data" / "blog"
    posts = []
    
    if not blog_dir.exists():
        logger.warning(f"Blog directory not found: {blog_dir}")
        return posts
    
    for md_file in blog_dir.glob("*.md"):
        try:
            raw = md_file.read_text(encoding='utf-8')
            
            # Parse YAML frontmatter
            if raw.startswith('---'):
                parts = raw.split('---', 2)
                if len(parts) >= 3:
                    meta = yaml.safe_load(parts[1])
                    body_md = parts[2].strip()
                else:
                    continue
            else:
                continue
            
            # Slug from filename (e.g. 2026-02-23-my-post.md -> my-post)
            slug = md_file.stem
            # Strip leading date prefix if present (YYYY-MM-DD-)
            if len(slug) > 11 and slug[4] == '-' and slug[7] == '-' and slug[10] == '-':
                slug = slug[11:]
            
            posts.append({
                'slug': slug,
                'filename': md_file.stem,
                'title': meta.get('title', 'Untitled'),
                'date': meta.get('date'),
                'category': meta.get('category', 'general'),
                'summary': meta.get('summary', ''),
                'author': meta.get('author', ''),
                'body_md': body_md,
            })
        except Exception as e:
            logger.error(f"Error loading blog post {md_file.name}: {e}")
    
    # Sort by date descending
    posts.sort(key=lambda p: p.get('date') or '', reverse=True)
    return posts


def render_markdown(text):
    """Convert markdown to HTML with extensions."""
    import markdown
    extensions = [
        'markdown.extensions.fenced_code',
        'markdown.extensions.codehilite',
        'markdown.extensions.tables',
        'markdown.extensions.toc',
        'markdown.extensions.footnotes',
    ]
    extension_configs = {
        'markdown.extensions.codehilite': {
            'css_class': 'highlight',
            'linenums': False,
        }
    }
    return markdown.markdown(text, extensions=extensions, extension_configs=extension_configs)


@app.route("/blog")
def blog():
    """Blog listing page."""
    from flask import request
    category = request.args.get('category', None)
    posts = load_blog_posts()
    
    if category:
        posts = [p for p in posts if p['category'] == category]
    
    categories = sorted(set(p['category'] for p in load_blog_posts()))
    
    return render_template(
        "blog.html",
        posts=posts,
        categories=categories,
        active_category=category,
    )


@app.route("/blog/<slug>")
def blog_post(slug):
    """Individual blog post page."""
    posts = load_blog_posts()
    
    post = None
    for p in posts:
        if p['slug'] == slug:
            post = p
            break
    
    if post is None:
        return "Post not found", 404
    
    post['body_html'] = render_markdown(post['body_md'])
    
    return render_template("blog_post.html", post=post)

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