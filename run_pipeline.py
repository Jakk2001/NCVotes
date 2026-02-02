"""
Complete data pipeline orchestrator for NC Elections Transparency Project.
Runs scraping, ETL, visualization generation, and web server startup in sequence.
"""
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.scraper import registration as scraper_reg
from src.scraper import results as scraper_res
from src.etl import load_raw_voters
from src.visualization import demographics, trends, choropleth
from src.database.connection import test_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_full_pipeline():
    """Run the complete data pipeline and start the web server."""
    logger.info("=" * 60)
    logger.info("Starting NC Elections Transparency Project Data Pipeline")
    logger.info("=" * 60)
    
    # Step 1: Test database connection
    logger.info("\n[1/7] Testing database connection...")
    if not test_connection():
        logger.error("Database connection failed. Please check your configuration.")
        return False
    logger.info("✓ Database connection successful")
    
    # Step 2: Scrape registration data
    logger.info("\n[2/7] Scraping voter registration data...")
    if not scraper_reg.scrape_registration():
        logger.error("Registration scraping failed")
        return False
    logger.info("✓ Registration data scraped successfully")
    
    # Step 3: Load registration data to database
    logger.info("\n[3/7] Loading raw voter data to database...")
    logger.warning("This will take 30-60 minutes for 9M+ records...")
    if not load_raw_voters.load_raw_voters():
        logger.error("Raw voter data loading failed")
        return False
    logger.info("✓ Raw voter data loaded successfully")
    
    # Step 4: Generate demographics visualization
    logger.info("\n[4/7] Generating demographics visualization...")
    if not demographics.plot_party_breakdown():
        logger.warning("Demographics visualization failed (non-critical)")
    else:
        logger.info("✓ Demographics visualization created")
    
    # Step 5: Generate trends visualization
    logger.info("\n[5/7] Generating trends visualization...")
    if not trends.plot_registration_trends():
        logger.warning("Trends visualization failed (non-critical)")
    else:
        logger.info("✓ Trends visualization created")
    
    # Step 6: Generate choropleth visualization
    logger.info("\n[6/7] Generating county map visualization...")
    if not choropleth.plot_registration_choropleth():
        logger.warning("Choropleth visualization failed (non-critical)")
    else:
        logger.info("✓ County map visualization created")
    
    # Step 7: Start Flask web server
    logger.info("\n[7/7] Starting Flask web server...")
    logger.info("=" * 60)
    logger.info("Pipeline completed successfully!")
    logger.info("Starting web server at http://localhost:5000")
    logger.info("Press CTRL+C to stop the server")
    logger.info("=" * 60)
    
    try:
        from src.frontend.app import app
        # Disable reloader to prevent re-running the entire pipeline
        app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("\nWeb server stopped by user")
    except Exception as e:
        logger.error(f"Failed to start web server: {e}")
        return False
    
    return True

def run_scrapers_only():
    """Run only the data scraping steps."""
    logger.info("Running scrapers only...")
    
    logger.info("[1/2] Scraping registration data...")
    scraper_reg.scrape_registration()
    
    logger.info("[2/2] Scraping election results...")
    scraper_res.scrape_results()
    
    logger.info("Scraping complete")

def run_etl_only():
    """Run only the ETL steps."""
    logger.info("Running ETL only...")
    
    if not test_connection():
        logger.error("Database connection failed")
        return False
    
    logger.info("Loading raw voter data...")
    load_raw_voters.load_raw_voters()
    
    logger.info("ETL complete")

def run_visualizations_only():
    """Run only the visualization generation steps."""
    logger.info("Running visualizations only...")
    
    demographics.plot_party_breakdown()
    trends.plot_registration_trends()
    choropleth.plot_registration_choropleth()
    
    logger.info("Visualizations complete")

def run_server_only():
    """Start the web server only (assumes data is already loaded)."""
    logger.info("Starting web server...")
    logger.info("Server will run at http://localhost:5000")
    logger.info("Press CTRL+C to stop")
    
    try:
        from src.frontend.app import app
        # use_reloader=True here is fine since we're not running the pipeline
        app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=True)
    except KeyboardInterrupt:
        logger.info("\nWeb server stopped by user")

def main():
    """Main entry point with command-line argument handling."""
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "scrape":
            run_scrapers_only()
        elif command == "etl":
            run_etl_only()
        elif command == "viz":
            run_visualizations_only()
        elif command == "server":
            run_server_only()
        elif command == "full":
            run_full_pipeline()
        else:
            print("Usage: python run_pipeline.py [scrape|etl|viz|server|full]")
            print("  scrape - Download data only")
            print("  etl    - Load data to database only")
            print("  viz    - Generate visualizations only")
            print("  server - Start web server only")
            print("  full   - Run complete pipeline + start server (default)")
            sys.exit(1)
    else:
        # Default: run full pipeline
        success = run_full_pipeline()
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()