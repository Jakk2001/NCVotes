#!/usr/bin/env python3
"""
Helper script to generate and test interactive maps.
Provides convenient command-line interface for map operations.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import argparse
import logging
from pathlib import Path
from src.database.connection import test_connection
from config.settings import OUTPUT_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_requirements():
    """Check if all required dependencies are installed."""
    missing = []
    
    try:
        import plotly
    except ImportError:
        missing.append('plotly')
    
    try:
        import geopandas
    except ImportError:
        missing.append('geopandas')
    
    if missing:
        logger.error(f"Missing required packages: {', '.join(missing)}")
        logger.error("Install with: pip install " + " ".join(missing))
        return False
    
    return True

def generate_maps(layer=None):
    """
    Generate interactive maps.
    
    Args:
        layer: Specific layer to generate, or None for all
    """
    if not test_connection():
        logger.error("Database connection failed. Check your configuration.")
        return False
    
    # Import the new map generator
    from src.visualization.interactive_map import (
        create_total_voters_map,
        create_party_map,
        create_race_map,
        create_gender_map,
        create_all_maps
    )
    
    if layer and layer != 'all':
        logger.info(f"Generating {layer} map...")
        
        if layer == 'total':
            result = create_total_voters_map()
        elif layer == 'party':
            result = create_party_map()
        elif layer == 'race':
            result = create_race_map()
        elif layer == 'gender':
            result = create_gender_map()
        else:
            logger.error(f"Invalid layer: {layer}")
            return False
            
        if result:
            logger.info(f"✓ Map created: {result}")
            return True
        else:
            logger.error("Map generation failed")
            return False
    else:
        logger.info("Generating all map layers...")
        results = create_all_maps()
        
        success_count = sum(1 for r in results.values() if r is not None)
        logger.info(f"✓ Generated {success_count}/4 maps successfully")
        
        for layer_name, result in results.items():
            if result:
                logger.info(f"  - {layer_name}: {result}")
            else:
                logger.warning(f"  - {layer_name}: Failed")
        
        return success_count == 4

def generate_precinct_map(county_name):
    """Generate precinct map for a specific county."""
    logger.warning("Precinct maps are not yet implemented in the new version")
    logger.info("This feature will be added in a future update")
    return False

def list_maps():
    """List all generated maps."""
    maps_dir = OUTPUT_DIR / 'maps'
    
    if not maps_dir.exists():
        logger.info("No maps directory found. Generate maps first.")
        return
    
    map_files = list(maps_dir.glob('*.html'))
    
    if not map_files:
        logger.info("No maps found. Generate maps first.")
        return
    
    logger.info(f"Found {len(map_files)} map(s):")
    for map_file in sorted(map_files):
        size_mb = map_file.stat().st_size / (1024 * 1024)
        logger.info(f"  - {map_file.name} ({size_mb:.2f} MB)")

def open_map(layer='total'):
    """Open a map in the default browser."""
    maps_dir = OUTPUT_DIR / 'maps'
    map_file = maps_dir / f'interactive_map_{layer}.html'
    
    if not map_file.exists():
        logger.error(f"Map not found: {map_file}")
        logger.error("Generate it first with: python generate_maps.py --generate --layer " + layer)
        return False
    
    import webbrowser
    logger.info(f"Opening {map_file} in browser...")
    webbrowser.open(f"file://{map_file.absolute()}")
    return True

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Generate and manage interactive voter registration maps',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate all 4 maps
  python generate_maps.py --generate all
  
  # Generate specific map
  python generate_maps.py --generate party
  
  # List existing maps
  python generate_maps.py --list
  
  # Open a map in browser
  python generate_maps.py --open party
  
Note: Each map (party, race, gender) has a dropdown menu to switch between subcategories.
      - Party map: Switch between % Democrat, % Republican, % Unaffiliated
      - Race map: Switch between % White, % Black, % Other
      - Gender map: Switch between % Female, % Male
        """
    )
    
    parser.add_argument(
        '--generate', '-g',
        choices=['all', 'total', 'party', 'race', 'gender'],
        help='Generate interactive map(s)'
    )
    
    parser.add_argument(
        '--precinct', '-p',
        metavar='COUNTY',
        help='Generate precinct map for specified county'
    )
    
    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List all generated maps'
    )
    
    parser.add_argument(
        '--open', '-o',
        choices=['total', 'party', 'race', 'gender'],
        help='Open map in default browser'
    )
    
    parser.add_argument(
        '--check',
        action='store_true',
        help='Check if required dependencies are installed'
    )
    
    args = parser.parse_args()
    
    # Check requirements if requested
    if args.check:
        if check_requirements():
            logger.info("✓ All required dependencies are installed")
            return 0
        else:
            return 1
    
    # If no arguments, show help
    if not any([args.generate, args.precinct, args.list, args.open]):
        parser.print_help()
        return 0
    
    # Check requirements before doing anything else
    if not check_requirements():
        return 1
    
    # Execute requested action
    success = True
    
    if args.list:
        list_maps()
    
    if args.generate:
        success = generate_maps(args.generate)
    
    if args.precinct:
        success = generate_precinct_map(args.precinct)
    
    if args.open:
        success = open_map(args.open)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())