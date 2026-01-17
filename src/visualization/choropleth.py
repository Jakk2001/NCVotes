"""
Create choropleth map of voter registration by county.
Improved version with better error handling and styling.
"""
import geopandas as gpd
import logging
from pathlib import Path
from src.database.connection import get_engine
from src.database.queries import get_registration_by_county
from src.visualization.base import BaseVisualization
from config.settings import GEO_DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_county_geojson() -> gpd.GeoDataFrame:
    """
    Load NC county GeoJSON data.
    
    Returns:
        GeoDataFrame with county geometries
    """
    geojson_path = GEO_DATA_DIR / "nc_counties.geojson"
    
    if not geojson_path.exists():
        logger.error(f"GeoJSON file not found: {geojson_path}")
        raise FileNotFoundError(f"County GeoJSON not found at {geojson_path}")
    
    logger.info(f"Loading county geometries from {geojson_path}")
    gdf = gpd.read_file(geojson_path)
    
    return gdf

def plot_registration_choropleth():
    """Create choropleth map of voter registration by county."""
    try:
        # Load geographic data
        gdf = load_county_geojson()
        
        # Get registration data
        engine = get_engine()
        reg_df = get_registration_by_county(engine)
        
        if reg_df.empty:
            logger.warning("No registration data found")
            return False
        
        # Normalize county names for merging
        gdf['county_name'] = gdf['County'].str.lower().str.replace(" county", "", regex=False).str.strip()
        reg_df['county_name'] = reg_df['county'].str.lower().str.strip()
        
        # Merge geographic and registration data
        merged = gdf.merge(reg_df, on="county_name", how="left")
        
        # Fill NaN values with 0 for counties without data
        merged['registered'] = merged['registered'].fillna(0)
        
        # Create visualization
        viz = BaseVisualization(
            title="Voter Registration by County in North Carolina",
            filename="county_choropleth.png"
        )
        
        fig, ax = viz.create_figure(figsize=(14, 10))
        
        # Create choropleth
        merged.plot(
            column="registered",
            cmap="Blues",
            linewidth=0.8,
            ax=ax,
            edgecolor="0.2",
            legend=True,
            legend_kwds={
                'label': "Registered Voters",
                'orientation': "horizontal",
                'shrink': 0.8,
                'pad': 0.05
            }
        )
        
        # Set title
        viz.set_title()
        ax.axis("off")
        
        # Add text with data summary
        total_registered = merged['registered'].sum()
        counties_with_data = (merged['registered'] > 0).sum()
        
        summary_text = f"Total Registered: {total_registered:,.0f}\nCounties: {counties_with_data}"
        ax.text(
            0.02, 0.98, 
            summary_text,
            transform=ax.transAxes,
            fontsize=10,
            verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8)
        )
        
        # Save and close
        viz.save()
        viz.close()
        
        logger.info("County choropleth chart created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create choropleth: {e}", exc_info=True)
        return False

def main():
    """Entry point for command-line execution."""
    success = plot_registration_choropleth()
    if not success:
        logger.error("Choropleth visualization failed")
        exit(1)

if __name__ == "__main__":
    main()