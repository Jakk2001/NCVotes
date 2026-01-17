"""
Create voter registration trend visualizations over time.
Improved version using base visualization class.
"""
import logging
import matplotlib.pyplot as plt
from src.database.connection import get_engine
from src.database.queries import get_registration_trends
from src.visualization.base import BaseVisualization

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def plot_registration_trends():
    """Create line chart of total voter registrations over time."""
    try:
        # Get data
        engine = get_engine()
        df = get_registration_trends(engine)
        
        if df.empty:
            logger.warning("No registration trend data found")
            return False
        
        # Create visualization
        viz = BaseVisualization(
            title="Total Voter Registrations Over Time in North Carolina",
            filename="registration_trends.png"
        )
        
        fig, ax = viz.create_figure(figsize=(12, 6))
        
        # Create line plot
        ax.plot(
            df['registration_date'], 
            df['total'], 
            marker='o',
            linewidth=2,
            markersize=6,
            color='#2E86AB'
        )
        
        # Set labels
        viz.set_title()
        ax.set_xlabel("Registration Date", fontsize=12)
        ax.set_ylabel("Total Registered Voters", fontsize=12)
        
        # Format y-axis with thousands separators
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x:,.0f}"))
        
        # Add grid
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Rotate x-axis labels for better readability
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # Save and close
        viz.save()
        viz.close()
        
        logger.info("Registration trends chart created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create trends chart: {e}", exc_info=True)
        return False

def main():
    """Entry point for command-line execution."""
    success = plot_registration_trends()
    if not success:
        logger.error("Trends visualization failed")
        exit(1)

if __name__ == "__main__":
    main()
