"""
Create voter registration demographics visualizations.
Improved version using base visualization class and proper styling.
"""
import logging
import matplotlib.pyplot as plt
from src.database.connection import get_engine
from src.database.queries import get_registration_by_party
from src.visualization.base import BaseVisualization, apply_party_colors, add_value_labels

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def plot_party_breakdown():
    """Create bar chart of voter registration by party."""
    try:
        # Get data
        engine = get_engine()
        df = get_registration_by_party(engine)
        
        if df.empty:
            logger.warning("No registration data found")
            return False
        
        # Create visualization
        viz = BaseVisualization(
            title="Voter Registration by Party in North Carolina",
            filename="party_breakdown.png"
        )
        
        fig, ax = viz.create_figure(figsize=(10, 6))
        
        # Apply party colors
        colors = apply_party_colors(df, 'party')
        
        # Create bar chart
        bars = ax.bar(df['party'], df['total'], color=colors, edgecolor='black', linewidth=0.5)
        
        # Set labels
        viz.set_title()
        ax.set_xlabel("Party Affiliation", fontsize=12)
        ax.set_ylabel("Registered Voters", fontsize=12)
        
        # Add value labels on bars
        add_value_labels(ax, format_func=lambda x: f"{x:,.0f}")
        
        # Format y-axis with thousands separators
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x:,.0f}"))
        
        # Add grid for readability
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        ax.set_axisbelow(True)
        
        # Save and close
        viz.save()
        viz.close()
        
        logger.info("Party breakdown chart created successfully")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create party breakdown chart: {e}", exc_info=True)
        return False

def main():
    """Entry point for command-line execution."""
    success = plot_party_breakdown()
    if not success:
        logger.error("Demographics visualization failed")
        exit(1)

if __name__ == "__main__":
    main()
