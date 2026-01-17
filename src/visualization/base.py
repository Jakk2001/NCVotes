"""
Base visualization module with shared configuration and utilities.
Eliminates code duplication across visualization scripts.
"""
import matplotlib.pyplot as plt
import matplotlib
from pathlib import Path
import logging
from config.settings import VIZ_CONFIG, PARTY_COLORS, CHARTS_DIR

logger = logging.getLogger(__name__) 

# Configure matplotlib defaults
matplotlib.rcParams['font.family'] = VIZ_CONFIG['font_family']
matplotlib.rcParams['font.size'] = VIZ_CONFIG['font_size']
matplotlib.rcParams['figure.dpi'] = VIZ_CONFIG['dpi']

class BaseVisualization:
    """Base class for all visualizations with common setup."""
    
    def __init__(self, title: str, filename: str):
        """
        Initialize visualization.
        
        Args:
            title: Chart title
            filename: Output filename (without path)
        """
        self.title = title
        self.filename = filename
        self.output_path = CHARTS_DIR / filename
        self.fig = None
        self.ax = None
        
    def create_figure(self, figsize=None):
        """
        Create a new figure with standard configuration.
        
        Args:
            figsize: Tuple of (width, height), defaults to config value
        """
        if figsize is None:
            figsize = VIZ_CONFIG['default_figsize']
        
        self.fig, self.ax = plt.subplots(figsize=figsize)
        return self.fig, self.ax
    
    def set_title(self, title: str = None):
        """Set the chart title."""
        if title is None:
            title = self.title
        self.ax.set_title(title, fontsize=VIZ_CONFIG['title_font_size'])
    
    def save(self, tight_layout: bool = True):
        """
        Save the figure to disk.
        
        Args:
            tight_layout: Whether to apply tight_layout before saving
        """
        try:
            CHARTS_DIR.mkdir(parents=True, exist_ok=True)
            
            if tight_layout:
                plt.tight_layout()
            
            self.fig.savefig(
                self.output_path,
                format=VIZ_CONFIG['figure_format'],
                dpi=VIZ_CONFIG['dpi'],
                bbox_inches='tight'
            )
            logger.info(f"Saved visualization: {self.output_path}")
            
        except Exception as e:
            logger.error(f"Failed to save visualization: {e}")
            raise
    
    def close(self):
        """Close the figure to free memory."""
        if self.fig is not None:
            plt.close(self.fig)

def get_party_color(party: str, default: str = '#888888') -> str:
    """
    Get the standard color for a political party.
    
    Args:
        party: Party abbreviation (DEM, REP, etc.)
        default: Default color if party not found
        
    Returns:
        Hex color code
    """
    return PARTY_COLORS.get(party.upper(), default)

def apply_party_colors(data, party_column: str = 'party') -> list:
    """
    Generate list of colors for a dataset based on party values.
    
    Args:
        data: DataFrame or dict with party information
        party_column: Name of the party column
        
    Returns:
        List of color codes
    """
    if hasattr(data, party_column):
        # DataFrame
        return [get_party_color(p) for p in data[party_column]]
    elif isinstance(data, dict) and party_column in data:
        # Dict
        return [get_party_color(p) for p in data[party_column]]
    else:
        logger.warning(f"Could not find party column '{party_column}'")
        return None

def format_large_numbers(value: float, precision: int = 1) -> str:
    """
    Format large numbers with K/M/B suffixes.
    
    Args:
        value: Number to format
        precision: Decimal places
        
    Returns:
        Formatted string
    """
    if value >= 1e9:
        return f"{value/1e9:.{precision}f}B"
    elif value >= 1e6:
        return f"{value/1e6:.{precision}f}M"
    elif value >= 1e3:
        return f"{value/1e3:.{precision}f}K"
    else:
        return f"{value:.{precision}f}"

def add_value_labels(ax, orientation='vertical', format_func=None):
    """
    Add value labels to bar charts.
    
    Args:
        ax: Matplotlib axis
        orientation: 'vertical' or 'horizontal'
        format_func: Optional function to format values
    """
    if format_func is None:
        format_func = lambda x: f"{x:,.0f}"
    
    for container in ax.containers:
        ax.bar_label(container, fmt=format_func, padding=3)