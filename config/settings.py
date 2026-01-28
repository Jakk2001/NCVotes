"""
Centralized configuration management for NCVotes project.
Loads environment variables and provides consistent paths throughout the application.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Directory paths
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
GEO_DATA_DIR = DATA_DIR / "geo"
OUTPUT_DIR = PROJECT_ROOT / "outputs"
CHARTS_DIR = OUTPUT_DIR / "charts"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
GEO_DATA_DIR.mkdir(parents=True, exist_ok=True)
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "database": os.getenv("PGDATABASE", "ncvotes"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
    "port": os.getenv("PGPORT", "5432"),
}

# Data source URLs
DATA_URLS = {
    "registration": "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip",
    "results_2024": "https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2024_11_05/results_pct_20241105.zip",
}

# Manifest file location
MANIFEST_PATH = RAW_DATA_DIR / "manifest.json"

# Visualization settings
VIZ_CONFIG = {
    "dpi": 300,
    "figure_format": "png",
    "default_figsize": (12, 8),
    "font_family": "Arial",
    "font_size": 12,
    "title_font_size": 16,
}

# Party color mapping
PARTY_COLORS = {
    "DEM": "#3366cc",
    "REP": "#dc3912",
    "UNA": "#888888",
    "LIB": "#ff9900",
    "GRE": "#109618",
}

# Email configuration
EMAIL_CONFIG = {
    "enabled": os.getenv("EMAIL_ENABLED", "false").lower() == "true",
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "smtp_user": os.getenv("EMAIL_USER", ""),  # Your Gmail address
    "smtp_password": os.getenv("EMAIL_PASSWORD", ""),  # Gmail App Password
    "to_email": os.getenv("EMAIL_TO", os.getenv("EMAIL_USER", "")),  # Defaults to sending to yourself
}

def get_db_url():
    """Construct PostgreSQL connection URL."""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )