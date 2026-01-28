"""
ETL: Load raw statewide voter registration file into raw_voters table.
Optimized for large files with progress tracking and error recovery.
Includes email notifications on successful completion.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import logging
from sqlalchemy import text
from src.database.connection import get_engine
from src.scraper.manifest import get_latest_file
from src.email.notifications import send_update_email
from config.settings import RAW_DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_raw_voters(chunksize: int = 10000) -> bool:
    """
    Load raw voter file in chunks to handle large files efficiently.
    
    Args:
        chunksize: Number of rows to process at once (reduced for stability)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Get the latest voter file from manifest
        latest_entry = get_latest_file("registration_data")
        
        if not latest_entry:
            logger.error("No voter data file found in manifest")
            return False
        
        file_path = RAW_DATA_DIR / latest_entry["filename"]
        
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return False
        
        logger.info(f"Loading raw voter data from {file_path}")
        
        # Get total rows for progress tracking
        logger.info("Counting total rows...")
        total_rows = sum(1 for _ in open(file_path, encoding='latin1')) - 1  # -1 for header
        logger.info(f"Total rows to process: {total_rows:,}")
        
        # Check if we should truncate existing data
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM raw.raw_voters"))
            existing_rows = result.scalar()
            
        if existing_rows > 0:
            logger.warning(f"Table already has {existing_rows:,} rows")
            logger.info("Truncating existing data...")
            with engine.connect() as conn:
                conn.execute(text("TRUNCATE TABLE raw.raw_voters"))
                conn.commit()
            logger.info("Existing data truncated")
        
        # Load data in chunks
        rows_loaded = 0
        chunk_counter = 0
        
        for chunk in pd.read_csv(
            file_path, 
            sep='\t', 
            dtype=str, 
            encoding='latin1', 
            chunksize=chunksize,
            low_memory=False,
            quotechar='"',
            on_bad_lines='skip'
        ):
            chunk_counter += 1
            
            # Strip quotes from column names if present
            chunk.columns = chunk.columns.str.strip().str.replace('"', '')

            
            # After loading each chunk, add this:
            from datetime import datetime

            current_year = datetime.now().year

            def calculate_age_group(birth_year):
                """Calculate age group from birth year."""
                if pd.isna(birth_year) or not str(birth_year).isdigit():
                    return 'Unknown'
                
                try:
                    age = current_year - int(birth_year)
                    if 18 <= age <= 25:
                        return '18-25'
                    elif 26 <= age <= 35:
                        return '26-35'
                    elif 36 <= age <= 50:
                        return '36-50'
                    elif 51 <= age <= 65:
                        return '51-65'
                    elif age > 65:
                        return '65+'
                    else:
                        return 'Unknown'
                except:
                    return 'Unknown'

            # Add to chunk before writing to database
            chunk['age_group'] = chunk['birth_year'].apply(calculate_age_group)
            
            
            # Write to database - use None method for simpler inserts
            chunk.to_sql(
                'raw_voters',
                engine,
                schema='raw',
                if_exists='append',
                index=False,
                method=None  # Use default insert method (slower but more stable)
            )
            
            rows_loaded += len(chunk)
            percent_complete = (rows_loaded / total_rows) * 100
            
            # Log every 10 chunks or at milestones
            if chunk_counter % 10 == 0 or rows_loaded % 100000 < chunksize:
                logger.info(
                    f"Progress: {rows_loaded:,}/{total_rows:,} rows "
                    f"({percent_complete:.1f}% complete)"
                )
        
        logger.info(f"✓ Successfully loaded {rows_loaded:,} voter records")
        
        # Send email notification about the update
        logger.info("Sending email notification...")
        try:
            send_update_email()
            logger.info("Email notification sent")
        except Exception as e:
            logger.warning(f"Failed to send email notification: {e}")
            # Don't fail the entire process if email fails
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to load raw voter data: {e}", exc_info=True)
        return False

def main():
    """Entry point for command-line execution."""
    success = load_raw_voters()
    if not success:
        logger.error("Raw voter data loading failed")
        exit(1)
    logger.info("Raw voter data loading completed successfully")

if __name__ == "__main__":
    main()