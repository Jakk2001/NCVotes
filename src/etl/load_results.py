#!/usr/bin/env python3
"""
ETL for statewide election results.
Truncates before loading to prevent duplication across pipeline runs.
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
from config.settings import RAW_DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def transform_results(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names and types."""
    df = df.rename(columns={
        'election_dt': 'election_date',
        'contest_name': 'contest',
        'candidate_name': 'candidate',
        'party_abbrv': 'party',
        'votes': 'votes'
    })
    df['election_date'] = pd.to_datetime(df['election_date'], errors='coerce').dt.date
    df['votes'] = pd.to_numeric(df['votes'], errors='coerce').fillna(0).astype(int)
    return df[['election_date', 'county', 'precinct', 'contest', 'district', 'candidate', 'party', 'votes']]


def load_results() -> bool:
    """
    Load election results files into the database.
    Truncates existing data first to prevent duplication.

    Returns:
        True if successful, False otherwise
    """
    try:
        engine = get_engine()
        raw_dir = Path(RAW_DATA_DIR)

        files = [f for f in raw_dir.iterdir() if 'results' in f.name and f.suffix == '.txt']

        if not files:
            logger.warning(f"No results files found in {raw_dir}")
            return False

        logger.info(f"Found {len(files)} results file(s) to load")

        # Truncate existing data to prevent duplication
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM elections.election_results"))
            existing_rows = result.scalar()

            if existing_rows > 0:
                logger.warning(f"Table already has {existing_rows:,} rows — truncating")
                conn.execute(text("TRUNCATE TABLE elections.election_results"))
                conn.commit()
                logger.info("Existing election results truncated")

        # Load each file
        total_loaded = 0
        for fpath in files:
            logger.info(f"Loading {fpath.name}...")
            df = pd.read_csv(fpath, sep='\t', dtype=str)
            clean = transform_results(df)
            clean.to_sql(
                'election_results',
                engine,
                schema='elections',
                if_exists='append',
                index=False
            )
            total_loaded += len(clean)
            logger.info(f"Loaded {len(clean):,} rows from {fpath.name}")

        logger.info(f"Successfully loaded {total_loaded:,} total election result rows")
        return True

    except Exception as e:
        logger.error(f"Failed to load election results: {e}", exc_info=True)
        return False


def main():
    """Entry point for command-line execution."""
    success = load_results()
    if not success:
        logger.error("Election results loading failed")
        exit(1)
    logger.info("Election results loading completed successfully")


if __name__ == '__main__':
    main()