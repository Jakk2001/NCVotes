"""
Centralized SQL query definitions.
All database queries should be defined here for maintainability.
"""
import pandas as pd
from sqlalchemy.engine import Engine
import logging
 
logger = logging.getLogger(__name__)

def get_registration_by_party(engine: Engine) -> pd.DataFrame:
    """
    Get total voter registration counts by party.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        DataFrame with columns: party, total
    """
    query = """
    SELECT party, SUM(total) as total
    FROM registration.voter_registration
    GROUP BY party
    ORDER BY total DESC;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch registration by party: {e}")
        raise

def get_registration_trends(engine: Engine) -> pd.DataFrame:
    """
    Get voter registration trends over time.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        DataFrame with columns: registration_date, total
    """
    query = """
    SELECT registration_date, SUM(total) as total
    FROM registration.voter_registration
    GROUP BY registration_date
    ORDER BY registration_date;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch registration trends: {e}")
        raise

def get_registration_by_county(engine: Engine) -> pd.DataFrame:
    """
    Get total voter registration counts by county.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        DataFrame with columns: county, registered
    """
    query = """
    SELECT 
        county_desc AS county, 
        SUM(total) AS registered
    FROM registration.voter_registration
    GROUP BY county_desc
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch registration by county: {e}")
        raise

def get_registration_by_demographics(engine: Engine, demographic: str) -> pd.DataFrame:
    """
    Get registration counts by demographic category.
    
    Args:
        engine: SQLAlchemy engine
        demographic: One of 'race', 'ethnicity', 'gender', 'age'
        
    Returns:
        DataFrame with columns: demographic_value, total
    """
    valid_demographics = ['race', 'ethnicity', 'gender', 'age']
    if demographic not in valid_demographics:
        raise ValueError(f"demographic must be one of {valid_demographics}")
    
    query = f"""
    SELECT {demographic}, SUM(total) as total
    FROM registration.voter_registration
    GROUP BY {demographic}
    ORDER BY total DESC;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch registration by {demographic}: {e}")
        raise

def get_election_results_by_race(engine: Engine, election_date: str) -> pd.DataFrame:
    """
    Get election results for a specific election date.
    
    Args:
        engine: SQLAlchemy engine
        election_date: Election date in YYYY-MM-DD format
        
    Returns:
        DataFrame with election results
    """
    query = """
    SELECT candidate, party, SUM(votes) as total_votes
    FROM elections.election_results
    WHERE election_date = :election_date
    GROUP BY candidate, party
    ORDER BY total_votes DESC;
    """
    try:
        return pd.read_sql(query, engine, params={"election_date": election_date})
    except Exception as e:
        logger.error(f"Failed to fetch election results: {e}")
        raise