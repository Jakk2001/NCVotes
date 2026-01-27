"""
Centralized SQL query definitions.
All database queries should be defined here for maintainability.
"""
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def get_registration_by_party(engine: Engine) -> pd.DataFrame:
    """
    Get total voter registration counts by party.
    Counts Active + Inactive voters only (excludes Removed and Denied).
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        DataFrame with columns: party, total
    """
    query = """
    SELECT 
        COALESCE(party_cd, 'UNK') as party, 
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
    GROUP BY party_cd
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
    Counts Active + Inactive voters only (excludes Removed and Denied).
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        DataFrame with columns: registration_date, total
    """
    query = """
    SELECT 
        registr_dt as registration_date,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
      AND registr_dt ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
    GROUP BY registr_dt
    ORDER BY TO_DATE(registr_dt, 'MM/DD/YYYY');
    """
    try:
        df = pd.read_sql(query, engine)
        # Convert to proper datetime in pandas
        df['registration_date'] = pd.to_datetime(df['registration_date'], format='%m/%d/%Y', errors='coerce')
        df = df.dropna(subset=['registration_date'])
        return df
    except Exception as e:
        logger.error(f"Failed to fetch registration trends: {e}")
        raise

def get_registration_by_county(engine: Engine) -> pd.DataFrame:
    """
    Get total voter registration counts by county.
    Counts Active + Inactive voters only (excludes Removed and Denied).
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        DataFrame with columns: county, registered
    """
    query = """
    SELECT 
        county_desc AS county, 
        COUNT(*) AS registered
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
    GROUP BY county_desc
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch registration by county: {e}")
        raise

def get_registration_by_precinct(engine: Engine, county: str = None) -> pd.DataFrame:
    """
    Get voter registration by precinct.
    Counts Active + Inactive voters only (excludes Removed and Denied).
    
    Args:
        engine: SQLAlchemy engine
        county: Optional county filter
        
    Returns:
        DataFrame with precinct-level data
    """
    where_clause = "WHERE status_cd IN ('A', 'I')"
    if county:
        where_clause += f" AND county_desc = '{county}'"
    
    query = f"""
    SELECT 
        county_desc as county,
        precinct_desc as precinct,
        party_cd as party,
        COUNT(*) as total
    FROM raw.raw_voters
    {where_clause}
    GROUP BY county_desc, precinct_desc, party_cd
    ORDER BY county_desc, precinct_desc, party_cd;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch precinct data: {e}")
        raise

def get_registration_by_demographics(engine: Engine, demographic: str) -> pd.DataFrame:
    """
    Get registration counts by demographic category.
    Counts Active + Inactive voters only (excludes Removed and Denied).
    
    Args:
        engine: SQLAlchemy engine
        demographic: One of 'race', 'ethnicity', 'gender', 'age'
        
    Returns:
        DataFrame with columns: demographic_value, total
    """
    field_mapping = {
        'race': 'race_code',
        'ethnicity': 'ethnic_code',
        'gender': 'gender_code',
        'age': 'age_at_year_end'
    }
    
    if demographic not in field_mapping:
        raise ValueError(f"demographic must be one of {list(field_mapping.keys())}")
    
    field = field_mapping[demographic]
    
    query = f"""
    SELECT 
        {field} as {demographic}, 
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
    GROUP BY {field}
    ORDER BY total DESC;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch registration by {demographic}: {e}")
        raise

def get_registration_by_status(engine: Engine) -> pd.DataFrame:
    """
    Get voter registration counts by status.
    Shows all statuses for analysis purposes.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        DataFrame with columns: status, status_desc, total
    """
    query = """
    SELECT 
        status_cd as status,
        voter_status_desc as status_desc,
        COUNT(*) as total
    FROM raw.raw_voters
    GROUP BY status_cd, voter_status_desc
    ORDER BY total DESC;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch registration by status: {e}")
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
