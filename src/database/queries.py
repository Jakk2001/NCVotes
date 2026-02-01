"""
Centralized SQL query definitions.
All database queries should be defined here for maintainability.
"""
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def get_party_by_race(engine: Engine) -> pd.DataFrame:
    """Get voter registration counts by party and race."""
    query = """
    SELECT 
        party_cd as party,
        race_code as race,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
    GROUP BY party_cd, race_code
    ORDER BY party_cd, total DESC;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch party by race: {e}")
        raise

def get_party_by_gender(engine: Engine) -> pd.DataFrame:
    """Get voter registration counts by party and gender."""
    query = """
    SELECT 
        party_cd as party,
        gender_code as gender,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
    GROUP BY party_cd, gender_code
    ORDER BY party_cd, total DESC;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch party by gender: {e}")
        raise

def get_party_by_age_group(engine: Engine) -> pd.DataFrame:
    """Get voter registration counts by party and age group."""
    query = """
    SELECT 
        party_cd as party,
        age_group,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
      AND age_group IS NOT NULL
      AND age_group != 'Unknown'
    GROUP BY party_cd, age_group
    ORDER BY party_cd, 
             CASE age_group
                WHEN '18-25' THEN 1
                WHEN '26-35' THEN 2
                WHEN '36-50' THEN 3
                WHEN '51-65' THEN 4
                WHEN '65+' THEN 5
             END;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch party by age group: {e}")
        raise

def get_gender_breakdown(engine: Engine) -> pd.DataFrame:
    """Get voter registration counts by gender."""
    query = """
    SELECT 
        gender_code as gender,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
    GROUP BY gender_code
    ORDER BY total DESC;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch gender breakdown: {e}")
        raise

def get_gender_by_age_group(engine: Engine) -> pd.DataFrame:
    """Get voter registration counts by gender and age group."""
    query = """
    SELECT 
        gender_code as gender,
        age_group,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
      AND age_group IS NOT NULL
      AND age_group != 'Unknown'
    GROUP BY gender_code, age_group
    ORDER BY gender_code,
             CASE age_group
                WHEN '18-25' THEN 1
                WHEN '26-35' THEN 2
                WHEN '36-50' THEN 3
                WHEN '51-65' THEN 4
                WHEN '65+' THEN 5
             END;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch gender by age group: {e}")
        raise

def get_gender_by_race(engine: Engine) -> pd.DataFrame:
    """Get voter registration counts by gender and race."""
    query = """
    SELECT 
        gender_code as gender,
        race_code as race,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
    GROUP BY gender_code, race_code
    ORDER BY gender_code, total DESC;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch gender by race: {e}")
        raise

def get_age_group_breakdown(engine: Engine) -> pd.DataFrame:
    """Get voter registration counts by age group."""
    query = """
    SELECT 
        age_group,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
      AND age_group IS NOT NULL
      AND age_group != 'Unknown'
    GROUP BY age_group
    ORDER BY CASE age_group
                WHEN '18-25' THEN 1
                WHEN '26-35' THEN 2
                WHEN '36-50' THEN 3
                WHEN '51-65' THEN 4
                WHEN '65+' THEN 5
             END;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch age group breakdown: {e}")
        raise

def get_race_breakdown(engine: Engine) -> pd.DataFrame:
    """Get voter registration counts by race."""
    query = """
    SELECT 
        race_code as race,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
    GROUP BY race_code
    ORDER BY total DESC;
    """
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch race breakdown: {e}")
        raise

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

def get_county_data_by_layer(engine: Engine, layer: str) -> pd.DataFrame:
    """
    Get county-level data for interactive map layers.
    
    Args:
        engine: SQLAlchemy engine
        layer: Data layer ('total', 'party', 'race', 'gender')
        
    Returns:
        DataFrame with county-level statistics
    """
    if layer == 'total':
        query = """
        SELECT 
            county_desc AS county,
            COUNT(*) AS registered
        FROM raw.raw_voters
        WHERE status_cd IN ('A', 'I')
        GROUP BY county_desc
        ORDER BY registered DESC;
        """
    
    elif layer in ['party', 'party_dem', 'party_rep', 'party_una']:
        # All party layers use the same query (returns all party percentages)
        query = """
        WITH county_totals AS (
            SELECT 
                county_desc AS county,
                COUNT(*) AS total
            FROM raw.raw_voters
            WHERE status_cd IN ('A', 'I')
            GROUP BY county_desc
        ),
        party_counts AS (
            SELECT 
                county_desc AS county,
                party_cd,
                COUNT(*) AS count
            FROM raw.raw_voters
            WHERE status_cd IN ('A', 'I')
            GROUP BY county_desc, party_cd
        )
        SELECT 
            ct.county,
            ct.total,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'DEM' THEN pc.count END), 0) AS dem_count,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'REP' THEN pc.count END), 0) AS rep_count,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'UNA' THEN pc.count END), 0) AS una_count,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'LIB' THEN pc.count END), 0) AS lib_count,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'GRE' THEN pc.count END), 0) AS gre_count,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'CST' THEN pc.count END), 0) AS cst_count,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'DEM' THEN pc.count END), 0) * 100.0 / ct.total AS dem_pct,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'REP' THEN pc.count END), 0) * 100.0 / ct.total AS rep_pct,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'UNA' THEN pc.count END), 0) * 100.0 / ct.total AS una_pct,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'LIB' THEN pc.count END), 0) * 100.0 / ct.total AS lib_pct,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'GRE' THEN pc.count END), 0) * 100.0 / ct.total AS gre_pct,
            COALESCE(MAX(CASE WHEN pc.party_cd = 'CST' THEN pc.count END), 0) * 100.0 / ct.total AS cst_pct
        FROM county_totals ct
        LEFT JOIN party_counts pc ON ct.county = pc.county
        GROUP BY ct.county, ct.total
        ORDER BY ct.total DESC;
        """
    
    elif layer == 'race':
        query = """
        WITH county_totals AS (
            SELECT 
                county_desc AS county,
                COUNT(*) AS total
            FROM raw.raw_voters
            WHERE status_cd IN ('A', 'I')
            GROUP BY county_desc
        ),
        race_counts AS (
            SELECT 
                county_desc AS county,
                race_code,
                COUNT(*) AS count
            FROM raw.raw_voters
            WHERE status_cd IN ('A', 'I')
            GROUP BY county_desc, race_code
        )
        SELECT 
            ct.county,
            ct.total,
            COALESCE(MAX(CASE WHEN rc.race_code = 'W' THEN rc.count END), 0) AS white_count,
            COALESCE(MAX(CASE WHEN rc.race_code = 'B' THEN rc.count END), 0) AS black_count,
            COALESCE(MAX(CASE WHEN rc.race_code = 'A' THEN rc.count END), 0) AS asian_count,
            COALESCE(MAX(CASE WHEN rc.race_code = 'I' THEN rc.count END), 0) AS native_count,
            COALESCE(MAX(CASE WHEN rc.race_code = 'M' THEN rc.count END), 0) AS multi_count,
            COALESCE(MAX(CASE WHEN rc.race_code = 'O' THEN rc.count END), 0) AS other_count,
            COALESCE(MAX(CASE WHEN rc.race_code = 'P' THEN rc.count END), 0) AS pacific_count,
            COALESCE(MAX(CASE WHEN rc.race_code = 'U' THEN rc.count END), 0) AS undesig_count,
            COALESCE(MAX(CASE WHEN rc.race_code = 'W' THEN rc.count END), 0) * 100.0 / ct.total AS white_pct,
            COALESCE(MAX(CASE WHEN rc.race_code = 'B' THEN rc.count END), 0) * 100.0 / ct.total AS black_pct,
            COALESCE(MAX(CASE WHEN rc.race_code = 'A' THEN rc.count END), 0) * 100.0 / ct.total AS asian_pct,
            COALESCE(MAX(CASE WHEN rc.race_code = 'I' THEN rc.count END), 0) * 100.0 / ct.total AS native_pct,
            COALESCE(MAX(CASE WHEN rc.race_code = 'M' THEN rc.count END), 0) * 100.0 / ct.total AS multi_pct,
            COALESCE(MAX(CASE WHEN rc.race_code = 'O' THEN rc.count END), 0) * 100.0 / ct.total AS other_pct,
            COALESCE(MAX(CASE WHEN rc.race_code = 'P' THEN rc.count END), 0) * 100.0 / ct.total AS pacific_pct,
            COALESCE(MAX(CASE WHEN rc.race_code = 'U' THEN rc.count END), 0) * 100.0 / ct.total AS undesig_pct
        FROM county_totals ct
        LEFT JOIN race_counts rc ON ct.county = rc.county
        GROUP BY ct.county, ct.total
        ORDER BY ct.total DESC;
        """
    
    elif layer == 'gender':
        query = """
        WITH county_totals AS (
            SELECT 
                county_desc AS county,
                COUNT(*) AS total
            FROM raw.raw_voters
            WHERE status_cd IN ('A', 'I')
            GROUP BY county_desc
        ),
        gender_counts AS (
            SELECT 
                county_desc AS county,
                gender_code,
                COUNT(*) AS count
            FROM raw.raw_voters
            WHERE status_cd IN ('A', 'I')
            GROUP BY county_desc, gender_code
        )
        SELECT 
            ct.county,
            ct.total,
            COALESCE(MAX(CASE WHEN gc.gender_code = 'F' THEN gc.count END), 0) AS female_count,
            COALESCE(MAX(CASE WHEN gc.gender_code = 'M' THEN gc.count END), 0) AS male_count,
            COALESCE(MAX(CASE WHEN gc.gender_code = 'U' THEN gc.count END), 0) AS undesig_count,
            COALESCE(MAX(CASE WHEN gc.gender_code = 'F' THEN gc.count END), 0) * 100.0 / ct.total AS female_pct,
            COALESCE(MAX(CASE WHEN gc.gender_code = 'M' THEN gc.count END), 0) * 100.0 / ct.total AS male_pct,
            COALESCE(MAX(CASE WHEN gc.gender_code = 'U' THEN gc.count END), 0) * 100.0 / ct.total AS undesig_pct
        FROM county_totals ct
        LEFT JOIN gender_counts gc ON ct.county = gc.county
        GROUP BY ct.county, ct.total
        ORDER BY ct.total DESC;
        """
    
    else:
        raise ValueError(f"Invalid layer: {layer}")
    
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        logger.error(f"Failed to fetch county data for layer {layer}: {e}")
        raise

def get_precinct_data_by_county(engine: Engine, county: str) -> pd.DataFrame:
    """
    Get detailed precinct-level data for a specific county.
    
    Args:
        engine: SQLAlchemy engine
        county: County name
        
    Returns:
        DataFrame with precinct-level statistics including party breakdown
    """
    query = """
    WITH precinct_totals AS (
        SELECT 
            precinct_desc AS precinct,
            COUNT(*) AS total
        FROM raw.raw_voters
        WHERE status_cd IN ('A', 'I')
          AND county_desc = :county
        GROUP BY precinct_desc
    ),
    party_counts AS (
        SELECT 
            precinct_desc AS precinct,
            party_cd,
            COUNT(*) AS count
        FROM raw.raw_voters
        WHERE status_cd IN ('A', 'I')
          AND county_desc = :county
        GROUP BY precinct_desc, party_cd
    )
    SELECT 
        pt.precinct,
        pt.total,
        COALESCE(MAX(CASE WHEN pc.party_cd = 'DEM' THEN pc.count END), 0) AS dem_count,
        COALESCE(MAX(CASE WHEN pc.party_cd = 'REP' THEN pc.count END), 0) AS rep_count,
        COALESCE(MAX(CASE WHEN pc.party_cd = 'UNA' THEN pc.count END), 0) AS una_count,
        COALESCE(MAX(CASE WHEN pc.party_cd = 'DEM' THEN pc.count END), 0) * 100.0 / pt.total AS dem_pct,
        COALESCE(MAX(CASE WHEN pc.party_cd = 'REP' THEN pc.count END), 0) * 100.0 / pt.total AS rep_pct,
        COALESCE(MAX(CASE WHEN pc.party_cd = 'UNA' THEN pc.count END), 0) * 100.0 / pt.total AS una_pct
    FROM precinct_totals pt
    LEFT JOIN party_counts pc ON pt.precinct = pc.precinct
    GROUP BY pt.precinct, pt.total
    ORDER BY pt.total DESC;
    """
    try:
        return pd.read_sql(query, engine, params={"county": county})
    except Exception as e:
        logger.error(f"Failed to fetch precinct data for {county}: {e}")
        raise