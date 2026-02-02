"""
Query functions for key stats on trends page.
Calculates registration trends matching the patterns from queries.py
"""
from sqlalchemy.engine import Engine
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def get_key_stats(engine: Engine) -> dict:
    """
    Calculate key statistics about voter registrations.
    Returns current totals and new registrations in different timeframes.
    """
    stats = {}
    
    # Get current total (matches get_registration_by_party pattern)
    stats['current_total'] = get_current_total(engine)
    
    # Get new registrations by timeframe
    stats['new_registrations'] = get_new_registrations_summary(engine)
    
    # Get party breakdown of new registrations
    stats['party_new_regs'] = get_party_new_registrations_summary(engine)
    
    # Get age group breakdown of new registrations
    stats['age_new_regs'] = get_age_new_registrations_summary(engine)
    
    return stats

def get_current_total(engine: Engine) -> int:
    """
    Get current total registered voters.
    Matches pattern from get_registration_by_party in queries.py
    """
    query = """
    SELECT COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
    """
    try:
        df = pd.read_sql(query, engine)
        return int(df.iloc[0]['total'])
    except Exception as e:
        logger.error(f"Failed to fetch current total: {e}")
        return 0

def get_new_registrations_summary(engine: Engine) -> dict:
    """Get counts of voters who registered in each timeframe."""
    
    # Calculate date thresholds
    today = datetime.now()
    dates = {
        '2_weeks': (today - timedelta(weeks=2)).strftime('%m/%d/%Y'),
        'last_election': '11/05/2024',
        'last_general': '11/05/2024', 
        '2_years': (today - timedelta(days=730)).strftime('%m/%d/%Y'),
        '4_years': (today - timedelta(days=1460)).strftime('%m/%d/%Y')
    }
    
    results = {}
    
    for label, cutoff_date in dates.items():
        query = f"""
        SELECT COUNT(*) as total
        FROM raw.raw_voters
        WHERE status_cd IN ('A', 'I')
          AND registr_dt ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
          AND TO_DATE(registr_dt, 'MM/DD/YYYY') > TO_DATE('{cutoff_date}', 'MM/DD/YYYY')
        """
        
        try:
            df = pd.read_sql(query, engine)
            results[label] = int(df.iloc[0]['total']) if not df.empty else 0
        except Exception as e:
            logger.error(f"Failed to fetch new registrations for {label}: {e}")
            results[label] = 0
    
    return results

def get_party_new_registrations_summary(engine: Engine) -> dict:
    """
    Get party breakdown of new registrations in each timeframe.
    Matches pattern from get_party_by_age_group in queries.py
    """
    
    today = datetime.now()
    dates = {
        '2_weeks': (today - timedelta(weeks=2)).strftime('%m/%d/%Y'),
        'last_election': '11/05/2024',
        'last_general': '11/05/2024',
        '2_years': (today - timedelta(days=730)).strftime('%m/%d/%Y'),
        '4_years': (today - timedelta(days=1460)).strftime('%m/%d/%Y')
    }
    
    results = {}
    
    for label, cutoff_date in dates.items():
        query = f"""
        SELECT 
            party_cd as party,
            COUNT(*) as total
        FROM raw.raw_voters
        WHERE status_cd IN ('A', 'I')
          AND party_cd IN ('DEM', 'REP', 'UNA', 'LIB', 'GRE')
          AND registr_dt ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
          AND TO_DATE(registr_dt, 'MM/DD/YYYY') > TO_DATE('{cutoff_date}', 'MM/DD/YYYY')
        GROUP BY party_cd
        ORDER BY party_cd;
        """
        
        try:
            df = pd.read_sql(query, engine)
            results[label] = df.to_dict('records') if not df.empty else []
        except Exception as e:
            logger.error(f"Failed to fetch party new registrations for {label}: {e}")
            results[label] = []
    
    return results

def get_age_new_registrations_summary(engine: Engine) -> dict:
    """
    Get age group breakdown of new registrations in each timeframe.
    Matches pattern from get_age_group_breakdown in queries.py
    """
    
    today = datetime.now()
    dates = {
        '2_weeks': (today - timedelta(weeks=2)).strftime('%m/%d/%Y'),
        'last_election': '11/05/2024',
        'last_general': '11/05/2024',
        '2_years': (today - timedelta(days=730)).strftime('%m/%d/%Y'),
        '4_years': (today - timedelta(days=1460)).strftime('%m/%d/%Y')
    }
    
    results = {}
    
    for label, cutoff_date in dates.items():
        query = f"""
        SELECT 
            age_group,
            COUNT(*) as total
        FROM raw.raw_voters
        WHERE status_cd IN ('A', 'I')
          AND age_group IS NOT NULL
          AND age_group != 'Unknown'
          AND registr_dt ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
          AND TO_DATE(registr_dt, 'MM/DD/YYYY') > TO_DATE('{cutoff_date}', 'MM/DD/YYYY')
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
            df = pd.read_sql(query, engine)
            results[label] = df.to_dict('records') if not df.empty else []
        except Exception as e:
            logger.error(f"Failed to fetch age new registrations for {label}: {e}")
            results[label] = []
    
    return results