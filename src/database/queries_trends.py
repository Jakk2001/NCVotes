"""
Additional queries for registration trends analysis.
These queries track how demographics change over time.
"""
from sqlalchemy.engine import Engine
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def get_party_trends_over_time(engine: Engine) -> pd.DataFrame:
    """Get registration trends by party over time."""
    query = """
    SELECT 
        TO_DATE(registr_dt, 'MM/DD/YYYY') as registration_date,
        party_cd as party,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
      AND registr_dt ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
      AND party_cd IN ('DEM', 'REP', 'UNA', 'LIB', 'GRE')
    GROUP BY TO_DATE(registr_dt, 'MM/DD/YYYY'), party_cd
    ORDER BY registration_date, party_cd;
    """
    try:
        df = pd.read_sql(query, engine)
        df['registration_date'] = pd.to_datetime(df['registration_date'])
        return df
    except Exception as e:
        logger.error(f"Failed to fetch party trends: {e}")
        raise

def get_age_group_trends_over_time(engine: Engine) -> pd.DataFrame:
    """Get registration trends by age group over time."""
    query = """
    SELECT 
        TO_DATE(registr_dt, 'MM/DD/YYYY') as registration_date,
        age_group,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
      AND registr_dt ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
      AND age_group IS NOT NULL
      AND age_group != 'Unknown'
    GROUP BY TO_DATE(registr_dt, 'MM/DD/YYYY'), age_group
    ORDER BY registration_date, 
             CASE age_group
                WHEN '18-25' THEN 1
                WHEN '26-35' THEN 2
                WHEN '36-50' THEN 3
                WHEN '51-65' THEN 4
                WHEN '65+' THEN 5
             END;
    """
    try:
        df = pd.read_sql(query, engine)
        df['registration_date'] = pd.to_datetime(df['registration_date'])
        return df
    except Exception as e:
        logger.error(f"Failed to fetch age group trends: {e}")
        raise

def get_gender_trends_over_time(engine: Engine) -> pd.DataFrame:
    """Get registration trends by gender over time."""
    query = """
    SELECT 
        TO_DATE(registr_dt, 'MM/DD/YYYY') as registration_date,
        gender_code as gender,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
      AND registr_dt ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
      AND gender_code IN ('M', 'F', 'U')
    GROUP BY TO_DATE(registr_dt, 'MM/DD/YYYY'), gender_code
    ORDER BY registration_date, gender_code;
    """
    try:
        df = pd.read_sql(query, engine)
        df['registration_date'] = pd.to_datetime(df['registration_date'])
        return df
    except Exception as e:
        logger.error(f"Failed to fetch gender trends: {e}")
        raise

def get_race_trends_over_time(engine: Engine) -> pd.DataFrame:
    """Get registration trends by race over time."""
    query = """
    SELECT 
        TO_DATE(registr_dt, 'MM/DD/YYYY') as registration_date,
        race_code as race,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
      AND registr_dt ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
      AND race_code IN ('W', 'B', 'A', 'I', 'M', 'O', 'U')
    GROUP BY TO_DATE(registr_dt, 'MM/DD/YYYY'), race_code
    ORDER BY registration_date, race_code;
    """
    try:
        df = pd.read_sql(query, engine)
        df['registration_date'] = pd.to_datetime(df['registration_date'])
        return df
    except Exception as e:
        logger.error(f"Failed to fetch race trends: {e}")
        raise

def get_weekly_registration_counts(engine: Engine) -> pd.DataFrame:
    """Get weekly registration counts for seasonal pattern analysis."""
    query = """
    SELECT 
        DATE_TRUNC('week', TO_DATE(registr_dt, 'MM/DD/YYYY'))::date as week_start,
        COUNT(*) as total
    FROM raw.raw_voters
    WHERE status_cd IN ('A', 'I')
      AND registr_dt ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
    GROUP BY DATE_TRUNC('week', TO_DATE(registr_dt, 'MM/DD/YYYY'))::date
    ORDER BY week_start;
    """
    try:
        df = pd.read_sql(query, engine)
        df['week_start'] = pd.to_datetime(df['week_start'], utc=True).dt.tz_localize(None)
        return df
    except Exception as e:
        logger.error(f"Failed to fetch weekly counts: {e}")
        raise

def get_cumulative_registration(engine: Engine) -> pd.DataFrame:
    """Get cumulative registration totals over time."""
    query = """
    WITH daily_counts AS (
        SELECT 
            TO_DATE(registr_dt, 'MM/DD/YYYY') as registration_date,
            COUNT(*) as daily_total
        FROM raw.raw_voters
        WHERE status_cd IN ('A', 'I')
          AND registr_dt ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
        GROUP BY TO_DATE(registr_dt, 'MM/DD/YYYY')
    )
    SELECT 
        registration_date,
        daily_total,
        SUM(daily_total) OVER (ORDER BY registration_date) as cumulative_total
    FROM daily_counts
    ORDER BY registration_date;
    """
    try:
        df = pd.read_sql(query, engine)
        df['registration_date'] = pd.to_datetime(df['registration_date'])
        return df
    except Exception as e:
        logger.error(f"Failed to fetch cumulative registration: {e}")
        raise




