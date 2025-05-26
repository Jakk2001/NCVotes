# viz/etl/choropleth.py
import pandas as pd
from utils.db import get_engine

def get_registration_by_county():
    engine = get_engine()
    query = """
     SELECT 
        county AS county, 
        SUM(total) AS registered
    FROM registration.voter_registration
    GROUP BY county
    """
    return pd.read_sql_query(query, engine)
