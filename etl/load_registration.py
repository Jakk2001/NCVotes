#!/usr/bin/env python3
"""
ETL for aggregated voter registration trends
"""
import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load DB credentials
load_dotenv()
engine = create_engine(
    f"postgresql://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@"
    f"{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}"
)

RAW_DIR = os.path.join('data', 'raw')


def transform_registration(df: pd.DataFrame) -> pd.DataFrame:
    # Rename and convert types
    df = df.rename(columns={
        'registr_dt': 'registration_date',
        'party_cd': 'party',
        'race_code': 'race',
        'ethnic_code': 'ethnicity',
        'age_at_year_end': 'age',
        'gender_code': 'gender',
        'total_voters': 'total'
    })
    df['registration_date'] = pd.to_datetime(df['registration_date'], format='%m/%d/%Y', errors='coerce').dt.date
    df['total'] = pd.to_numeric(df['total'], errors='coerce').fillna(0).astype(int)
    return df[['registration_date','county_desc','party','race','ethnicity','gender','age','total']]


def load_registration():
    files = [f for f in os.listdir(RAW_DIR) if 'registration' in f and f.endswith('.txt')]
    for fname in files:
        path = os.path.join(RAW_DIR, fname)
        df = pd.read_csv(path, sep='	', dtype=str)
        clean = transform_registration(df)
        # Append to table
        clean.to_sql('voter_registration', engine, schema='registration', if_exists='append', index=False)
        print(f"Loaded registration data from {fname}")


if __name__ == '__main__':
    load_registration()