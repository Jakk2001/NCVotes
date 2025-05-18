#!/usr/bin/env python3
"""
ETL for statewide election results
"""
import os
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


def transform_results(df: pd.DataFrame) -> pd.DataFrame:
    # Standardize columns
    df = df.rename(columns={
        'election_dt': 'election_date',
        'contest_name': 'contest',
        'candidate_name': 'candidate',
        'party_abbrv': 'party',
        'votes': 'votes'
    })
    df['election_date'] = pd.to_datetime(df['election_date'], errors='coerce').dt.date
    df['votes'] = pd.to_numeric(df['votes'], errors='coerce').fillna(0).astype(int)
    return df[['election_date','county','precinct','contest','district','candidate','party','votes']]


def load_results():
    files = [f for f in os.listdir(RAW_DIR) if 'results' in f and f.endswith('.txt')]
    for fname in files:
        path = os.path.join(RAW_DIR, fname)
        df = pd.read_csv(path, sep='	', dtype=str)
        clean = transform_results(df)
        clean.to_sql('election_results', engine, schema='elections', if_exists='append', index=False)
        print(f"Loaded election results from {fname}")


if __name__ == '__main__':
    load_results()