#!/usr/bin/env python3
"""
ETL: Load pre-aggregated county-level voter registration files
"""
import os
import json
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load DB credentials
load_dotenv()
engine = create_engine(
    f"postgresql://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@"
    f"{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}"
)

RAW_DIR = os.path.join("data", "raw")
MANIFEST_PATH = os.path.join(RAW_DIR, "manifest.json")


def transform_registration(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform pre-aggregated registration data."""
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
    return df[['registration_date', 'county_desc', 'party', 'race', 'ethnicity', 'gender', 'age', 'total']]


def load_registration():
    """Load the most recent pre-aggregated registration file from manifest."""
    if not os.path.exists(MANIFEST_PATH):
        print("Manifest not found. Cannot proceed.")
        return

    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    latest_file = None
    for entry in reversed(manifest):
        filename = entry["filename"].lower()
        if filename.endswith(".txt") and "registration" in filename:
            latest_file = entry["filename"]
            break

    if not latest_file:
        print("No pre-aggregated registration .txt file found in manifest.")
        return

    file_path = os.path.join(RAW_DIR, latest_file)
    if not os.path.exists(file_path):
        print(f"File {latest_file} listed in manifest but not found in raw data folder.")
        return

    print(f"Loading aggregated registration data from {latest_file}...")

    df = pd.read_csv(file_path, sep='\t', dtype=str, encoding='latin1')
    clean = transform_registration(df)
    clean.to_sql('voter_registration', engine, schema='registration', if_exists='append', index=False)

    print("Aggregated registration data loaded successfully.")


if __name__ == "__main__":
    load_registration()
