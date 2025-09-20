#!/usr/bin/env python3
"""
ETL: Load raw statewide voter registration file into raw_voters table (chunked for large files)
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


def load_raw_voters():
    """Load raw voter file in manageable chunks to avoid memory issues."""
    if not os.path.exists(MANIFEST_PATH):
        print("Manifest not found. Cannot proceed.")
        return

    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    latest_file = None
    for entry in reversed(manifest):
        filename = entry["filename"].lower()
        if filename.endswith(".txt") and "ncvoter" in filename:
            latest_file = entry["filename"]
            break

    if not latest_file:
        print("No extracted voter registration .txt file found in manifest.")
        return

    file_path = os.path.join(RAW_DIR, latest_file)
    if not os.path.exists(file_path):
        print(f"File {latest_file} listed in manifest but not found in raw data folder.")
        return

    print(f"Loading raw voter data from {latest_file} in chunks...")

    chunksize = 10000
    chunk_counter = 0
    for chunk in pd.read_csv(file_path, sep='\t', dtype=str, encoding='latin1', chunksize=chunksize):
        chunk.to_sql('raw_voters', engine, schema='registration', if_exists='append', index=False)
        chunk_counter += 1
        print(f"Inserted chunk {chunk_counter} ({chunk.shape[0]} rows)")

    print("Raw voter data loaded successfully.")


if __name__ == "__main__":
    load_raw_voters()
