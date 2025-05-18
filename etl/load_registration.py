import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

conn = psycopg2.connect(
    host=os.getenv("PGHOST"),
    dbname=os.getenv("PGDATABASE"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    port=os.getenv("PGPORT")
)

def clean_and_insert(csv_path):
    df = pd.read_csv(csv_path)

    # Clean column names to lowercase
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Rename expected columns (example; adjust as needed)
    df = df.rename(columns={
        "reg_date": "registration_date",
        "county_desc": "county",
        "party_cd": "party",
        "race_code": "race",
        "ethnic_code": "ethnicity",
        "age": "age",
        "gender_code": "gender",
        "total_voters": "total"
    })

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO registration.voter_registration
                (county, party, race, ethnicity, age, gender, registration_date, total)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get("county"),
                row.get("party"),
                row.get("race"),
                row.get("ethnicity"),
                int(row.get("age") or 0),
                row.get("gender"),
                row.get("registration_date"),
                int(row.get("total") or 0)
            ))

    conn.commit()
    print("Finished loading:", csv_path)

if __name__ == "__main__":
    csv_file = "../data/raw/sample_registration.csv"  # Replace with your actual file
    clean_and_insert(csv_file)
