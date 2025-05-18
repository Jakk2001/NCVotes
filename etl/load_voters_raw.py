"""
ETL script to load the statewide raw voter registration .txt file
into the registration.raw_voters table in PostgreSQL.
"""
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

# Load DB credentials from .env at project root
load_dotenv()

DB_PARAMS = {
    "host": os.getenv("PGHOST"),
    "dbname": os.getenv("PGDATABASE"),
    "user": os.getenv("PGUSER"),
    "password": os.getenv("PGPASSWORD"),
    "port": os.getenv("PGPORT", 5432)
}

# Connect once, reuse for the run
conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()

def cleanse_dates(df: pd.DataFrame, col: str) -> pd.Series:
    # Convert date strings like '02/12/2022' to datetime.date
    return pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce").dt.date

def load_raw_voters(txt_path: str, chunksize: int = 100_000):
    # The file is tab-delimited with no header row (fields defined in layout)
    col_names = [
        "county_id", "county_desc", "voter_reg_num", "ncid", "last_name",
        "first_name", "middle_name", "name_suffix_lbl", "status_cd",
        "voter_status_desc", "reason_cd", "voter_status_reason_desc",
        "res_street_address", "res_city_desc", "state_cd", "zip_code",
        "mail_addr1", "mail_addr2", "mail_addr3", "mail_addr4",
        "mail_city", "mail_state", "mail_zipcode", "full_phone_number",
        "confidential_ind", "registr_dt", "race_code", "ethnic_code",
        "party_cd", "gender_code", "birth_year", "age_at_year_end",
        "birth_state", "drivers_lic", "precinct_abbrv", "precinct_desc",
        "municipality_abbrv", "municipality_desc", "ward_abbrv", "ward_desc",
        "cong_dist_abbrv", "super_court_abbrv", "judic_dist_abbrv",
        "nc_senate_abbrv", "nc_house_abbrv", "county_commiss_abbrv",
        "county_commiss_desc", "township_abbrv", "township_desc",
        "school_dist_abbrv", "school_dist_desc", "fire_dist_abbrv",
        "fire_dist_desc", "water_dist_abbrv", "water_dist_desc",
        "sewer_dist_abbrv", "sewer_dist_desc", "sanit_dist_abbrv",
        "sanit_dist_desc", "rescue_dist_abbrv", "rescue_dist_desc",
        "munic_dist_abbrv", "munic_dist_desc", "dist_1_abbrv",
        "dist_1_desc", "vtd_abbrv", "vtd_desc"
    ]

    for chunk in pd.read_csv(txt_path,
                             sep="\t",
                             names=col_names,
                             header=None,
                             chunksize=chunksize,
                             dtype=str,
                             na_values=["", "xx/xx/xxxx"]):
        # Clean up date column
        chunk["registr_dt"] = cleanse_dates(chunk, "registr_dt")

        # Build INSERT query
        cols = ", ".join(col_names)
        vals = ", ".join(["%s"] * len(col_names))
        insert_sql = f"""
            INSERT INTO registration.raw_voters ({cols})
            VALUES ({vals})
        """

        # Insert each row batch‑wise
        data = chunk.where(pd.notnull(chunk), None).values.tolist()
        cursor.executemany(insert_sql, data)
        conn.commit()
        print(f"Loaded chunk with {len(chunk)} rows")

    print("Finished loading raw voters")

if __name__ == "__main__":
    # Path to the extracted .txt inside data/raw/
    txt_file = os.path.join("data", "raw", "ncvoter_Statewide.txt")
    load_raw_voters(txt_file)
    cursor.close()
    conn.close()