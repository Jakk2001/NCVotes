#!/usr/bin/env python3
"""
setup_and_run_etl.py

Creates a small sample voter file in data/raw/ and then runs the raw voter ETL script.
"""

import os
import pandas as pd
import subprocess

# 1. Ensure data/raw directory exists
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

# 2. Define sample data
sample_data = [
    [1, 'Wake', '000000000001', 'NC0000000001', 'Doe', 'John', '', '', 'AC', 'Active', '', 'Active registration', '123 Main St', 'Raleigh', 'NC', '27601', '', '', '', '', '', '', '', '', '', '02/12/2022', 'W', 'N', 'DEM', 'M', '1980', '42', 'NC', 'Y', '01', 'Precinct 1'],
    [3, 'Orange', '000000000002', 'NC0000000002', 'Smith', 'Jane', '', '', 'AC', 'Active', '', 'Active registration', '456 Elm St', 'Chapel Hill', 'NC', '27514', '', '', '', '', '', '', '', '', '', '02/12/2022', 'B', 'N', 'REP', 'F', '1975', '47', 'NC', 'N', '02', 'Precinct 2']
]

col_names = [
    'county_id','county_desc','voter_reg_num','ncid','last_name','first_name',
    'middle_name','name_suffix_lbl','status_cd','voter_status_desc','reason_cd',
    'voter_status_reason_desc','res_street_address','res_city_desc','state_cd',
    'zip_code','mail_addr1','mail_addr2','mail_addr3','mail_addr4','mail_city',
    'mail_state','mail_zipcode','full_phone_number','confidential_ind','registr_dt',
    'race_code','ethnic_code','party_cd','gender_code','birth_year','age_at_year_end',
    'birth_state','drivers_lic','precinct_abbrv','precinct_desc'
]

# 3. Create DataFrame and save as tab-delimited without header
df = pd.DataFrame(sample_data, columns=col_names)
sample_path = os.path.join(RAW_DIR, 'ncvoter_Statewide_sample.txt')
df.to_csv(sample_path, sep='\t', index=False, header=False)
print(f"Sample file created at: {sample_path}")

# 4. Run the ETL loader for raw voters
etl_script = os.path.join(PROJECT_ROOT, 'etl', 'load_raw_voters.py')
print(f"Running ETL loader script: {etl_script}")
result = subprocess.run(['python', etl_script])

if result.returncode == 0:
    print("ETL loader ran successfully.")
else:
    print(f"ETL loader failed with exit code {result.returncode}.")
