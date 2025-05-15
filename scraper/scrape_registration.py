"""
Download and extract statewide voter registration data from NC S3 archive
"""
import os
import requests
import zipfile
from datetime import datetime
from utils.manifest import update_manifest


# Configuration
DATA_URL = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip"
OUTPUT_DIR = os.path.join("..", "data", "raw")


def download_zip(url: str, output_path: str):
    """Download zip file from URL"""
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    with open(output_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)


def extract_zip(zip_path: str, extract_to: str):
    """Extract all contents of a zip file"""
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_to)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"ncvoter_statewide_{timestamp}.zip"
    zip_path = os.path.join(OUTPUT_DIR, zip_filename)

    print(f"Downloading statewide data to {zip_path}...")
    download_zip(DATA_URL, zip_path)

    print(f"Extracting data to {OUTPUT_DIR}...")
    extract_zip(zip_path, OUTPUT_DIR)

    print("Registration data setup complete.")

    update_manifest(zip_filename, DATA_URL)   # or RESULTS_URL

    print("Manifest Updated.")


if __name__ == "__main__":
    main()