"""
Download and extract statewide election results data from NC S3 archive
"""
import os
import requests
import zipfile
import json
from datetime import datetime
from utils.manifest import update_manifest


# Configuration
RESULTS_URL = "https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/2024_11_05/results_pct_20241105.zip"
OUTPUT_DIR = os.path.join("..", "data", "raw")


def download_zip(url: str, output_path: str):
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    with open(output_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)


def extract_zip(zip_path: str, extract_to: str):
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_to)

def update_manifest(filename, url):
    manifest_path = os.path.join("data", "raw", "manifest.json")
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)  # create dirs if needed

    entry = {
        "filename": filename,
        "url": url,
        "downloaded_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    if os.path.exists(manifest_path):
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    else:
        manifest = []

    manifest.append(entry)

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"election_results_statewide_{timestamp}.zip"
    zip_path = os.path.join(OUTPUT_DIR, zip_filename)

    print(f"Downloading election results to {zip_path}...")
    download_zip(RESULTS_URL, zip_path)

    print(f"Extracting results to {OUTPUT_DIR}...")
    extract_zip(zip_path, OUTPUT_DIR)

    print("Election results data setup complete.")

    update_manifest(zip_filename, RESULTS_URL)   # or RESULTS_URL

    print("Manifest Updated.")


if __name__ == "__main__":
    main()