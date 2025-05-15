import json
import os
from datetime import datetime

MANIFEST_PATH = os.path.join("..", "data", "raw", "manifest.json")

def update_manifest(filename, url):
    entry = {
        "filename": filename,
        "url": url,
        "downloaded_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    # Load existing manifest or start new
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    else:
        manifest = []

    manifest.append(entry)
    # Write back
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

# Example usage after download and extraction:
# update_manifest(zip_filename, DATA_URL)