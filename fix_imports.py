#!/usr/bin/env python3
"""
Automatically update imports in migrated files.
"""
import re
from pathlib import Path

def update_imports(file_path):
    """Update imports in a Python file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # Update common imports
    replacements = [
        # Database imports
        (r'from utils\.db import get_engine', 'from src.database.connection import get_engine'),
        
        # Manifest imports
        (r'from utils\.manifest import update_manifest', 'from src.scraper import manifest'),
        (r'update_manifest\(', 'manifest.add_entry('),
        
        # Geo imports
        (r'from utils\.geo import load_county_geojson', '# Geo function now in this file'),
        
        # Style imports
        (r'from viz\.style import', 'from config.settings import'),
        
        # ETL/query imports
        (r'from etl\.choropleth import', 'from src.database.queries import'),
        
        # Settings imports for paths
        (r'OUTPUT_DIR = os\.path\.join\("data", "raw"\)', 
         '# Using RAW_DATA_DIR from config.settings'),
    ]
    
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)
    
    # Only write if changes were made
    if content != original:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✓ Updated: {file_path}")
        return True
    else:
        print(f"  No changes: {file_path}")
        return False

def main():
    """Update all Python files in src/"""
    src_dir = Path('src')
    python_files = list(src_dir.rglob('*.py'))
    
    print(f"Found {len(python_files)} Python files to check\n")
    
    updated_count = 0
    for py_file in python_files:
        if '__pycache__' not in str(py_file):
            if update_imports(py_file):
                updated_count += 1
    
    print(f"\n✓ Complete! Updated {updated_count} files")

if __name__ == '__main__':
    main()

