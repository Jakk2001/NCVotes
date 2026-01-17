"""
Manifest file management for tracking downloaded data files.
Provides a single source of truth for data file provenance.
"""
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import logging
from config.settings import MANIFEST_PATH

logger = logging.getLogger(__name__)

def load_manifest() -> List[Dict]:
    """
    Load the manifest file.
    
    Returns:
        List of manifest entries
    """
    if not MANIFEST_PATH.exists():
        logger.info("No existing manifest found, creating new one")
        return []
    
    try:
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        logger.debug(f"Loaded manifest with {len(manifest)} entries")
        return manifest
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse manifest file: {e}")
        return []

def save_manifest(manifest: List[Dict]) -> None:
    """
    Save the manifest file.
    
    Args:
        manifest: List of manifest entries
    """
    try:
        MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Manifest saved with {len(manifest)} entries")
    except Exception as e:
        logger.error(f"Failed to save manifest: {e}")
        raise

def add_entry(filename: str, url: str, file_type: str = "unknown", 
              metadata: Optional[Dict] = None) -> None:
    """
    Add a new entry to the manifest.
    
    Args:
        filename: Name of the downloaded file
        url: Source URL
        file_type: Type of file (registration, results, etc.)
        metadata: Additional metadata dictionary
    """
    manifest = load_manifest()
    
    entry = {
        "filename": filename,
        "url": url,
        "file_type": file_type,
        "downloaded_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "metadata": metadata or {}
    }
    
    manifest.append(entry)
    save_manifest(manifest)
    logger.info(f"Added manifest entry for {filename}")

def get_latest_file(file_type: str) -> Optional[Dict]:
    """
    Get the most recent file of a given type.
    
    Args:
        file_type: Type of file to find
        
    Returns:
        Manifest entry dict or None if not found
    """
    manifest = load_manifest()
    
    # Filter by type and sort by download time
    matching = [e for e in manifest if e.get("file_type") == file_type]
    if not matching:
        logger.warning(f"No files of type '{file_type}' found in manifest")
        return None
    
    latest = max(matching, key=lambda x: x["downloaded_at"])
    logger.info(f"Found latest {file_type} file: {latest['filename']}")
    return latest

def get_all_files(file_type: Optional[str] = None) -> List[Dict]:
    """
    Get all files, optionally filtered by type.
    
    Args:
        file_type: Optional file type filter
        
    Returns:
        List of manifest entries
    """
    manifest = load_manifest()
    
    if file_type:
        return [e for e in manifest if e.get("file_type") == file_type]
    return manifest

def file_exists(filename: str) -> bool:
    """
    Check if a file is already in the manifest.
    
    Args:
        filename: Name of file to check
        
    Returns:
        True if file exists in manifest
    """
    manifest = load_manifest()
    return any(e["filename"] == filename for e in manifest)