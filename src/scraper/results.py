"""
Download and extract statewide election results data from NC S3 archive.
Improved version with error handling, logging, and validation.
"""
import zipfile
from datetime import datetime
from pathlib import Path
import requests
import logging
from config.settings import DATA_URLS, RAW_DATA_DIR
from src.scraper import manifest

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def download_file(url: str, output_path: Path, chunk_size: int = 8192) -> bool:
    """
    Download a file from URL with progress tracking.
    
    Args:
        url: URL to download from
        output_path: Path to save file
        chunk_size: Size of chunks to download
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Downloading from {url}")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = (downloaded / total_size) * 100
                        logger.debug(f"Download progress: {percent:.1f}%")
        
        logger.info(f"Download complete: {output_path}")
        return True
        
    except requests.RequestException as e:
        logger.error(f"Failed to download file: {e}")
        return False
    except IOError as e:
        logger.error(f"Failed to write file: {e}")
        return False

def extract_zip(zip_path: Path, extract_to: Path) -> bool:
    """
    Extract all contents of a zip file.
    
    Args:
        zip_path: Path to zip file
        extract_to: Directory to extract to
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Extracting {zip_path} to {extract_to}")
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(extract_to)
        logger.info("Extraction complete")
        return True
        
    except zipfile.BadZipFile as e:
        logger.error(f"Invalid zip file: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to extract zip: {e}")
        return False

def scrape_results(election_date: str = "2024_11_05") -> bool:
    """
    Main function to download and extract election results data.
    
    Args:
        election_date: Election date in YYYY_MM_DD format
        
    Returns:
        True if successful, False otherwise
    """
    # Use configured URL or construct one
    url = DATA_URLS.get("results_2024", 
        f"https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/{election_date}/results_pct_{election_date}.zip")
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"election_results_{election_date}_{timestamp}.zip"
    zip_path = RAW_DATA_DIR / zip_filename
    
    # Ensure output directory exists
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Download the zip file
    if not download_file(url, zip_path):
        return False
    
    # Verify the file was downloaded
    if not zip_path.exists() or zip_path.stat().st_size == 0:
        logger.error("Downloaded file is missing or empty")
        return False
    
    # Add zip file to manifest
    manifest.add_entry(
        filename=zip_filename,
        url=url,
        file_type="results_zip",
        metadata={
            "size_bytes": zip_path.stat().st_size,
            "election_date": election_date
        }
    )
    
    # Extract the zip file
    if not extract_zip(zip_path, RAW_DATA_DIR):
        return False
    
    # Log extracted files to manifest
    extracted_count = 0
    for file_path in RAW_DATA_DIR.glob("*.txt"):
        if "results" in file_path.name.lower():
            manifest.add_entry(
                filename=file_path.name,
                url=url,
                file_type="results_data",
                metadata={
                    "size_bytes": file_path.stat().st_size,
                    "extracted_from": zip_filename,
                    "election_date": election_date
                }
            )
            extracted_count += 1
            logger.info(f"Registered extracted file: {file_path.name}")
    
    if extracted_count == 0:
        logger.warning("No results .txt files found after extraction")
        return False
    
    logger.info(f"Successfully scraped results data: {extracted_count} files extracted")
    return True

def main():
    """Entry point for command-line execution."""
    success = scrape_results()
    if not success:
        logger.error("Results scraping failed")
        exit(1)
    logger.info("Results scraping completed successfully")

if __name__ == "__main__":
    main()