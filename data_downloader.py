import requests
import os
from tqdm import tqdm

# --- Configuration ---
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEARS = [str(year) for year in range(2011, 2025)] # 2011, 2012, ..., 2024
MONTHS = [f"{i:02d}" for i in range(1, 13)] # 01, 02, ..., 12
TAXI_TYPE = "yellow" # We'll stick to one taxi type
RAW_DATA_DIR = "data/raw"
LOOKUP_FILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
LOOKUP_FILE_PATH = "data/taxi_zone_lookup.csv"

def download_file(url, local_filepath):
    """Downloads a file from a URL with a progress bar."""
    print(f"Downloading {url} to {local_filepath}...")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total_size_in_bytes = int(r.headers.get('content-length', 0))
            block_size = 1024 # 1 Kibibyte
            
            progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
            with open(local_filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=block_size):
                    progress_bar.update(len(chunk))
                    f.write(chunk)
            progress_bar.close()
            
            if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
                print("ERROR, something went wrong during download")
        print(f"Successfully downloaded {local_filepath}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")

def main():
    # --- Create Directories ---
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    
    # --- Download Trip Data ---
    for year in YEARS:
        for month in MONTHS:
            filename = f"{TAXI_TYPE}_tripdata_{year}-{month}.parquet"
            url = f"{BASE_URL}/{filename}"
            local_filepath = os.path.join(RAW_DATA_DIR, filename)
            
            if not os.path.exists(local_filepath):
                download_file(url, local_filepath)
            else:
                print(f"File {local_filepath} already exists. Skipping.")

    # --- Download Taxi Zone Lookup File ---
    if not os.path.exists(LOOKUP_FILE_PATH):
        download_file(LOOKUP_FILE_URL, LOOKUP_FILE_PATH)
    else:
        print(f"File {LOOKUP_FILE_PATH} already exists. Skipping.")

if __name__ == "__main__":
    main()
