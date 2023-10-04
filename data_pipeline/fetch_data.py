# 3rd parties
import requests
import zipfile
import pandas as pd

# Internals
import helpers

_SO_SURVEY_PREFIX = "stack-overflow-developer-survey"

def _unpack(zip_path, year):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        extract_path = f"data/{_SO_SURVEY_PREFIX}-{year}"
        zip_ref.extractall(extract_path)
        print(f"Successfully extracted the ZIP file to {extract_path}")

def _write(file_path, content, year):
    with open(file_path, 'wb') as f:
        f.write(content)
    print(f"Successfully downloaded the survey data for the year {year}.")

def fetch(year, use_cache) -> pd.DataFrame:
    """
    The 'Data Ingestion' stage
    
    Parameters:
    - year (int): The year for which you want to download the Stack Overflow developer survey data.
    """
    if use_cache:
        return _read_file(year, use_cache)
    else:
        _download_file_and_unpack(year)
        return _read_file(year, use_cache)

def _download_file_and_unpack(year):
    base_url = f"{helpers.get_base_url(year)}"

    url = f"{base_url}/{_SO_SURVEY_PREFIX}-{year}.zip"

    response = requests.get(url)

    if response.status_code == 200:
        file_path = f"data/{_SO_SURVEY_PREFIX}-{year}.zip"
        
        _write(file_path, response.content, year)
        _unpack(file_path, year)
    else:
        raise Exception(f"Failed to download the survey data for the year {year}. Status code: {response.status_code}")
    
def _read_file(year, cache):
    csv_path = _get_zip_path(year)
    # Read the CSV file into a Pandas DataFrame.
    try:
        df = pd.read_csv(csv_path)
        print(f"Successfully loaded the survey data for the year {year}.")
        return df

    except FileNotFoundError:
        print(f"Could not find the survey data for the year {year}.")
        if cache is False:
            return None
        else:
            _download_file_and_unpack(year)
            return _read_file(year, cache)
    
def _get_zip_path(year):
    return f"data/{_SO_SURVEY_PREFIX}-{year}/survey_results_public.csv"