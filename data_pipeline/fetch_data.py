# 3rd parties
import requests
import zipfile
import pandas as pd

# Internals
import helpers

_SO_SURVEY_PREFIX = "stack-overflow-developer-survey"

def _unpack(zip_path, year):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        extract_path = f"{_SO_SURVEY_PREFIX}-{year}"
        zip_ref.extractall(extract_path)
        print(f"Successfully extracted the ZIP file to {extract_path}")

def _write(file_path, content, year):
    with open(file_path, 'wb') as f:
        f.write(content)
    print(f"Successfully downloaded the survey data for the year {year}.")

def fetch(year):
    """
    The 'Data Ingestion' stage
    
    Parameters:
    - year (int): The year for which you want to download the Stack Overflow developer survey data.
    """
    
    base_url = f"{helpers.get_base_url(year)}"
    
    url = f"{base_url}/{_SO_SURVEY_PREFIX}-{year}.zip"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        file_path = f"{_SO_SURVEY_PREFIX}-{year}.zip"
        
        _write(file_path, response.content, year)
        _unpack(file_path, year)
        # Define the path to the extracted CSV file.
        csv_path = f"{_SO_SURVEY_PREFIX}-{year}/survey_results_public.csv"

        # Read the CSV file into a Pandas DataFrame.
        try:
            df = pd.read_csv(csv_path)
            print(f"Successfully loaded the survey data for the year {year}.")
            return df

        except FileNotFoundError:
            print(f"Could not find the survey data for the year {year}.")
            return None
        
    else:
        print(f"Failed to download the survey data for the year {year}. Status code: {response.status_code}")


