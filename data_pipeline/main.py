# Main entry point for so-survey-analytics data pipeline
# This script is used to mini "ETL" process for so-survey data per year
# Resource: https://insights.stackoverflow.com/survey
# Steps:
#   1. fetching the .zip files and preform unpacking
#   2. pre-processing the data and validating for any invalids
#   3. loading the data to out provided PostgreSQL DB instance (See README.md for more setup information)

# 3rd parties
import pandas as pd

# Data pipeline internals
from sys import argv
import fetch_data, \
    preprocess_data, \
    load_data, \
    helpers

_SEP = 40 * "*"
_AVAIL_YEARS = [
    2013,
    2014,
    2015,
    2016,
    2017,
    2018,
    2019,
    2020,
    2021,
    2022,
    2023,
    # TODO add you futuristic year
]

def main(args):
    print(f"so-survey-analytics data pipeline process starting...\n{args = }")

    # Global configurations and setup
    cfgs = helpers.setup()

    # Init years list - which we will iterate for the data fetch
    years, cache = _parse_args(args)
    
    # Create an empty DataFrame to hold all years' data
    all_years_data = pd.DataFrame()
  
    # Initialize a variable to hold the maximum responseId so far
    max_responseId = 0

    # (1) Fetching data + Unpacking .zip files per year
    for year in years:
        print(f"{_SEP}\nProcessing so-survey data for {year}\n{_SEP}")
        
        data = fetch_data.fetch(year, cache)
        data['survey_year'] = year

        response_pk_colname = 'ResponseId' if year >= 2021 else 'Respondent'

        # Update the responseId to make it globally unique
        data['ResponseId'] = data[response_pk_colname] + max_responseId
        if "Respondent" in data.columns:
            data = data.drop(columns=["Respondent"])
        
        # Update max_responseId for the next iteration
        max_responseId = data['ResponseId'].max()
    
        # (2) Preprocessing the raw data
        processed = preprocess_data.process(data, year)
        
        # Concatenate the processed data for the year to the all_years_data DataFrame
        all_years_data = pd.concat([all_years_data, processed], ignore_index=True)

    # (3) Load the processed data
    load_data.load(all_years_data, cfgs)

    print(f"\n* finished loading so-survey-analytics data to {cfgs}\nTotal # rows: {len(all_years_data)}")

def _parse_args(args):
    """Just parsing args passed from shell"""
    use_cache = "--cache" in args
    args = [arg for arg in args if arg != '--cache']
    if len(args) == 0:
        return (_AVAIL_YEARS, use_cache)
    else:
        for arg in args:
            if int(arg) not in _AVAIL_YEARS:
                raise Exception(
                    "survey year '{0}' is not available to data pipeline.\nif you think the year '{0}' should exists please open a new issue: https://github.com/MadBull1995/so-survey-analytics/issues/new"
                    .format(arg)
                )
        return ([int(y) for y in args], use_cache)

if __name__ == '__main__':
    args = argv[1:]
    main(args)