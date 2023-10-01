# Main entry point for so-survey-analytics data pipeline
# This script is used to mini "ETL" process for so-survey data per year
# Resource: https://insights.stackoverflow.com/survey
# Steps:
#   1. fetching the .zip files and preform unpacking
#   2. pre-processing the data and validating for any invalids
#   3. loading the data to out provided PostgreSQL DB instance (See README.md for more setup information)

# 3rd parties


# Data pipeline internals
from sys import argv
import fetch_data, \
    preprocess_data, \
    load_data, \
    helpers

_SEP = 40 * "*"
_AVAIL_YEARS = [
    2023,
]

def main(args):
    print(f"so-survey-analytics data pipeline process starting...\n{args = }")

    # Global configurations and setup
    cfgs = helpers.setup()

    # Init years list - which we will iterate for the data fetch
    years = _parse_args(args)

    # (1) Fetching data + Unpacking .zip files per year
    for year in years:
        print(f"{_SEP}\nProcessing so-survey data for {year}\n{_SEP}")
        data = fetch_data.fetch(year)
    
        # (2) Preprocessing the raw data
        processed = preprocess_data.process(data)

        # (3) Load the processed data
        load_data.load(processed)

    print(f"finished loading so-survey-analytics data to {cfgs}")

def _parse_args(args):
    if len(args) == 0:
        return None
    else:
        for year in args:
            if int(year) not in _AVAIL_YEARS:
                raise Exception(
                    "survey year '{0}' is not available to data pipeline.\nif you think the year '{0}' should exists please open a new issue: https://github.com/MadBull1995/so-survey-analytics/issues/new"
                    .format(year)
                )
        return [int(y) for y in  args]

if __name__ == '__main__':
    args = argv[1:]
    main(args)