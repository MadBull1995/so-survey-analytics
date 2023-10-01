# Main entry point for so-survey-analytics data pipeline
# This script is used to mini "ETL" process for so-survey data per year
# Resource: https://insights.stackoverflow.com/survey
# Steps:
#   1. fetching the .zip files and preform unpacking
#   2. pre-processing the data and validating for any invalids
#   3. loading the data to out provided PostgreSQL DB instance (See README.md for more setup information)

# 3rd parties


# Data pipeline internals
import fetch_data, \
    preprocess_data, \
    load_data, \
    helpers

sep = 40 * "*"

def main():
    print("so-survey-analytics data pipeline process starting...")

    # Global configurations and setup
    cfgs = helpers.setup()

    # Init years list - which we will iterate for the data fetch
    # TODO: Impl more intuative year parsing maybe using args
    years = [
        2023
    ]

    # (1) Fetching data + Unpacking .zip files per year
    for year in years:
        print(f"{sep}\nProcessing so-survey data for {year}\n{sep}")
        data = fetch_data.fetch(year)
    
        # (2) Preprocessing the raw data
        processed = preprocess_data.process(data)

        # (3) Load the processed data
        load_data.load(processed)

    print(f"finished loading so-survey-analytics data to {cfgs}")

if __name__ == '__main__':
    main()