from dotenv import load_dotenv
import os

def _load_db_env():
    return os.getenv('DATABASE_URL')

def setup():
    # Load the .env file
    load_dotenv()

    # Access variables
    database_url = _load_db_env()
    
    print(f"Configs:\n{database_url = }")
    
    return database_url

def get_base_url(year):
    """Some workaround to the fact that the 2023 raw data is in different route then older data points"""

    if year != 2023:
        return f"https://info.stackoverflowsolutions.com/rs/719-EMH-566/images"
    else:
        return "https://cdn.stackoverflow.co/files/jo7n4k8s/production/49915bfd46d0902c3564fd9a06b509d08a20488c.zip"