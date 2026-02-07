# **********************************************************
# Creating a Data Pipeline to Monitor Local Crime Trends
# **********************************************************

# **********************************************************
# Import necessary libraries
# **********************************************************
from dotenv import load_dotenv
import pandas as pd
from sodapy import Socrata
import os
import logging
import datetime as dt
from datetime import timedelta
# from prefect import task

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# **********************************************************
# Dataset IDs Configuration
# **********************************************************
DATASET_IDS = [
    {"name": "Incident_Data", "id": "3gki-wyrb"},
    {"name": "Budget_Salaries", "id": "ixg8-tyau"},
    {"name": "Crime_Reports", "id": "xuad-73uj"},
    {"name": "Computer_Aided_Dispatch_Entries", "id": "2z9k-mv9g"},
    {"name": "Housing_Median_Sales_Prices", "id": "9nnq-4isb"},
    {"name": "Building_Permits_Addition_Alteration", "id": "qu2z-8suj"}
]

# API Endpoints reference
# Crime Reports: https://data.cambridgema.gov/api/v3/views/xuad-73uj/query.json
# Budget - Salaries: https://data.cambridgema.gov/api/v3/views/ixg8-tyau/query.json
# Computer Aided Dispatch Entries: https://data.cambridgema.gov/api/v3/views/ppai-cur6/query.json
# Commonwealth Connect Service Requests: https://data.cambridgema.gov/api/v3/views/2z9k-mv9g/query.json
# Housing Median Sales Prices: https://data.cambridgema.gov/api/v3/views/9nnq-4isb/query.json
# Building Permits: Addition/Alteration: https://data.cambridgema.gov/api/v3/views/qu2z-8suj/query.json

# **********************************************************
# Data Extraction Functions
# ********************************************************** 
# @task(retries=3, retry_delay_seconds=[10,10,10]) # Retry the task up to 3 times with a delay of 10 seconds between each retry
def extract_data_from_api(dataset_id: str, data_category: str, limit: int = 2000) -> pd.DataFrame:
    """
    Extracts different categories of data from Cambridge Police Department datasets using the Socrata Open API.

    Args:
        dataset_id (str): The unique identifier for the dataset on Socrata.
        data_category (str): The name/category of the data being extracted (used for logging).
        limit (int): Maximum number of records to retrieve per API request. Defaults to 2000.

    Returns:
        pd.DataFrame: DataFrame containing all extracted records from the specified dataset.
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Retrieve API credentials from environment variables
    SITE_LINK = os.getenv('Site_Link')
    USER_NAME = os.getenv('User_Name')
    PASSWORD = os.getenv('Password')
    App_Token = os.getenv('App_Token')
    timeout_sec = 30

    # Initialize Socrata client with authentication
    client = Socrata(
                 'data.cambridgema.gov',
                 App_Token,
                 username=USER_NAME,
                 password=PASSWORD,
                 timeout=timeout_sec
                 )
    
    # Paginate through all available records using offset
    all_results = []
    offset = 0

    while True:
        # Fetch a batch of records from the API
        results = client.get(dataset_id, limit=limit, offset=offset)
        if len(results) == 0:
            break
        all_results.extend(results)
        # Move to next batch
        offset += limit

    logger.info(f'Total Records retrieved for {data_category}: {len(all_results)}')

    # Convert records to DataFrame
    return pd.DataFrame.from_records(all_results)


# **********************************************************
# Example Usage
# **********************************************************
if __name__ == "__main__":
    # Example: Extract Incident Data
    # Extracting the first dataset as a test
    dataset = DATASET_IDS[0]
    dataset_id = dataset['id']
    name = dataset['name']
    
    logger.info(f"Extracting {name}...")
    df = extract_data_from_api(dataset_id, name, 2000)
    logger.info(f"Head of data:\n{df.head()}")


