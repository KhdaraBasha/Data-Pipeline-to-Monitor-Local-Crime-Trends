# Data Pipeline to Monitor Local Crime Trends

## Project Overview
This project processes local crime data from the Cambridge Police Department to monitor and analyze local crime trends. It utilizes an ETL (Extract, Transform, Load) pipeline to fetch data from the Socrata Open Data API, validate its quality, transform it for analysis, and load it into a PostgreSQL database.

## Architecture
The pipeline consists of the following stages:
1.  **Extract**: Fetches incident data from the Cambridge Open Data API (`extract_api_data.py`).
2.  **Validate**: Performs data quality checks including schema validation, numeric ID verification, ISO 8601 datetime format checks, and missing value detection (`validate.py`).
3.  **Transform**: Cleanses data by removing duplicates, filtering invalid rows, and decomposing datetime fields into granular components (year, month, day, etc.) (`transform.py`).
4.  **Load**: Inserts the processed data into a PostgreSQL database table `cpd_db.tb_cpd_incidents` (`load.py`).

## Prerequisites
- **Python 3.x**
- **PostgreSQL**: A running PostgreSQL instance.
- **Socrata API Token**: Required for high-volume requests.

## Setup
1.  **Clone the repository**.
2.  **Install dependencies** using the provided `requirements.txt`:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Environment Variables**:
    Create a `.env` file in the root directory with the following variables:
    ```env
    # Socrata API Credentials
    App_Token=your_app_token
    User_Name=your_username
    Password=your_password
    Site_Link=data.cambridgema.gov
    
    # Database Credentials
    DB_HOST=localhost
    DB_NAME=your_db_name
    DB_USER=your_db_user
    DB_PASS=your_db_password
    DB_PORT=5432
    ```

## Usage
The pipeline can be orchestrated using the `etl_pipeline.ipynb` notebook or by importing the modules sequentially in a Python script:

```python
from extract_api_data import extract_data_from_api
from validate import fn_validate_data
from transform import fn_transform_data
from load import fn_load_data_table

def run_pipeline():
    # 1. Extract
    print("Extracting data...")
    df = extract_data_from_api()
    
    # 2. Validate
    print("Validating data...")
    df = fn_validate_data(df)
    
    # 3. Transform
    print("Transforming data...")
    df = fn_transform_data(df)
    
    # 4. Load
    print("Loading data...")
    fn_load_data_table(df)
    print("Pipeline completed successfully.")

if __name__ == "__main__":
    run_pipeline()
```

## Testing
Each module includes a corresponding test notebook (e.g., `extract_api_data_test.ipynb`, `validate_test.ipynb`) for unit testing and interactive debugging.
