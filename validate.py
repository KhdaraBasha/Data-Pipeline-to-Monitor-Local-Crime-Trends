# ============================================================================
# Data Validation - Crime Trends Data Quality Assurance Module
# ============================================================================
# Description: This module contains utility functions for validating raw 
#              crime data including schema verification, data type checking,
#              datetime validation, and missing value detection.
# ============================================================================

# Import necessary modules
from datetime import datetime
from collections import Counter
import pandas as pd
import logging
# from perfect import task

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# NOTE: Do not call `extract_data_from_api()` at module import time. The ETL
# orchestration should call the extractor and then pass the resulting DataFrame
# into the validation functions. Calling the API at import time causes
# side-effects when modules are imported (for example, from `etl_pipeline`).

# ============================================================================
# UTILITIES - Data Validation Helper Functions
# ============================================================================

# ============================================================================
# Function: fn_check_valid_schema
# Purpose:  Validate that the DataFrame contains all expected columns for the
#           Cambridge Police dataset. Raises an error if schema is invalid.
# Parameters: df (pandas.DataFrame) - Input dataframe to validate
# Returns:  None (raises ValueError if schema mismatch)
# ============================================================================
def fn_check_valid_schema(df):
    '''
    Check whether the DataFrame content contains the expected columns for the Cambridge Ploice dataset.
    Otherwise, raise an error!.
    '''
    schema_cols = ['date_time', 'id', 'type', 'subtype', 'location', 'last_updated', 'description']
    
    if Counter(df.columns) != Counter(schema_cols):
        logger.error('DataFrame Schema validation failed.')
        raise ValueError('DataFrame Schema does not match with the expected schema.')

# ============================================================================
# Function: fn_check_numeric_id
# Purpose:  Convert 'id' column values to numeric type. Non-numeric IDs are
#           replaced with NaN for removal in downstream transformation steps.
# Parameters: df (pandas.DataFrame) - Input dataframe to process
# Returns:  pandas.DataFrame - Dataframe with numeric IDs (NaN for invalid)
# ============================================================================
def fn_check_numeric_id(df):
    '''
    Convert 'id' values to numeric.
    If any 'id' values are non-numeric, replace them with Nan, so they can be removed downstream in the data transformations.
    '''
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    return df

# ============================================================================
# Function: fn_verify_datetime
# Purpose:  Validate that all 'date_time' values conform to ISO 8601 format.
#           Raises an error if any datetime values are invalid or non-compliant.
# Parameters: df (pandas.DataFrame) - Input dataframe with 'date_time' column
# Returns:  None (raises ValueError if invalid datetime found)
# ============================================================================
def fn_verify_datetime(df):
    '''
    Verify 'date_time' values follow ISO 8601 format (https://www.iso.org/iso-8601-date-and-time-format.html).
    Raise a ValueError if any of the 'date_time' values are invalid.
    '''
    try:
        # Optimized vectorized validation
        pd.to_datetime(df['date_time'], errors='raise')
    except Exception as e:
        logger.error(f'Datetime validation failed: {e}')
        raise ValueError(f'Invalid datetime format detected: {e}')

# ============================================================================
# Function: fn_check_missing_values
# Purpose:  Check for missing/null values in critical columns required for
#           police incident records (datetime, ID, type, location). Raises
#           an error if any required field contains null values.
# Parameters: df (pandas.DataFrame) - Input dataframe to check
# Returns:  None (raises ValueError if missing values detected)
# ============================================================================
def fn_check_missing_values(df):
    '''
    Check whether there are any missing values in columns that require data.
    For police logs, each incident should have a datetime, ID, incident type, and location.
    '''
    required_cols = ['date_time', 'id', 'type', 'location']

    for col in required_cols:
        if df[col].isnull().sum() > 0:
            logger.error(f"Missing values found in column '{col}'")
            raise ValueError(f"Missing values are present in the '{col}' attribute.")

# ============================================================================
# Function: fn_validate_data
# Purpose:  Main orchestration function that applies all data quality validation
#           checks in sequence:
#           1. Schema validation
#           2. Numeric ID type conversion
#           3. DateTime format verification
#           4. Missing values detection
# Parameters: df (pandas.DataFrame) - Raw crime data to validate
# Returns:  pandas.DataFrame - Validated dataframe
# ============================================================================
def fn_validate_data(df):
    '''
    Check the data satisfies the following data quality checks:
    - schema is valid
    - IDs are numeric
    - datetime follows ISO 8601 format
    - no missing values in columns that require data
    '''
    logger.info("Starting data validation...")
    
    # Step 1: Validate DataFrame schema matches expected structure
    fn_check_valid_schema(df)

    # Step 2: Convert IDs to numeric type, replacing invalid values with NaN
    df = fn_check_numeric_id(df)

    # Step 3: Verify all datetime values follow ISO 8601 format
    fn_verify_datetime(df)

    # Step 4: Check for missing values in required columns
    fn_check_missing_values(df)

    logger.info("Data validation completed successfully.")
    return df

