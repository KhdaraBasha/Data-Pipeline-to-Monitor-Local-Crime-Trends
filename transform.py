# ============================================================================
# Transform Data - Crime Trends Data Processing Module
# ============================================================================
# Description: This module contains utility functions for transforming raw 
#              crime data including deduplication, validation, and datetime parsing.
# ============================================================================

# Import necessary modules
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# UTILITIES - Data Transformation Helper Functions
# ============================================================================

# ============================================================================
# Function: fn_remove_duplicates
# Purpose:  Remove duplicate rows from a dataframe based on the 'id' column,
#           keeping only the first occurrence of each duplicate ID.
# Parameters: df (pandas.DataFrame) - Input dataframe to process
# Returns:  pandas.DataFrame - Deduplicated dataframe
# ============================================================================
def fn_remove_duplicates(df):
    '''
    Remove duplicate rows from dataframe based on 'id' column. Keep the first occurrence.
    '''
    initial_count = len(df)
    df = df.drop_duplicates(subset=['id'], keep='first')
    dropped_count = initial_count - len(df)
    if dropped_count > 0:
        logger.info(f"Dropped {dropped_count} duplicate records.")
    return df

# ============================================================================
# Function: fn_remove_invalid_rows
# Purpose:  Remove rows with invalid or missing IDs (NaN values) from the
#           dataframe, as these represent non-numeric or missing crime IDs.
# Parameters: df (pandas.DataFrame) - Input dataframe to process
# Returns:  None (modifies dataframe in-place with dropna)
# ============================================================================
def fn_remove_invalid_rows(df):
    '''
    Remove rows where the 'id' is NaN, as these IDs were identified as non-numeric.
    '''
    # Return a dataframe with rows that have non-null 'id' values
    initial_count = len(df)
    df = df.dropna(subset=['id'])
    dropped_count = initial_count - len(df)
    if dropped_count > 0:
        logger.info(f"Dropped {dropped_count} rows with invalid IDs.")
    return df

# ============================================================================
# Function: fn_split_datetime
# Purpose:  Parse and decompose the 'date_time' column into separate
#           temporal components (year, month, day, hour, minute, second) for
#           granular time-based analysis of crime trends.
# Parameters: df (pandas.DataFrame) - Input dataframe with 'date_time' column
# Returns:  pandas.DataFrame - Dataframe with new temporal columns added
# ============================================================================
def fn_split_datetime(df):
    '''
    Split the date_time column into separate year, month, day, and time columns.
    '''
    # convert to datetime
    df['date_time'] = pd.to_datetime(df['date_time'])

    # Extract year, month, day, and time 
    df['year'] = df['date_time'].dt.year
    df['month'] = df['date_time'].dt.month
    df['day'] = df['date_time'].dt.day
    df['hour'] = df['date_time'].dt.hour
    df['minute'] = df['date_time'].dt.minute
    df['second'] = df['date_time'].dt.second
    
    return df

# ============================================================================
# Function: fn_transform_data
# Purpose:  Main orchestration function that applies all data transformation
#           steps to raw crime data in a sequential pipeline:
#           1. Deduplication
#           2. Validation (remove invalid IDs)
#           3. Datetime decomposition
# Parameters: df (pandas.DataFrame) - Raw crime data dataframe
# Returns:  pandas.DataFrame - Fully transformed and cleaned dataframe
# ============================================================================
def fn_transform_data(df):
    '''
    Apply the following transformations to the passed in dataframe:
    - deduplicate records (keep the first)
    - remove invalid rows
    - split datetime into year, month, day, and time columns
    '''
    logger.info("Starting data transformation...")

    # Step 1: Remove duplicate records based on 'id' column
    df = fn_remove_duplicates(df)
    
    # Step 2: Remove rows with invalid/NaN IDs
    df = fn_remove_invalid_rows(df)
    
    # Step 3: Split datetime column into separate date/time components
    df = fn_split_datetime(df)

    logger.info("Data transformation completed.")
    return df

