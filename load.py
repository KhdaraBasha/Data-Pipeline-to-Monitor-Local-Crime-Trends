# ============================================================================
# Load Data Into PostgreSQL - Crime Trends Loader
# ============================================================================
# Description: Functions to create a PostgreSQL connection, ensure the target
#              table exists, and load a transformed pandas DataFrame into the
#              `cpd_db.tb_cpd_incidents` table.
# ============================================================================

# Import necessary modules
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# Function: fn_get_db_engine
# Purpose:  Create and return a SQLAlchemy engine for the Postgres DB.
# Params:   None (reads connection details from environment variables)
# Returns:  SQLAlchemy engine or None on failure
# ----------------------------------------------------------------------------
def fn_get_db_engine():
    '''
    Create and return a SQLAlchemy engine using credentials stored in environment variables.
    '''
    load_dotenv()

    # database connection credentials
    host = os.getenv('DB_HOST')
    database = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    port = os.getenv('DB_PORT')

    try:
        # URL-encode the password to handle special characters like @
        encoded_password = quote_plus(password)
        engine = create_engine(f"postgresql://{user}:{encoded_password}@{host}:{int(port)}/{database}")
        return engine

    except Exception as e:
        logger.error(f'Error while creating database engine: {e}')
        return None

# ----------------------------------------------------------------------------
# Function: fn_create_table
# Purpose:  Ensure the target table for incidents exists in Postgres. Creates
#           the table if it does not already exist.
# Params:   engine (SQLAlchemy Engine) - Database engine to use
# Returns:  None
# ----------------------------------------------------------------------------
def fn_create_table(engine):
    '''
    Create the `cpd_db.tb_cpd_incidents` table with the expected schema if it
    does not already exist.
    '''
    if engine is None:
        logger.error("Database engine is None, cannot create table.")
        return

    try:
        # Create Table SQL Query
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS cpd_db.tb_cpd_incidents (
                date_time TIMESTAMP,
                id INTEGER PRIMARY KEY,
                type TEXT,
                subtype TEXT,
                location TEXT,
                description TEXT,
                last_updated TIMESTAMP,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                hour INTEGER,
                minute INTEGER,
                second INTEGER
            )
        '''

        # Execute query to create table using SQLAlchemy connection
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.commit()
            
    except Exception as e:
        logger.error(f'Error while creating table: {e}')

# ----------------------------------------------------------------------------
# Function: fn_load_data_table
# Purpose:  Load a transformed pandas DataFrame into the Postgres table.
# Params:   df (pandas.DataFrame) - Transformed data to insert
# Returns:  None
# Notes:    Uses SQLAlchemy engine for bulk insert via pandas.to_sql.
# ----------------------------------------------------------------------------
def fn_load_data_table(df):
    '''
    Loads the transformed data passed in as a DataFrame into the
    `cpd_db.tb_cpd_incidents` table in the configured Postgres instance.
    '''
    # Get database engine
    engine = fn_get_db_engine()
    
    if engine is None:
        logger.error("Failed to get database engine. Skipping load.")
        return

    # Ensure the table exists before attempting to insert
    fn_create_table(engine)

    # Insert data into Postgres DB into the 'tb_cpd_incidents' table under
    # the 'cpd_db' schema. Using the `schema` parameter is recommended when
    # writing to a specific schema rather than embedding the schema in table
    # name. We set `if_exists='replace'` to overwrite during tests; change as
    # needed for production (e.g., 'append').
    try:
        df.to_sql(
            name='tb_cpd_incidents',
            con=engine,
            schema='cpd_db',
            if_exists='replace',
            index=False,
        )
        logger.info("Data loaded successfully.")
    except Exception as e:
        logger.error(f'Error while loading data into table: {e}')


