import logging
from extract_api_data import extract_data_from_api, DATASET_IDS
from validate import fn_validate_data
from transform import fn_transform_data
from load import fn_load_data_table

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fn_etl():
    '''
    Execute the ETL pipeline:
    - Extract CPD incident data from the Socrata API
    - Validate and transform the extracted data to prepare it for storage
    - Import the transformed data into Postgres 
    '''

    logger.info('Extracting data...')
    # Extract the first dataset (Incident Data) as per default behavior
    dataset = DATASET_IDS[0]
    generated_df = extract_data_from_api(dataset['id'], dataset['name'])

    logger.info("Performing data quality checks...")
    validated_df = fn_validate_data(generated_df)

    logger.info("Performing data transformations...")
    transformed_df = fn_transform_data(validated_df)

    logger.info("Importing data into Postgres...")
    fn_load_data_table(transformed_df)

    return transformed_df

if __name__ == "__main__":
    try:
        df = fn_etl()
        logger.info(f"ETL pipeline completed. Processed {len(df)} records.")
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
