# Approach Note: Data Pipeline to Monitor Local Crime Trends

## Problem Statement
The goal is to monitor local crime trends in Cambridge, MA. This requires a reliable automated system to ingest, clean, and store crime incident data from the Cambridge Police Department for downstream analysis and reporting.

## Solution Architecture
We implemented a modular ETL (Extract, Transform, Load) pipeline using Python. The solution separates concerns into distinct modules for better maintainability, testing, and scalability.

### 1. Data Extraction (extract_api_data.py)
**Source:** Cambridge Open Data API (Socrata).
**Method:** We use the `sodapy` library to interact with the Socrata API.
**Key Implementation Details:**
-   **Pagination:** Implemented a loop to fetch data in batches (limit=2000) to handle large datasets efficiently and avoid timeouts.
-   **Environment Variables:** Sensitive credentials (API tokens) are managed via a `.env` file to ensure security and configurability across environments.
-   **Resilience:** The extraction function is designed to handle potential API failures gracefully (though basic retry logic is currently commented out, it is structurally ready for enhancement).

### 2. Data Validation (validate.py)
**Goal:** Ensure high data quality before any transformation or loading occurs.
**Validation Checks:**
-   **Schema Validation:** Verifies that the fetched DataFrame contains all expected columns (`date_time`, `id`, `type`, etc.).
-   **Type Safety:** Ensures `id` fields are numeric, flagging non-numeric values for removal.
-   **Temporal Consistency:** Validates that `date_time` strings conform to ISO 8601 format.
-   **Completeness:** Checks for nulls in critical fields (`date_time`, `id`, `type`, `location`) to prevent incomplete records from entering the system.

### 3. Data Transformation (transform.py)
**Goal:** Prepare data for analytical queries and reporting.
**Transformations:**
-   **Deduplication:** Removes duplicate records based on the unique `id` field, keeping the first occurrence.
-   **Cleaning:** Filters out rows with invalid (non-numeric/NaN) IDs identified during validation.
-   **Enrichment:** Decomposes the single `date_time` field into `year`, `month`, `day`, `hour`, `minute`, and `second` columns. This granular breakdown facilitates time-based aggregation (e.g., analyzing crime trends by hour of day or seasonality).

### 4. Data Loading (load.py)
**Goal:** Persist data for long-term storage.
**Target:** PostgreSQL Database.
**Implementation:**
-   **Schema Management:** Automatically checks for and creates the target table `tb_cpd_incidents` in the `cpd_db` schema if it doesn't exist.
-   **Bulk Loading:** Utilizes `pandas.to_sql` with SQLAlchemy for efficient bulk insertion of records.
-   **Connection Management:** Uses `psycopg2` for robust database connection handling.

## Future Improvements
-   **Orchestration:** Integrate with a workflow orchestrator like Prefect (hinted at in the code) or Airflow for scheduled daily runs and automated failure handling.
-   **Incremental Loading:** Modify the extraction logic to fetch only new records since the `last_updated` timestamp of the most recent record in the database.
-   **Logging:** Replace `print` statements with a structured logging framework (e.g., standard `logging` library) for better observability in production.
-   **Containerization:** Dockerize the application to ensure consistent execution environments.
