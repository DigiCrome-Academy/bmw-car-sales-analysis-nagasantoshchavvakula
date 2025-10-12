# etl_flow.py
# This script defines the Extract, Transform, and Load (ETL) pipeline using Prefect.

import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine, text
import logging
import os

# Set up logging for visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. Configuration (Mock or Environment Variables) ---
# NOTE: In a real project, these values would be managed securely 
# via Prefect Secrets or environment variables.
DB_URL = os.getenv("MYSQL_URL", "mysql+mysqlconnector://user:password@host:3306/db_name")
CSV_FILE_PATH = "car_sales_data.csv" # Assuming the file exists in the root directory
TABLE_NAME = "car_sales"

# Example Schema for MySQL (Based on CSV head)
# NOTE: Column names must be sanitized for SQL compatibility (e.g., 'Engine_Size_L' -> 'engine_size_l')
# This is where the student must ensure their schema.sql matches the transformed dataframe columns.
EXPECTED_COLUMNS = [
    'model', 'year', 'region', 'color', 'fuel_type', 'transmission', 
    'engine_size_l', 'mileage_km', 'price_usd', 'sales_volume', 'sales_classification'
]


# --- 2. Prefect Tasks (The individual steps) ---

@task(name="Extract Data from CSV")
def extract_data(file_path: str) -> pd.DataFrame:
    """Reads the raw CSV data into a Pandas DataFrame."""
    logging.info(f"Starting extraction from: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Data extracted successfully. Shape: {df.shape}")
        return df
    except FileNotFoundError:
        logging.error(f"File not found at {file_path}. Using mock data for testing.")
        # --- MOCK DATA FOR TESTING WHEN FILE IS MISSING ---
        data = {
            'Model': ['X3', '5 Series'], 'Year': [2022, 2024], 'Region': ['Europe', 'Asia'], 
            'Price_USD': [110000, 95000], 'Sales_Classification': ['High', 'Low'],
            'Engine_Size_L': [2.0, 3.0], 'Mileage_KM': [50000, 10000]
        }
        df_mock = pd.DataFrame(data)
        return df_mock
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

@task(name="Transform and Clean Data")
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Performs data cleaning, sanitation, and type conversion."""
    logging.info("Starting data transformation.")
    
    # 1. Sanitize Column Names (e.g., lowercase, replace spaces/special characters)
    df.columns = df.columns.str.lower().str.replace('[^a-zA-Z0-9_]', '_', regex=True)
    
    # 2. Rename Columns to match a clean SQL schema
    # The original columns like 'Engine_Size_L' and 'Mileage_KM' are now cleaner
    df.rename(columns={
        'engine_size_l': 'engine_size_l', # Already sanitized
        'mileage_km': 'mileage_km',     # Already sanitized
        'price_usd': 'price_usd',
        'sales_volume': 'sales_volume'
    }, inplace=True)

    # 3. Type Conversion and Cleaning (Example: Convert columns to numeric, handle errors)
    for col in ['engine_size_l', 'mileage_km', 'price_usd', 'sales_volume']:
        if col in df.columns:
            # Coerce non-numeric values to NaN, then fill NaN (e.g., with 0 or mean)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    # 4. Filter/Validate: Ensure the transformed DF contains the expected columns
    final_df = df[EXPECTED_COLUMNS]
    
    logging.info(f"Transformation complete. Cleaned data shape: {final_df.shape}")
    return final_df

@task(name="Load Data to MySQL")
def load_data(df: pd.DataFrame, db_url: str, table_name: str):
    """Loads the clean DataFrame into the specified MySQL table."""
    logging.info(f"Connecting to MySQL database at: {db_url.split('@')[-1]}")
    try:
        engine = create_engine(db_url)
        
        # Test connection
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            if result.scalar() == 1:
                logging.info("Successfully connected to MySQL.")
            else:
                raise ConnectionError("Failed connection test.")

        # Load data: 'replace' drops and recreates the table, 'append' adds to existing table
        # 'if_exists='replace'' is good for initial runs/clean testing.
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Successfully loaded {len(df)} rows into table: '{table_name}'.")

    except Exception as e:
        logging.error(f"Error during database load or connection: {e}")
        # In a production environment, this would notify the Prefect flow manager of failure.
        raise


# --- 3. Prefect Flow (The Orchestration Logic) ---

@flow(name="Sales ETL Pipeline", description="Orchestrates data extraction, cleaning, and loading into MySQL for BI analysis.")
def sales_etl_flow(csv_path: str = CSV_FILE_PATH):
    """
    Main flow for the sales data ETL process.
    """
    # 1. Extract
    raw_data = extract_data(file_path=csv_path)
    
    # Check if extraction was successful (raw_data is a DataFrame)
    if raw_data is not None:
        # 2. Transform
        cleaned_data = transform_data(df=raw_data)
        
        # 3. Load
        # The load task is dependent on the cleaned_data from the transform task
        load_data(df=cleaned_data, db_url=DB_URL, table_name=TABLE_NAME)
    else:
        logging.critical("Flow aborted due to failure in the extraction task.")


# --- 4. Execution ---

if __name__ == "__main__":
    # When this script is executed directly, the flow runs.
    # In a real environment, this flow would be deployed to a Prefect server.
    
    # IMPORTANT: The flow must be called to be registered/run.
    print(f"Starting Prefect flow '{sales_etl_flow.name}'...")
    sales_etl_flow()
    print("Prefect flow definition complete. Check Prefect UI for execution status.")
