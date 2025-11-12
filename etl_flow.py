"""
========================================
ETL Pipeline: BMW sales data (2010-2024)
========================================

Description:
----------------
This Python script defines a Prefect-based ETL (Extract, Transform, Load)
pipeline for processing BMW car sales data stored in a CSV file.
The flow reads raw data, cleans/transforms it, and loads it into a MySQL database.

Components:
----------------
- Extract: Reads BMW sales data from a CSV file.
- Transform: Cleans and processes the data (e.g., handles missing values, converts data types).
- Load: Inserts the cleaned data into a MySQL database.

Technologies Used:
--------------------
- Python: The programming language used for scripting the ETL process.
- Prefect: For orchestrating the ETL workflow.
- Pandas: For data manipulation and cleaning.
- SQLAlchemy+MySQL Connector: For database connectivity and operations.
- MySQL: The target database for storing the processed data.

Execution:
--------------------
To run the ETL pipeline, execute the script using Python. Ensure that all dependencies are installed and the MySQL database is accessible.

Input:
--------------------
- CSV file containing BMW sales data from 2010 to 2024.

Output:
--------------------
- Cleaned and transformed data loaded into a MySQL database table.

"""
# -----------------------------------------------
# Import necessary libraries
# -----------------------------------------------
import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine
import os
from  dotenv import load_dotenv
from urllib.parse import quote_plus

# -----------------------------------------------------
# Load environment variables for database connection
# -----------------------------------------------------
load_dotenv()

# Database connection parameters from environment variables
DB_HOST = os.getenv("MYSQL_HOST")
DB_PORT = os.getenv("MYSQL_PORT")
DB_USER = os.getenv("MYSQL_USER")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD")
DB_NAME = os.getenv("MYSQL_DATABASE")
DB_TABLE = os.getenv("MYSQL_TABLE")
CSV_FILE_PATH = os.getenv("DATASET_PATH")

# -----------------------------------------------
# Prefect Tasks
# -----------------------------------------------
@task
def extract_data(file_path: str) -> pd.DataFrame:
    """
    Extract task:
    ---------------
    Reads BMW sales data from a CSV file.
    Args:
        file_path (str): Path to the CSV file.
        Returns:
        pd.DataFrame: Extracted data as a pandas DataFrame.  
    """
    df = pd.read_csv(file_path)
    print("Data extracted successfully.")
    print(f"Number of rows extracted: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    return df

@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Task:
    ---------------
    Cleans and prepares data to match the database schema.
    - Renames columns to lowercase, underscore-separated format.
    - Converts numeric columns to proper numeric types.
    - Handles invalid or missing values.
    Args:
        df (pd.DataFrame): Raw data DataFrame.
        Returns:
        pd.DataFrame: Cleaned and transformed DataFrame.
    """
    # clean column names
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("[^a-z0-9_]", "", regex=True)
    )
    # Dynamically detecting numeric columns
    numeric_columns=[]
    for col in df.columns:
        try:
            # Try converting column to numeric temporarily
            numeric_series = pd.to_numeric(df[col], errors="coerce")
            numeric_ratio = numeric_series.notna().mean()  # % of numeric values
            if numeric_ratio > 0.8:  # If >80% values can be numeric
                numeric_columns.append(col)
        except Exception:
            continue
    print(f"Detected numeric columns: {numeric_columns}")
    # Convert detected numeric columns
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    print("Data transformed successfully.")
    return df

@task
def load_data(df: pd.DataFrame):
    """
    Load Task:
    ---------------
    Loads the cleaned data into a MySQL database.
    - Creates a database connection using SQLAlchemy.
    - Inserts data into the specified table.
    - Handles table creation if it does not exist.
    Args:
        df (pd.DataFrame): Cleaned data DataFrame.

    """
    # establishing database connection
    encoded_password = quote_plus(DB_PASSWORD)
    connection_route = f"mysql+mysqlconnector://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_route)
    print("Database connection established.")

    # Load data into the database
    try:
        df.to_sql(name=DB_TABLE, con=engine, if_exists="replace", index=False)
        print(f"Data loaded successfully into table '{DB_TABLE}'.")
    except Exception as e:
        print(f"Error loading data into database: {e}")
    finally:
        engine.dispose()
        print("Database connection closed.")
# -----------------------------------------------
# Prefect Flow
# -----------------------------------------------
@flow
def sales_etl_flow():
    """
    ETL Flow:
    ---------------
    Orchestrates the ETL process:
    - Extracts data from CSV.
    - Transforms the data.
    - Loads the data into MySQL database.
    """
    # Extract
    raw_data = extract_data(CSV_FILE_PATH)
    
    # Transform
    cleaned_data = transform_data(raw_data)
    
    # Load
    load_data(cleaned_data)
    print("ETL flow completed successfully.")

# -----------------------------------------------
# Main Execution (Entry Point)
# -----------------------------------------------
"""
This block ensures that the ETL flow runs when the script is executed directly.
"""
if __name__ == "__main__":
    sales_etl_flow()
    






    





    
