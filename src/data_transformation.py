import os
import sys
import pandas as pd
import sqlite3
from datetime import datetime
from logger import logging
from exception import CustomException

# Function to perform feature engineering
def transform_data(df):
    try:
        # Example: Create a new feature for customer tenure in months
        df['tenure_months'] = df['tenure']  # Assuming 'tenure' is already in months
        
        # Example: Create a new feature for average monthly charges
        df['avg_monthly_charges'] = df['TotalCharges'] / df['tenure_months']
        
        # Example: Create a binary feature for high monthly charges
        df['high_monthly_charges'] = (df['MonthlyCharges'] > df['MonthlyCharges'].median()).astype(int)
        
        logging.info("Feature engineering completed.")
        return df
    except Exception as e:
        logging.error(f"Failed to transform data: {e}")
        raise CustomException(e, sys)

# Function to store transformed data in SQLite
def store_transformed_data(df, db_path, table_name):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        
        # Store the transformed data in a table
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        logging.info(f"Transformed data saved to SQLite table: {table_name}")
        print(f"Transformed data saved to SQLite table: {table_name}")
        
        # Close the database connection
        conn.close()
    except Exception as e:
        logging.error(f"Failed to store transformed data: {e}")
        raise CustomException(e, sys)

# Main function to run data transformation
def run_data_transformation(df, timestamp):
    try:
        # Define folders
        db_folder = ".././data/database"
        os.makedirs(db_folder, exist_ok=True)
        
        # Perform feature engineering
        transformed_df = transform_data(df)
        
        # Store the transformed data in SQLite
        db_path = os.path.join(db_folder, "customer_churn.db")
        table_name = "transformed_data"
        store_transformed_data(transformed_df, db_path, table_name)
    except Exception as e:
        logging.error(f"Failed to run data transformation: {e}")
        raise CustomException(e, sys)