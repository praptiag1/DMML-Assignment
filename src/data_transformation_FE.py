import os
import sys
import pandas as pd
import numpy as np
from logger import logging
from exception import CustomException
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from datetime import datetime
import sqlite3
from data_preparation import perform_eda

# Function to clean and transform the dataset
def prepare_data(raw_data_folder, clean_data_folder, eda_folder, timestamp):
    try:
        # Load the latest local CSV file
        combined_data = pd.read_csv(raw_data_folder)
        num_cols = ["tenure","MonthlyCharges","TotalCharges"]
        categorical_cols = combined_data.select_dtypes(include=['object']).columns
        
        # Drop CustomerID column
        combined_data = combined_data.drop("customerID", axis=1)
        
        # Handle datatype for TotalCharges
        combined_data['TotalCharges'] = pd.to_numeric(combined_data['TotalCharges'], errors='coerce')
        combined_data['TotalCharges'] = combined_data['TotalCharges'].replace([np.inf, -np.inf], np.nan)  
        combined_data = combined_data.dropna(subset=['TotalCharges'])  
        
        # Handle missing values
        combined_data.dropna(inplace=True)

        #Label encoding of target column
        combined_data["Churn"] = combined_data["Churn"].replace({"Yes": 1, "No": 0})
        
        # Perform EDA
        perform_eda(combined_data, eda_folder, timestamp, num_cols, categorical_cols)

        # Handle outliers
        for cols in num_cols:
            if check_outlier(combined_data, cols):
                replace_with_thresholds(combined_data, cols)

        # Standardize numerical attributes
        scaler = StandardScaler()
        combined_data[num_cols] = scaler.fit_transform(combined_data[num_cols])
        
        # Encode categorical variables
        encoder = OneHotEncoder(drop='first', sparse_output=False)  # Use sparse_output=False for dense array
    
        # Encode categorical columns
        if len(categorical_cols) > 0:
            encoded_cols = encoder.fit_transform(combined_data[categorical_cols])
            encoded_df = pd.DataFrame(encoded_cols, columns=encoder.get_feature_names_out(categorical_cols))
            
            # Combine numerical and encoded categorical data
            combined_data = pd.concat([combined_data.drop(categorical_cols, axis=1), encoded_df], axis=1)
        else:
            logging.info("No categorical columns to encode.")
        
        # Save the cleaned dataset
        clean_data_file = os.path.join(clean_data_folder, f"clean_dataset_{timestamp}.csv")
        combined_data.to_csv(clean_data_file, index=False)

        logging.info(f"Data preparation completed. Clean dataset saved to: {clean_data_file}")
        print(f"Clean dataset saved to: {clean_data_file}")
        return combined_data
    except Exception as e:
        logging.error(f"Failed to prepare data: {e}")
        raise CustomException(e, sys)

#Define a Function about outlier threshold for data columns
def outlier_th(dataframe, col_name, q1=0.05, q3=0.95):
    quartile1 = dataframe[col_name].quantile(q1)
    quartile3 = dataframe[col_name].quantile(q3)
    interquantile_range = quartile3 - quartile1
    up_limit = quartile3 + 1.5 * interquantile_range
    low_limit = quartile1 - 1.5 * interquantile_range
    return low_limit, up_limit

#Define a Function about checking outlier for data columns
def check_outlier(dataframe, col_name):
    low_limit, up_limit = outlier_th(dataframe, col_name)
    if dataframe[(dataframe[col_name] > up_limit) | (dataframe[col_name] < low_limit)].any(axis=None):
        return True
    else:
        return False

#Define a Function about replace with threshold for data columns
def replace_with_thresholds(dataframe, variable):
    low_limit, up_limit = outlier_th(dataframe, variable)
    dataframe.loc[(dataframe[variable] < low_limit), variable] = low_limit
    dataframe.loc[(dataframe[variable] > up_limit), variable] = up_limit

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

# Main function to run data preparation
def run_data_transformation(timestamp):
    try:
        # Define folders
        raw_data_folder = ".././data/raw"
        clean_data_folder = ".././data/transformed"
        os.makedirs(clean_data_folder, exist_ok=True)
        eda_data_folder = f".././data/EDA_results/EDA_{timestamp}"
        os.makedirs(eda_data_folder, exist_ok=True)

        # Folder to store in db
        db_folder = ".././data/database"
        os.makedirs(db_folder, exist_ok=True)

        csv_file = os.path.join(raw_data_folder, f"customer_churn_{timestamp}.csv")
        if os.path.exists(csv_file):
            # Run data cleaning and transformation
            transformed_df = prepare_data(csv_file, clean_data_folder, eda_data_folder, timestamp)

            # Store the transformed data in SQLite
            db_path = os.path.join(db_folder, f"customer_churn_{timestamp}.db")
            table_name = "transformed_data"
            store_transformed_data(transformed_df, db_path, table_name)
    except Exception as e:
        logging.error(f"Failed to run data transformation: {e}")
        raise CustomException(e, sys)

