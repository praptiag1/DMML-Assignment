import os
import sys
import shutil
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from exception import CustomException
from logger import logging

# Function to ingest data from a local CSV file
def ingest_local_csv(file_path, output_folder, timestamp):
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Save the raw data to the output folder
        output_file = os.path.join(output_folder, f"local_csv_{timestamp}.csv")
        df.to_csv(output_file, index=False)
        
        logging.info(f"Successfully ingested local CSV file: {output_file}")
        print(f"Local CSV data saved to: {output_file}")
        return output_file
    except Exception as e:
        logging.error(f"Failed to ingest local CSV file: {e}")
        raise CustomException(e, sys)

# Function to ingest data from Kaggle using the Kaggle API
def ingest_kaggle_dataset(dataset_name, output_folder, timestamp):
    try:
        # Check if Kaggle API key exists
        kaggle_config_path = os.path.expanduser("~/.kaggle/kaggle.json")
        if not os.path.exists(kaggle_config_path):
            raise CustomException("Kaggle API key not found! Please place kaggle.json in ~/.kaggle/ or C:\\Users\\YourUsername\\.kaggle\\", sys)

        # Set environment variable explicitly (optional)
        os.environ['KAGGLE_CONFIG_DIR'] = os.path.dirname(kaggle_config_path)
        # Initialize Kaggle API
        api = KaggleApi()
        api.authenticate()
        
        # Ensure output folder exists
        os.makedirs(output_folder, exist_ok=True)

        # Define a temp folder for dataset extraction
        temp_folder = os.path.join(output_folder, "temp_kaggle_download")
        os.makedirs(temp_folder, exist_ok=True)

        # Download the dataset (unzipped)
        api.dataset_download_files(dataset_name, path=temp_folder, unzip=True)

        # Find the actual file inside the temp folder
        downloaded_files = os.listdir(temp_folder)
        if not downloaded_files:
            raise CustomException("No files were downloaded from Kaggle!", sys)

        # Get the first file found (assuming one main dataset file)
        downloaded_file_path = os.path.join(temp_folder, downloaded_files[0])

        # Set custom filename with timestamp
        final_file_path = os.path.join(output_folder, f"kaggle_csv_{timestamp}.csv")

        # Rename the downloaded file to the custom filename
        shutil.move(downloaded_file_path, final_file_path)

        # Clean up temp folder
        os.rmdir(temp_folder)

        logging.info(f"Successfully ingested Kaggle dataset: {dataset_name}")
        print(f"Kaggle dataset saved to: {output_folder}")
        return final_file_path
    except Exception as e:
        logging.error(f"Failed to ingest Kaggle dataset: {e}")
        raise CustomException(e, sys)

# Main function to run the ingestion process and store raw data in local folder
def run_data_ingestion(timestamp):

    # Define output folder for raw data
    raw_data_folder = ".././data/raw"

    local_file_output_folder = os.path.join(raw_data_folder,"local dataset")
    os.makedirs(local_file_output_folder, exist_ok=True)
    kaggle_file_output_folder = os.path.join(raw_data_folder,"kaggle dataset")
    os.makedirs(kaggle_file_output_folder, exist_ok=True)
    
    # Ingest local CSV file
    local_csv_path = ".././data/Telco-customer-churn.csv"  # Replace with your local CSV file path
    local_csv_file_path = ingest_local_csv(local_csv_path, local_file_output_folder, timestamp)
    
    # Ingest Kaggle dataset 
    kaggle_dataset = "praptiag/telco-churn-dataset"  # Replace with your Kaggle dataset name
    kaggle_csv_file_path = ingest_kaggle_dataset(kaggle_dataset, kaggle_file_output_folder, timestamp)
    try:
        # Combined both raw dataframes
        local_df = pd.read_csv(local_csv_file_path)
        kaggle_df = pd.read_csv(kaggle_csv_file_path)

        combined_dataset = pd.concat([local_df, kaggle_df], ignore_index=True)
        # Save combined dataset with timestamp
        combined_file_path = os.path.join(raw_data_folder, f"customer_churn_{timestamp}.csv")
        combined_dataset.to_csv(combined_file_path, index=False)
        
        logging.info(f"Customer Churn dataset saved to: {combined_file_path}")
        print(f"Customer Churn raw dataset saved to: {combined_file_path}")

        # # Remove the individual local and Kaggle dataset files
        # os.remove(local_csv_file_path)
        # os.remove(kaggle_csv_file_path)
        
        # logging.info("Removed individual local and Kaggle dataset files.")
        # print("Removed individual local and Kaggle dataset files.")
    except Exception as e:
        logging.error(f"Failed to save raw dataset: {e}")
        raise CustomException(e, sys)
