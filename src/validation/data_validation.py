import os
import sys
import pandas as pd
from datetime import datetime
from exception import CustomException
from logger import logging

# Function to validate a dataset
def validate_dataset(dataset_path, report_folder, dataset_name):
    try:
        # Read the dataset
        df = pd.read_csv(dataset_path)
        
        # Initialize validation results
        validation_results = {
            "Dataset": dataset_name,
            "Missing Values": {},
            "Data Types": {},
            "Duplicates": False,
            "Anomalies": {}
        }
        
        # Check for missing values
        missing_values = df.isnull().sum()
        validation_results["Missing Values"] = missing_values.to_dict()
        
        # Validate data types
        data_types = df.dtypes
        validation_results["Data Types"] = data_types.to_dict()
        
        # Check for duplicates
        duplicates = df.duplicated().any()
        validation_results["Duplicates"] = duplicates
        
        # Check for anomalies (e.g., negative values in numeric columns)
        anomalies = {}
        for col in df.select_dtypes(include=['float64', 'int64']).columns:
            if (df[col] < 0).any():
                anomalies[col] = "Negative values found"
        validation_results["Anomalies"] = anomalies
        
        # Save validation results to a report
        report_file = os.path.join(report_folder, f"{dataset_name}_validation_report.csv")
        report_df = pd.DataFrame.from_dict(validation_results, orient='index')
        report_df.to_csv(report_file)
        
        logging.info(f"Validation completed for {dataset_name}. Report saved to {report_file}")
        print(f"Validation report saved to: {report_file}")
    except Exception as e:
        logging.error(f"Failed to validate dataset {dataset_name}: {e}")
        raise CustomException(e, sys)

# Main function to run validation
def run_data_validation(timestamp):
    # Define folders
    raw_data_folder = ".././data/raw"
    report_folder = "../data/validation_reports"
    os.makedirs(report_folder, exist_ok=True)
    
    # Validate the local CSV file with the given timestamp
    csv_file = os.path.join(raw_data_folder, f"customer_churn_{timestamp}.csv")
    if os.path.exists(csv_file):
        dataset_name = f"customer_churn_{timestamp}"
        report_file = os.path.join(report_folder, f"{dataset_name}_validation_report.csv")
        if not os.path.exists(report_file):  # Skip if already validated
            validate_dataset(csv_file, report_folder, dataset_name)
        else:
            logging.info(f"Skipping validation for {dataset_name} (already validated).")
            print(f"Skipping validation for {dataset_name} (already validated).")


