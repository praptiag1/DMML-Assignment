import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from logger import logging
from exception import CustomException
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from datetime import datetime
import matplotlib
matplotlib.use('Agg')  # Use a non-interactive backend

# Function to perform EDA
def perform_eda(df, eda_folder, timestamp):
    try:
        # Generate summary statistics
        summary_stats = df.describe(include='all').transpose()
        summary_stats_file = os.path.join(eda_folder, f"summary_statistics_{timestamp}.csv")
        summary_stats.to_csv(summary_stats_file)
        logging.info(f"Summary statistics saved to: {summary_stats_file}")
        print(f"Summary statistics saved to: {summary_stats_file}")
        
        # Get numerical and categorical columns
        numerical_cols = ["tenure","MonthlyCharges","TotalCharges"]
        categorical_cols = df.select_dtypes(include=['object']).columns
        target_col = "Churn"
        
        # Generate subplots for histograms
        if len(numerical_cols) > 0:
            plt.figure(figsize=(15, 10))
            for i, col in enumerate(numerical_cols, 1):
                plt.subplot((len(numerical_cols) // 3) + 1, 3, i)  # Arrange subplots in a grid
                sns.histplot(df[col], kde=True)
                plt.title(f"Histogram of {col}")
            plt.tight_layout()
            histogram_file = os.path.join(eda_folder, f"histograms_{timestamp}.png")
            plt.savefig(histogram_file)
            plt.close()
            logging.info(f"Histograms saved to: {histogram_file}")
            print(f"Histograms saved to: {histogram_file}")
        
        # Generate subplots for box plots
        if len(numerical_cols) > 0:
            plt.figure(figsize=(15, 10))
            for i, col in enumerate(numerical_cols, 1):
                plt.subplot((len(numerical_cols) // 3) + 1, 3, i)  # Arrange subplots in a grid
                sns.boxplot(x=df[col])
                plt.title(f"Box Plot of {col}")
            plt.tight_layout()
            box_plot_file = os.path.join(eda_folder, f"box_plots_{timestamp}.png")
            plt.savefig(box_plot_file)
            plt.close()
            logging.info(f"Box plots saved to: {box_plot_file}")
            print(f"Box plots saved to: {box_plot_file}")
        
        # Generate subplots for bar plots
        if len(categorical_cols) > 0:
            plt.figure(figsize=(15, 10))
            for i, col in enumerate(categorical_cols, 1):
                plt.subplot((len(categorical_cols) // 3) + 1, 3, i)  # Arrange subplots in a grid
                sns.countplot(x=df[col])
                plt.title(f"Bar Plot of {col}")
            plt.tight_layout()
            bar_plot_file = os.path.join(eda_folder, f"bar_plots_{timestamp}.png")
            plt.savefig(bar_plot_file)
            plt.close()
            logging.info(f"Bar plots saved to: {bar_plot_file}")
            print(f"Bar plots saved to: {bar_plot_file}")

        # Generate a bar plot for the target column
        if target_col in df.columns:
            plt.figure(figsize=(8, 6))
            sns.countplot(x=df[target_col])
            plt.title(f"Distribution of {target_col}")
            target_bar_plot_file = os.path.join(eda_folder, f"target_bar_plot_{timestamp}.png")
            plt.savefig(target_bar_plot_file)
            plt.close()
            logging.info(f"Target bar plot saved to: {target_bar_plot_file}")
            print(f"Target bar plot saved to: {target_bar_plot_file}")
        else:
            logging.warning(f"Target column '{target_col}' not found in the dataset.")
            print(f"Target column '{target_col}' not found in the dataset.")

        # Generate subplots for bar plots (if categorical columns exist)
        if len(categorical_cols) > 0:
            num_rows = (len(categorical_cols) // 3) + 1
            fig, axes = plt.subplots(num_rows, 3, figsize=(20, 6 * num_rows))
            axes = axes.flatten()  # Flatten the axes array for easy iteration
            for i, col in enumerate(categorical_cols):
                sns.countplot(x=df[col], ax=axes[i])
                axes[i].set_title(f"Bar Plot of {col}")
            for j in range(i + 1, len(axes)):  # Hide unused subplots
                fig.delaxes(axes[j])
            plt.tight_layout()
            bar_plot_file = os.path.join(eda_folder, f"bar_plots_{timestamp}.png")
            plt.savefig(bar_plot_file)
            plt.close()
            logging.info(f"Bar plots saved to: {bar_plot_file}")
            print(f"Bar plots saved to: {bar_plot_file}")
        else:
            logging.info("No categorical columns found for bar plots.")
            print("No categorical columns found for bar plots.")


    except Exception as e:
        logging.error(f"Failed to perform EDA: {e}")
        raise CustomException(e, sys)
# Function to clean and preprocess the dataset
def prepare_data(raw_data_folder, clean_data_folder, eda_folder, timestamp):
    try:
        # Load the latest local CSV file
        combined_data = pd.read_csv(raw_data_folder)
        
        # Drop CustomerID column
        combined_data = combined_data.drop("customerID", axis=1)
        
        # Handle datatype for TotalCharges
        combined_data['TotalCharges'] = pd.to_numeric(combined_data['TotalCharges'], errors='coerce')
        combined_data['TotalCharges'] = combined_data['TotalCharges'].replace([np.inf, -np.inf], np.nan)  
        combined_data = combined_data.dropna(subset=['TotalCharges'])  

        # Handle missing values
        # combined_data.fillna(combined_data.mean(), inplace=True)  # Impute numerical columns with mean

        # Bin labels
        bins=[0,30,70,combined_data['MonthlyCharges'].max()]
        labels=['Low','Mid','High']
        combined_data['MonthlyCharges_Category']=pd.cut(combined_data['MonthlyCharges'],bins=bins,labels=labels)
        combined_data['MonthlyCharges_Category']=combined_data['MonthlyCharges_Category'].replace({'Low':0,'Mid':1,'High':2})
        
        #Label encoding of target column
        combined_data["Churn"] = combined_data["Churn"].replace({"Yes": 1, "No": 0})
        
        # Perform EDA
        perform_eda(combined_data, eda_folder, timestamp)
        # Standardize numerical attributes
        scaler = StandardScaler()
        combined_data[["tenure","MonthlyCharges","TotalCharges"]] = scaler.fit_transform(combined_data[["tenure","MonthlyCharges","TotalCharges"]])
        
        # Encode categorical variables
        encoder = OneHotEncoder(drop='first', sparse_output=False)  # Use sparse_output=False for dense array
        categorical_cols = combined_data.select_dtypes(include=['object']).columns
        
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

# Main function to run data preparation
def run_data_preparation(timestamp):
    # Define folders
    raw_data_folder = ".././data/raw"
    clean_data_folder = ".././data/processed"
    os.makedirs(clean_data_folder, exist_ok=True)
    eda_data_folder = f".././data/EDA_results/EDA_{timestamp}"
    os.makedirs(eda_data_folder, exist_ok=True)
    csv_file = os.path.join(raw_data_folder, f"customer_churn_{timestamp}.csv")
    if os.path.exists(csv_file):
        # Run data preparation
        prepare_data(csv_file, clean_data_folder, eda_data_folder, timestamp)

# if __name__ == "__main__":
#     # For standalone execution (optional)
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     run_data_preparation(timestamp)