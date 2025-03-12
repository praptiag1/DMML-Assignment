import pandas as pd

# Load the full dataset
df = pd.read_csv("C:/Users/praptiagarwal/OneDrive - Nagarro/Desktop/BITS/DMML Assignment/Customer Churn Analysis/data/WA_Fn-UseC_-Telco-Customer-Churn.csv")

# Split the dataset (50% local, 50% to upload to Kaggle)
df_local = df.iloc[:len(df)//2]   # First half for local storage
df_kaggle = df.iloc[len(df)//2:]  # Second half for Kaggle storage

# Save both parts
df_local.to_csv("C:/Users/praptiagarwal/OneDrive - Nagarro/Desktop/BITS/DMML Assignment/Customer Churn Analysis/data/raw/local_data.csv", index=False)
df_kaggle.to_csv("C:/Users/praptiagarwal/OneDrive - Nagarro/Desktop/BITS/DMML Assignment/Customer Churn Analysis/data/raw/kaggle_data.csv", index=False)

print("Dataset split and saved successfully!")
