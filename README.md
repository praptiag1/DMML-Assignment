# Problem Formulation

## Business Problem
Customer churn is a significant issue for businesses, leading to revenue loss. Acquiring new customers is expensive, so retaining existing customers is more cost-effective. The goal of this project is to build a machine learning pipeline to predict customer churn using data from multiple sources.

## Key Business Objectives
1. Build an automated pipeline to process customer data from multiple sources.
2. Predict customer churn using machine learning.
3. Evaluate the model’s performance using metrics like accuracy, precision, recall, and F1 score.

## Key Data Sources
The dataset consists of two data sources:
1. **Local Dataset**: A CSV file with customer records.
2. **Kaggle API Dataset**: Additional data fetched via the Kaggle API.

## Pipeline Outputs
1. Clean datasets for exploratory data analysis (EDA).
    - Handle missing values, duplicates, and data inconsistencies.
    - Standardize data formats and ensure correct types.
2. Transformed features for machine learning (e.g., customer tenure, total spend, activity frequency).
    - Encode categorical variables (One-Hot Encoding, Label Encoding).
    - Normalize numerical features (MinMaxScaler).
    - Create new engineered features (e.g., customer tenure groups).
3. A deployable machine learning model to predict customer churn.
    - Train multiple models (Logistic Regression, Random Forest, XGBoost).
    - Select the best-performing model for deployment.
    - Save the model using MLflow for versioning.

## Evaluation Metrics
- **Accuracy**: Percentage of correctly predicted churn/non-churn cases.
- **Precision**: Percentage of correctly predicted churn cases out of all predicted churn cases.
- **Recall**: Percentage of correctly predicted churn cases out of all actual churn cases.
- **F1 Score**: A balance between precision and recall.