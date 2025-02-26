import numpy as np
import pandas as pd
# for feature Scaling import StandarScaler
from sklearn.preprocessing import StandardScaler, OneHotEncoder # Estimator
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
import os
from sqlalchemy import create_engine  # Import sqlalchemy library for SQL Dataset


class Data_pipeline:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file
# Define the function for data extraction
    def extract_data(self, data_path):
        # Load data from various file types into a pandas DataFrame.

        # get file extension
        file_extension = os.path.splitext(data_path)[1].lower()
        if file_extension == '.csv':
            data = pd.read_csv(data_path)
            print(data)
            print(f"CSV Data loaded successfully from {data_path}")
        
        elif file_extension == '.xlsx' or file_extension == '.xls':
            data = pd.read_excel(data_path)
            print(f"Excel data loaded successfully from {data_path}.")
            
        elif file_extension == '.json':
            data = pd.read_json(data_path)
            print(f"JSON data loaded successfully from {data_path}.")
        else:
            raise ValueError(f"Unsupported file type: {file_extension}. Please provide a CSV, Excel, JSON, or SQL file.")
        return data

    def transform_data(self, df):
        # Define the categorical and numerical columns
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        numerical_cols = df.select_dtypes(include = ['int', 'float']).columns.tolist()

        # Numerical data preprocessing : impute missing Values and encode
        numerical_data = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='mean')),        # Fill missing value with mean
            ('Scaler', StandardScaler())])                     # Standardize numerical features

        # Categorical data preprocessing : impute missing Values and encode
        categorical_data = Pipeline(steps=[
            ('Imputer', SimpleImputer(strategy="most_frequent")),            # Fill missing value with most frequent values
            ('Encoder', OneHotEncoder(handle_unknown='ignore')) ])       # One-hot encoding for Categorical features
        
        # Combine the transformers into a Preprocessor using ColumnTransformer
        preprocessor = ColumnTransformer(transformers=[
            ('num', numerical_data, numerical_cols),
            ('cat', categorical_data, categorical_cols)
        ])

        # Apply Transformation to the Data
        df_transformed = preprocessor.fit_transform(df)

        # Convert transfored data into Dataframe again
        df_transformed = pd.DataFrame(df_transformed)
        print('Data Transformation Completed')
        return df_transformed

    def load_data(self, df, output_path):
        df.to_csv(output_path, index = False)
        print(f"Processed data saved to a csv file. ")
    
    def etl_pipeline(self):
    # Run ETL process - Call extract_Data
        data = self.extract_data(self.input_file)

        # Run ETL process - call transformed_data
        transformed_data = self.transform_data(data)

        # call load_data
        self.load_data(transformed_data, self.output_file)

    
pipeline = Data_pipeline("Position_Salaries.csv", "output_path.txt")
pipeline.etl_pipeline()





        

