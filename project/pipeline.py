# Follow your project plan to build an automated data pipeline for your project
#     Write a script (for example in Python or Jayvee) that pulls the data sets you chose from the internet, transforms it and fixes errors, and finally stores your data in the /data directory
#         Place the script in the /project directory (any file name is fine)
#         Add a /project/pipeline.sh that starts your pipeline as you would do from the command line as entry point:
#             E.g. if you run your script on your command line using `python3 /project/pipeline.py`, create a /project/pipeline.sh with the content:
#                     #!/bin/bash
#                     python3 /project/pipeline.py
#     The output of the script should be: datasets in your /data directory (e.g., as SQLite databases)
#         Do NOT check in your data sets, just your script
#         You can use .gitignore to avoid checking in files on git
#         This data set will be the base for your data report in future project work
# Update the issues and project plan if necessary


import pandas as pd
import os
from kaggle.api.kaggle_api_extended import KaggleApi
from zipfile import ZipFile
from sqlalchemy import create_engine

class KaggleETLPipeline:
    def __init__(self, kaggle_dataset1, kaggle_dataset2, db_name):
        self.kaggle_dataset1 = kaggle_dataset1
        self.kaggle_dataset2 = kaggle_dataset2
        self.db_name = db_name
        self.engine = create_engine(f'sqlite:///{db_name}.sqlite', echo=False)
        self.api = KaggleApi()
        self.api.authenticate()
        self.files_to_delete = []

    def extract_data(self):
        # Download and extract the first Kaggle dataset
        self.api.dataset_download_files(self.kaggle_dataset1, path='.', unzip=True)
        dataset1_files = [file for file in os.listdir('.') if file.endswith('.csv')]
        df1 = pd.read_csv(dataset1_files[0])

        # Download and extract the second Kaggle dataset
        self.api.dataset_download_files(self.kaggle_dataset2, path='.', unzip=True)
        dataset2_files = [file for file in os.listdir('.') if file.endswith('.csv')]
        df2 = pd.read_csv(dataset2_files[0])

        # Mark files for deletion later
        self.files_to_delete.extend(dataset1_files + dataset2_files)

        print("Data extraction complete.")
        return df1, df2

    def transform_data(self, df1, df2):
        # Example transformations:
        # Remove unnecessary columns
        df1 = df1.drop(columns=['Unnamed: 0'], errors='ignore')
        df2 = df2.drop(columns=['Unnamed: 0'], errors='ignore')

        # Fill missing values with backward fill
        df1 = df1.bfill()
        df2 = df2.bfill()

        print("Data transformation complete.")
        return df1, df2

    def load_data(self, df1, df2):
        df1.to_sql("dataset1", self.engine, if_exists='replace', index=False)
        df2.to_sql("dataset2", self.engine, if_exists='replace', index=False)
        print(f"Data loaded into database '{self.db_name}.sqlite'.")

        # Clean up extracted files
        for file in self.files_to_delete:
            os.remove(file)

        self.engine.dispose()
        print("Temporary files deleted and database connection closed.")

    def run_pipeline(self):
        df1, df2 = self.extract_data()
        transformed_df1, transformed_df2 = self.transform_data(df1, df2)
        self.load_data(transformed_df1, transformed_df2)
        print("ETL pipeline completed successfully.")

if __name__ == '__main__':
    pipeline = KaggleETLPipeline(
        kaggle_dataset1='thedevastator/impact-of-co2-on-quality-of-life-around-the-world',
        kaggle_dataset2='ulrikthygepedersen/co2-emissions-by-country',
        db_name='kaggle_data'
    )
    pipeline.run_pipeline()
