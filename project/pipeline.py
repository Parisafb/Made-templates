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


import os
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

class ExtractData:
    def __init__(self):
        self.kaggle_api = KaggleApi()
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(self.script_dir, 'data')
        self.download_dir = os.path.abspath(self.data_dir)
        os.makedirs(self.download_dir, exist_ok=True)
        os.environ['KAGGLE_CONFIG_DIR'] = os.path.join(self.script_dir, ".kaggle")
        self.kaggle_api.authenticate()

    def download_dataset(self, dataset_name):
        self.kaggle_api.dataset_download_files(dataset_name, path=self.download_dir, unzip=True)

    def load_and_clean_data(self, dataset_name):
        dataset_path = os.path.join(self.download_dir, dataset_name)
        dataset = pd.read_csv(dataset_path)
        numeric_mean = dataset.select_dtypes(include=[np.number]).mean()
        df_numeric_imputed = dataset.select_dtypes(include=[np.number]).fillna(numeric_mean)
        dataset_imputed = pd.concat([dataset.select_dtypes(exclude=[np.number]), df_numeric_imputed], axis=1)
        return dataset_imputed

    def save_data(self, database_name, dataset, table_name):
        engine = create_engine(f'sqlite:///{self.download_dir}/{database_name}.sqlite')
        dataset.to_sql(table_name, con=engine, if_exists='replace', index=False)

    def remove_unnecessary_files(self):
        for filename in os.listdir(self.download_dir):
            if not filename.endswith(".sqlite"):
                file_path = os.path.join(self.download_dir, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)

if __name__ == '__main__':
    extract = ExtractData()

    # دانلود و پردازش داده‌های CO2
    extract.download_dataset('https://www.kaggle.com/datasets/ulrikthygepedersen/co2-emissions-by-country/data')
    co2_dataset = extract.load_and_clean_data('co2_emissions_kt_by_country.csv')
    extract.save_data('ClimateChangeDB', co2_dataset, 'co2_emissions')

    # دانلود و پردازش داده‌های کیفیت زندگی
    extract.download_dataset('https://www.kaggle.com/datasets/thedevastator/impact-of-co2-on-quality-of-life-around-the-world')
    qol_dataset = extract.load_and_clean_data('QoL_cleaned.csv')
    extract.save_data('ClimateChangeDB', qol_dataset, 'quality_of_life')

    # حذف فایل‌های غیرضروری
    extract.remove_unnecessary_files()

    print("Data pipeline executed successfully.")
