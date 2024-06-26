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
import pandas as pd
import requests
from zipfile import ZipFile
from sqlalchemy import create_engine

class Pipeline:
    def __init__(self, url1, url2, save_file_name):
        self.url1 = url1
        self.url2 = url2
        self.data1 = None
        self.data2 = None
        path = os.path.join('data', save_file_name + '.sqlite')
        self.engine = create_engine(f'sqlite:///{path}', echo=False)
        self.files_to_delete = []

    def get_data(self):
        self.data1, items_to_delete1 = self.get_data_helper(self.url1, 0, "co2_emissions")
        self.data2, items_to_delete2 = self.get_data_helper(self.url2, 0, "quality_of_life")
        self.files_to_delete.extend(items_to_delete1)
        self.files_to_delete.extend(items_to_delete2)

    def get_data_helper(self, url, idx, filename):
        """Helper function to get the data, as we may use it get data from several urls"""
        print("Downloading", url)
        response = requests.get(url)

        if response.status_code == 200:
            filename = filename + ".zip"

            # Write the downloaded content to the file
            with open(filename, 'wb') as f:
                f.write(response.content)

            # Extract the CSV file from the zip
            with ZipFile(filename, 'r') as zip_ref:
                csv_filename = zip_ref.namelist()[idx]  # Get the csv file name
                zip_ref.extract(csv_filename)  # Extract the file

            # Load the extracted CSV file into a pandas DataFrame
            df = pd.read_csv(csv_filename, encoding='unicode_escape')

            return df, [filename, csv_filename]
        else:
            print(f"Download failed for {url}. Status code: {response.status_code}")

    def transform_data(self):
        #CO2
        if self.data1 is not None:
            self.data1.dropna(inplace=True)  #delete missing
            self.data1 = self.data1.rename(columns={'Year': 'year', 'CO2_emissions': 'co2_emissions'})

        # Qol
        if self.data2 is not None:
            self.data2.dropna(inplace=True)  # delete missing
            columns_to_keep = ['Country', 'Stability(15%)', 'Rights(20%)', 'Health(15%)', 
                                'Safety(10%)', 'Climate(15%)', 'Costs(15%)', 'Popularity(10%)', 
                                'TotalQuality of life(100%)']
            self.data2 = self.data2[columns_to_keep]

    def save_data(self):
        if self.data1 is not None:
            self.data1.to_sql("CO2_Emissions", self.engine, if_exists='replace', index=False)
        if self.data2 is not None:
            self.data2.to_sql("Quality_of_Life", self.engine, if_exists='replace', index=False)

        for pa in self.files_to_delete:  # Removing downloaded and extracted data
            os.remove(pa)

        self.engine.dispose()

    def run_pipeline(self):
        self.get_data()
        print("Got the Datasets!")
        self.transform_data()
        print("Datasets Transformed!")
        self.save_data()
        print("Datasets Saved!")

if __name__ == '__main__':
    pipe = Pipeline("https://www.kaggle.com/datasets/ulrikthygepedersen/co2-emissions-by-country/download",
                    "https://www.kaggle.com/datasets/thedevastator/impact-of-co2-on-quality-of-life-around-the-world/download",
                    "ClimateChangeDB")
    pipe.run_pipeline()
