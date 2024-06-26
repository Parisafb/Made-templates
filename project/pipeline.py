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
import sqlite3
import requests
from io import BytesIO

class DataPipeline:
    def __init__(self, temp_url, health_url, db_name):
        self.temp_url = temp_url
        self.health_url = health_url
        self.db_name = db_name
        self.conn = sqlite3.connect(f'/project/data/{self.db_name}.sqlite')  # Ensure the path is correct

    def load_data(self):
        #load temp data
        temp_response = requests.get(self.temp_url)
        temp_response.raise_for_status()  # Raise an error for bad status codes
        self.temperature_data = pd.read_csv(BytesIO(temp_response.content))
         #load health data
        health_response = requests.get(self.health_url)
        health_response.raise_for_status()  # Raise an error for bad status codes
        self.health_data = pd.read_csv(BytesIO(health_response.content))

    def preprocess_data(self):
       #preprocess tempdata
        self.temperature_data['Year'] = self.temperature_data['Years']
        self.temperature_data_clean = self.temperature_data[(self.temperature_data['Year'] >= 2000) & (self.temperature_data['Year'] <= 2019)]
        
        # delete unnecessery 
        self.temperature_data_clean = self.temperature_data_clean[['Year', 'Country', 'Temperature', 'Anomaly']]
        
        # avg temp and anomaly
        self.temperature_data_clean = self.temperature_data_clean.groupby(['Year', 'Country']).agg(
            AverageAnnualTemperature=('Temperature', 'mean'),
            AverageAnnualAnomaly=('Anomaly', 'mean')
        ).reset_index()

        #preprocess helathdata
        self.health_data_clean = self.health_data[['Location', 'Period', 'Value']]
        self.health_data_clean.columns = ['Country', 'Year', 'MortalityRate']
        self.health_data_clean.dropna(inplace=True)

    def save_to_db(self):
        self.temperature_data_clean.to_sql('TemperatureData', self.conn, if_exists='replace', index=False)
        self.health_data_clean.to_sql('HealthData', self.conn, if_exists='replace', index=False)

    def run(self):
        self.load_data()
        self.preprocess_data()
        self.save_to_db()
        self.conn.close()

if __name__ == "__main__":
    temp_url = 'https://www.kaggle.com/datasets/josepferrersnchez/bearkley-earth-surface-temperature-data/download'  
    health_url = 'https://www.who.int/data/gho/data/indicators/indicator-details/GHO/probability-(-)-of-dying-between-age-30-and-exact-age-70-from-any-of-cardiovascular-disease-cancer-diabetes-or-chronic-respiratory-disease'
    pipeline = DataPipeline(temp_url, health_url, 'climate_health')
    pipeline.run()
