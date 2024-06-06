import os
import shutil
import subprocess
import zipfile
import pandas as pd
import sqlite3

# Set Kaggle 
os.environ['KAGGLE_CONFIG_DIR'] = os.path.expanduser('~/.kaggle')
os.environ['KAGGLE_DATASETS_CACHE'] = os.path.expanduser('~/.kaggle/cache')

#  directory exists
os.makedirs(os.environ['KAGGLE_DATASETS_CACHE'], exist_ok=True)

# Kaggle datasets to download
datasets = [
    ('arnabchaki/renewable-energy-production-by-country', 'renewable-energy.zip', 'renewable-energy'),
    ('kaggle dataset identifier for CO2 emissions', 'co2-emissions.zip', 'co2-emissions')  # Replace with actual identifier
]

# Download and extract 
for dataset, zip_file, folder in datasets:
    subprocess.run(f'kaggle datasets download -d {dataset}', shell=True, check=True)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(folder)
    os.remove(zip_file)

#  read CSV 
def read_csv_with_encodings(file_path, encodings=['latin1', 'ISO-8859-1', 'cp1252']):
    for enc in encodings:
        try:
            return pd.read_csv(file_path, encoding=enc)
        except Exception as e:
            print(f"Failed to read {file_path} with encoding {enc}: {e}")
    return None

# Load and preprocess renewable energy data
renewable_df = read_csv_with_encodings('renewable-energy/renewable_energy_production.csv')
if renewable_df is None:
    print("Failed to read renewable energy dataset with all attempted encodings.")
    exit(1)

# Preprocess renewable energy data
renewable_df = renewable_df[['Country', 'Year', 'Renewable_Energy_Production']]
renewable_df = renewable_df[renewable_df['Year'] >= 2010]  # Filter data for the past decade

# Load and preprocess CO2 emissions data
co2_df = read_csv_with_encodings('co2-emissions/CO2_Emissions.csv')  # Adjust path as needed
if co2_df is None:
    print("Failed to read CO2 emissions dataset with all attempted encodings.")
    exit(1)

# Preprocess CO2 emissions data
co2_df = co2_df[['Country', 'Year', 'CO2_Emissions']]
co2_df = co2_df[co2_df['Year'] >= 2010]  # Filter data for the past decade

# Merge datasets
merged_df = pd.merge(renewable_df, co2_df, on=['Country', 'Year'])

# Save merged data to SQLite database
os.makedirs('data', exist_ok=True)
conn = sqlite3.connect('data/merged_data.sqlite')
merged_df.to_sql('merged_data', conn, if_exists='replace', index=False)
conn.close()

# Clean up cache and extracted folders
shutil.rmtree('renewable-energy')
shutil.rmtree('co2-emissions')
for file in os.listdir(os.environ['KAGGLE_DATASETS_CACHE']):
    os.remove(os.path.join(os.environ['KAGGLE_DATASETS_CACHE'], file))
