# scripts/pipeline.py

import os
import shutil
import subprocess
import zipfile
import pandas as pd
import sqlite3

# Set Kaggle environment variables
os.environ['KAGGLE_CONFIG_DIR'] = os.path.expanduser('~/.kaggle')
os.environ['KAGGLE_DATASETS_CACHE'] = os.path.expanduser('~/.kaggle/cache')

# Ensure the cache directory exists
os.makedirs(os.environ['KAGGLE_DATASETS_CACHE'], exist_ok=True)

# Kaggle datasets to download
datasets = [
    ('arnabchaki/renewable-energy-production-by-country', 'renewable-energy.zip', 'renewable-energy'),
    ('edgar/ghg-emissions', 'co2-emissions.zip', 'co2-emissions')  # Replace with actual identifier if needed
]

# Download and extract datasets
for dataset, zip_file, folder in datasets:
    subprocess.run(f'kaggle datasets download -d {dataset}', shell=True, check=True)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(folder)
    os.remove(zip_file)

# Function to read CSV with various encodings
def read_csv_with_encodings(file_path, encodings=['latin1', 'ISO-8859-1', 'cp1252']):
    for enc in encodings:
        try:
            return pd.read_csv
