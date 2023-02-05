import pandas as pd
import json
import os
import requests
from abc import ABC, abstractmethod

# External website file url
# source_url = "https://em-datatsets.s3.amazonaws.com/car_sales.csv"

# Local source directory
base_path = os.path.abspath(__file__ + "/../../")

# Extract column mapping
with open('column_mapping.json') as f:
    cols = json.load(f)

def create_folder_if_not_exists(path:str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

# Downloads a file from a publicly available url
def download_file(url:str) -> str:
    local_filename = os.path.join(base_path,'data','input',url.split('/')[-1])
    create_folder_if_not_exists(local_filename)
    # Download as stream
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename

def main(source:str, source_type:str, online:bool, chunksize:int=None):
    # Reads the csv file and extracts a subset of the columns
    source_file = download_file(source) if online else source
    if source_type == 'excel':
        raw_df = pd.read_excel(source_file, usecols=cols.keys())
    elif source_type == 'csv':
        raw_df = pd.read_csv(source_file, encoding='utf-8', usecols=cols.keys(), chunksize=chunksize, on_bad_lines='skip')
    else:
        raise ValueError("Unsupported data source type")
    return raw_df
