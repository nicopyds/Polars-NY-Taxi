import os
import urllib

import multiprocessing as mp

import pandas as pd
import polars as pl

CWD = os.getcwd()
PATH_INPUT_FOLDER = os.path.join(CWD, "input2")

FILES_TO_DOWNLOAD = [
    
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
    
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-06.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-07.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-08.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-09.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-10.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet",
    
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-02.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-04.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-05.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-06.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-07.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-08.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-09.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-10.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-11.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-12.parquet",
    
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-02.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-03.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-04.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-05.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-06.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-07.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-08.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-09.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-10.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-11.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-12.parquet",
    
]

def make_input_folder(folder_path):
    
    if not os.path.isdir(folder_path):
        os.makedirs(folder_path)

def extract_basename(url):
    
    return os.path.basename(url)

def download_file(t):
    
    folder_path, url = t
    basename = os.path.join(folder_path, extract_basename(url = url))
    
    urllib.request.urlretrieve(url = url, filename = basename)

iterable = list(zip([PATH_INPUT_FOLDER for i in range(len(FILES_TO_DOWNLOAD))], FILES_TO_DOWNLOAD))
cpu_cores = mp.cpu_count() - 2

make_input_folder(folder_path = PATH_INPUT_FOLDER)

if __name__ == "__main__":

    pool = mp.get_context(method="fork").Pool(processes = cpu_cores)
    pool.map(func = download_file, iterable = iterable)
    pool.close()