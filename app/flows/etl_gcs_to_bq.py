from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

from os import environ 
from dotenv import load_dotenv, find_dotenv


load_dotenv(find_dotenv())

@task(retries = 3)
def extract_from_gcs(dataset_name: str) -> Path:
    '''Download trip data from GCS'''
    gcs_path = f'data/imdb/{dataset_name}.parquet'
    gcs_block = GcsBucket.load('imdb-gcs')
    gcs_block.get_directory(from_path = gcs_path, local_path = f'./')
    return gcs_path

@task(log_prints = True)
def transform(path: str) -> None:
    '''Data cleaning example'''
    df = pd.read_parquet(path)
    print(len(df))
    return df

@task(log_prints = True)
def write_to_bq(dataset_name: str, df) -> None:
    '''Write dataframe to BigQuery'''
    gcp_credentials_block = GcpCredentials.load('imdb-gcp-creds')
    table_name = dataset_name.replace('.', '_')
    df.to_gbq(
        destination_table = f'dl_imdb.{table_name}',
        project_id = 'de-imdb',
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize = 500_000,
        if_exists = 'replace'
    )

@flow(log_prints = True)
def etl_gcs_to_bq(dataset_name):
    '''Main ETL flow to load data into Big Query'''
    path = extract_from_gcs(dataset_name)
    df = transform(path)
    write_to_bq(dataset_name, df)