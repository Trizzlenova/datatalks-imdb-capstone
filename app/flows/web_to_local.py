from flask import Flask
from pathlib import Path
import pandas as pd
from prefect import flow, task

@task(retries = 3)
def fetch(dataset_url: str) -> pd.DataFrame:
    '''Read data from web into pandas dataframe'''
    df = pd.read_csv(dataset_url, sep='\t')
    return df

@task(log_prints = True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    '''Write dataframe out as a parquet file'''
    path = Path(f'data/{dataset_file}.parquet')
    df.to_parquet(path, compression = 'gzip')
    return path


@flow(log_prints = True)
def web_to_local():

    dataset_file = 'title.crew'
    dataset_url = f'https://datasets.imdbws.com/{dataset_file}.tsv.gz'

    df = fetch(dataset_url)
    print(f'df: {len(df)}')
    path = write_local(df, dataset_file)