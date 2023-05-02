from prefect import task
from pathlib import Path
import pandas as pd

@task(log_prints = True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    '''Write dataframe out as a parquet file'''
    path = Path(f'data/imdb/{dataset_file}.parquet')
    df.to_parquet(path, compression = 'gzip')
    return path