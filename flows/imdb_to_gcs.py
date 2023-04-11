from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

# https://datasets.imdbws.com/name.basics.tsv.gz
# https://datasets.imdbws.com/title.akas.tsv.gz
# https://datasets.imdbws.com/title.basics.tsv.gz
# https://datasets.imdbws.com/title.crew.tsv.gz
# https://datasets.imdbws.com/title.episode.tsv.gz
# https://datasets.imdbws.com/title.principals.tsv.gz
# https://datasets.imdbws.com/title.ratings.tsv.gz

url = f'https://datasets.imdbws.com/{dataset_file}.tsv.gz'

@task(retries = 3)
def fetch(dataset_url: str) -> pd.DataFrame:
    '''Read data from web into pandas dataframe'''

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints = True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    '''Fix dtype issues'''
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(df.head(2))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df

@task(log_prints = True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    '''Write dataframe out as a parquet file'''
    path = Path(f'data/{color}/{dataset_file}.csv.gz')
    df.to_csv(path, compression = 'gzip')
    return path

@task(log_prints = True)
def write_gcs(path: Path) -> None:
    '''Uploading local parquet file to GCS'''
    gcp_cloud_storage_bucket_block = GcsBucket.load('prefect-gcs')
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path = f'{path}',
        to_path = path
    )
    return

@flow(log_prints = True, retries=3)
def fetch_imdb_data(dataset_file: str) -> None:
    '''The main ETL function'''
    dataset_url = f'https://datasets.imdbws.com/{dataset_file}.tsv.gz'

    df = fetch(dataset_url)
    print(f'df: {len(df)}')
    df_clean = clean(df)
    print(f'df_clean: {len(df_clean)}')
    path = write_local(df_clean, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    year = 2019
    for month in range(1,13):
        etl_web_to_gcs('yellow', year, month)

# to execute, first run 'prefect orion start' in the terminal
# run this python script
# in bigquery:
'''
CREATE OR REPLACE EXTERNAL TABLE prefect-de-zoomcamp-376404.trips_data_all.green_tripdata
OPTIONS (
    FORMAT='CSV',
    URIS=['gs://de-prefect-overview-123987/data/green/*.gz']
)
'''