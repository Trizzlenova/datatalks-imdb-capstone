from prefect import task
import pandas as pd

@task(retries = 3)
def fetch(dataset_file: str) -> pd.DataFrame:
    dataset_url = f'https://datasets.imdbws.com/{dataset_file}.tsv.gz'
    df = pd.read_csv(dataset_url, sep='\t')

    return df