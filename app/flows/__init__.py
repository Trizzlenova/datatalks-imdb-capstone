from pathlib import Path
from prefect import flow
from prefect_gcp.cloud_storage import GcsBucket

from ..models import imdb_data

from .etl_fetch_to_pandas import fetch
from .etl_pandas_to_local import write_local
from .etl_local_to_gcs import write_gcs
from .etl_gcs_to_bq import etl_gcs_to_bq

@flow(log_prints = True)
def etl_web_to_bq():

    # for data in imdb_data:
        dataset_name = imdb_data[3]['dataset_name']
        print(dataset_name)
        schema = imdb_data[3]['schema']

        # df = fetch(dataset_name)
        # path = write_local(df, dataset_name)

        # path = Path(f'data/imdb/{dataset_name}.parquet')
        # write_gcs(path)
        etl_gcs_to_bq(dataset_name)