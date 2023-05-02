from pathlib import Path
import pandas as pd
from prefect import task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints = True)
def write_gcs(path: Path) -> None:
    '''Uploading local parquet file to GCS'''
    gcp_cloud_storage_bucket_block = GcsBucket.load('imdb-gcs')
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path = path,
        to_path = f'{path}'
    )
    return