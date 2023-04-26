import glob
from prefect import task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints = True)
def write_gcs(path: str) -> None:
    from_file = glob.glob(f'{path}/*.gz.parquet')[0]
    print(from_file)
    '''Uploading local parquet file to GCS'''
    gcp_cloud_storage_bucket_block = GcsBucket.load('imdb-gcs')
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path = from_file,
        to_path = f'{path}.parquet'
    )
    return