from flask import Flask
from .flows.web_to_local import *
from pathlib import Path
import os

from pyspark.sql import SparkSession, DataFrame, types
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

# from schemas import spark, title_crew_schema

title_crew_schema = types.StructType([
    types.StructField('tconst', types.StringType(), True),
    types.StructField('directors', types.StringType(), True),
    types.StructField('writers', types.StringType(), True)
])

@task(name = "fetch imdb data", retries = 3)
def get_local(path: str) -> DataFrame:
    '''Read data from web into spark dataframe'''

    # path = Path(f'data/title.crew.parquet')
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('imdb-project') \
        .getOrCreate()
    
    df = spark.read \
        .option("header", "true") \
        .schema(title_crew_schema) \
        .parquet(path, sep='\t')
    
    df.show()
    # path = Path(f'app/data/title.crew.parquet')
    df.write.parquet(
        path, 
        compression='gzip',
        mode = 'overwrite'
    )
    
    return path

@task(log_prints = True)
def write_gcs(path: Path) -> None:
    # path = Path(f'data/{dataset_file}.parquet')
    '''Uploading local parquet file to GCS'''
    gcp_cloud_storage_bucket_block = GcsBucket.load('prefect-gcs')
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path = f'{path}',
        to_path = 'data/imdb/title.crew.parquet'
    )
    return

@flow(log_prints = True)
def create_app():
    app = Flask(__name__)
    dirname = os.path.dirname(__file__)
    file = os.path.join(dirname, 'data/title.crew.parquet')
    path = get_local(file)
    print(path)
    # write_gcs(path)


    return app