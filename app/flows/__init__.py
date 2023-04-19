import glob
from prefect import flow
from prefect_gcp.cloud_storage import GcsBucket
from pyspark.sql import SparkSession

from ..models import imdb_data

from .etl_fetch_to_spark import fetch
from .etl_spark_to_local import write_local
from .etl_local_to_gcs import write_gcs
from .etl_gcs_to_bq import etl_gcs_to_bq

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

@flow(log_prints = True)
def etl_web_to_gcs():
    # dataset_name = imdb_data[0]['dataset_name']
    # schema = imdb_data[0]['schema']

    for data in imdb_data:
        dataset_name = data['dataset_name']
        schema = data['schema']

        df = fetch(spark, dataset_name, schema)
        path = write_local(df, dataset_name)
        write_gcs(path)
        etl_gcs_to_bq(dataset_name)