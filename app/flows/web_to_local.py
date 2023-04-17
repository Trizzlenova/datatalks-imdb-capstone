from flask import Flask
import glob
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import pyspark
from pyspark.sql import SparkSession, types, DataFrame

from ..models import imdb_data


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

@task(retries = 3)
def fetch(dataset_file: str, schema) -> None:
    dataset_url = f'https://datasets.imdbws.com/{dataset_file}.tsv.gz'
    spark.sparkContext.addFile(dataset_url)
    df = spark.read \
        .option("header", "true") \
        .csv(
            f'file://{pyspark.SparkFiles.get(dataset_file)}.tsv.gz',
            sep='\t',
            schema=schema
        )
    
    return df



@task(log_prints = True)
def write_local(df, dataset_name: str) -> glob:
    '''Write dataframe out as a parquet file'''
    path = f'data/imdb/{dataset_name}'

    df.write.parquet(
        path,
        compression='gzip',
        mode = 'overwrite'
    )

    return path



@task(log_prints = True)
def write_gcs(path: str) -> None:
    from_file = glob.glob(f'{path}/*.gz.parquet')[0]
    '''Uploading local parquet file to GCS'''
    gcp_cloud_storage_bucket_block = GcsBucket.load('prefect-gcs')
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path = from_file,
        to_path = f'{path}.parquet'
    )
    return


@flow(log_prints = True)
def web_to_local():
    # dataset_name = imdb_data[0]['dataset_name']
    # schema = imdb_data[0]['schema']

    for data in imdb_data:
        dataset_name = data['dataset_name']
        schema = data['schema']

        print(dataset_name)

        df = fetch(dataset_name, schema)
        path = write_local(df, dataset_name)
        write_gcs(path)
