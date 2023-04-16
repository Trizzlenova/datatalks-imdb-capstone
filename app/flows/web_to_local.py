from flask import Flask
from pathlib import Path
from prefect import flow, task
import pyspark
from pyspark.sql import SparkSession, types, DataFrame


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

title_crew_schema = types.StructType([
    types.StructField('tconst', types.StringType(), True),
    types.StructField('directors', types.StringType(), True),
    types.StructField('writers', types.StringType(), True)
])

@task(retries = 3)
def fetch(dataset_file: str) -> None:
    dataset_url = f'https://datasets.imdbws.com/{dataset_file}.tsv.gz'
    spark.sparkContext.addFile(dataset_url)
    df = spark.read \
        .option("header", "true") \
        .csv(
            f'file://{pyspark.SparkFiles.get(dataset_file)}.tsv.gz',
            sep='\t',
            schema=title_crew_schema
        )
    
    return df



@task(log_prints = True)
def write_local(df, dataset_file: str) -> Path:
    '''Write dataframe out as a parquet file'''

    df.write.parquet(
        f'app/data/{dataset_file}.parquet', 
        compression='gzip',
        mode = 'overwrite'
    )
    path = Path(f'app/data/{dataset_file}.parquet')

    return path


@task(log_prints = True)
def web_to_local():

    dataset_file = 'title.crew'

    df = fetch(dataset_file)
    path = write_local(df, dataset_file)

