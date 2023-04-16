from flask import Flask
from pathlib import Path
from prefect_fugue import fugue_engine


# from .flows import *
from pyspark.sql import SparkSession, DataFrame
from prefect import flow, task
# from schemas import spark, title_crew_schema
import pyspark
from pyspark.sql import SparkSession, types


title_crew_schema = types.StructType([
    types.StructField('tconst', types.StringType(), True),
    types.StructField('directors', types.StringType(), True),
    types.StructField('writers', types.StringType(), True)
])

@task(name = "fetch imdb data", retries = 3)
def fetch(spark, url: str) -> DataFrame:
    '''Read data from web into spark dataframe'''
    print('hit fetch')
    df = spark.read \
        .option("header", "true") \
        .csv(url, sep='\t')
        # .schema(title_crew_schema) \
    df.show()
    return df


# @task(log_prints = True)
# def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
#     '''Write dataframe out as a parquet file'''
#     path = Path(f'data/{dataset_file}.parquet')
#     df.to_parquet(path, compression = 'gzip')
#     return path

@flow(log_prints = True)
def create_app():
    app = Flask(__name__)

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('imdb-project') \
        .getOrCreate()

    dataset_url = 'https://datasets.imdbws.com/title.crew.tsv.gz'

    df = fetch(spark, dataset_url)
    print(f'df: {len(df)}')
    # df_clean = clean(df)
    # print(f'df_clean: {len(df_clean)}')
    # path = write_local(df_clean, color, dataset_file)

    return app

# @flow
# def native_spark_flow(engine):
#     with fugue_engine(engine) as engine:
#         my_spark_task(engine.spark_session, 1)


@task
def my_spark_task(spark, n=1):
    df = spark.createDataFrame([[f"hello spark {n}"]], "a string")
    df.show()

@flow
def native_spark_flow(engine):
    with fugue_engine(engine) as engine:
        my_spark_task(engine.spark_session, 1)

spark = SparkSession.builder.getOrCreate()
native_spark_flow(spark)