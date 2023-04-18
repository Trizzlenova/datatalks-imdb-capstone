from prefect import task
import pyspark

@task(retries = 3)
def fetch(spark, dataset_file: str, schema) -> None:
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