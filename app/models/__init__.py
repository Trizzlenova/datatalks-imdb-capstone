from pyspark.sql import types

# eventually loop through data
imdb_data = [
    {
        'dataset_name': 'title.crew',
        'schema': types.StructType([
            types.StructField('tconst', types.StringType(), True),
            types.StructField('directors', types.StringType(), True),
            types.StructField('writers', types.StringType(), True)
        ])
    }
]