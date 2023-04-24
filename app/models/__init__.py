from pyspark.sql import types

# eventually loop through all imdb data
imdb_data = [
    {
        'dataset_name': 'title.akas',
        'schema': types.StructType([
            types.StructField('titleId', types.StringType(), True),
            types.StructField('ordering', types.IntegerType(), True),
            types.StructField('title', types.StringType(), True),
            types.StructField('region', types.StringType(), True),
            types.StructField('language', types.StringType(), True),
            types.StructField('types', types.StringType(), True),
            types.StructField('attributes', types.StringType(), True),
            types.StructField('isOriginalTitle', types.IntegerType(), True)
        ])
    },
    {
        'dataset_name': 'title.basics',
        'schema': types.StructType([
            types.StructField('tconst', types.StringType(), True),
            types.StructField('titleType', types.StringType(), True),
            types.StructField('primaryTitle', types.StringType(), True),
            types.StructField('originalTitle', types.StringType(), True),
            types.StructField('isAdult', types.IntegerType(), True),
            types.StructField('startYear', types.IntegerType(), True),
            types.StructField('endYear', types.IntegerType(), True),
            types.StructField('runTimeMinutes', types.IntegerType(), True),
            types.StructField('genres', types.StringType(), True)
        ])
    },
    {
        'dataset_name': 'title.crew',
        'schema': types.StructType([
            types.StructField('tconst', types.StringType(), True),
            types.StructField('directors', types.StringType(), True),
            types.StructField('writers', types.StringType(), True)
        ])
    },
    {
        'dataset_name': 'title.episode',
        'schema': types.StructType([
            types.StructField('tconst', types.StringType(), True),
            types.StructField('parentTconst', types.StringType(), True),
            types.StructField('seasonNumber', types.IntegerType(), True),
            types.StructField('episodeNumber', types.IntegerType(), True),
        ])
    },
    {
        'dataset_name': 'title.principals',
        'schema': types.StructType([
            types.StructField('tconst', types.StringType(), True),
            types.StructField('ordering', types.IntegerType(), True),
            types.StructField('nconst', types.StringType(), True),
            types.StructField('category', types.StringType(), True),
            types.StructField('job', types.StringType(), True),
            types.StructField('characters', types.StringType(), True)
        ])
    },
    {
        'dataset_name': 'title.ratings',
        'schema': types.StructType([
            types.StructField('tconst', types.StringType(), True),
            types.StructField('averageRating', types.DoubleType(), True),
            types.StructField('numVotes', types.IntegerType(), True),
        ])
    }
]