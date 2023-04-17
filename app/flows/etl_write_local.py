from prefect import task

@task(log_prints = True)
def write_local(df, dataset_name: str) -> str:
    '''Write dataframe out as a parquet file'''
    path = f'data/imdb/{dataset_name}'

    df.write.parquet(
        path,
        compression='gzip',
        mode = 'overwrite'
    )

    return path