from pyspark.sql import functions as F

def write_parquet(df, path, mode="overwrite"):
    df.write.mode(mode).parquet(path)

def write_parquet_partitioned(df, path, partition_cols, mode="append"):
    df.write.mode(mode).partitionBy(partition_cols).parquet(path)

def write_json(df, path, mode="overwrite"):
    df.write.mode(mode).json(path)
