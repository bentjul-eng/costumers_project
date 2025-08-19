from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "processing_app") -> SparkSession:
    """
    Creates and returns a SparkSession.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
    )
    
    return builder.getOrCreate()
