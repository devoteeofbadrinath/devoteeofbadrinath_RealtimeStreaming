from collections.abc import Sequence
from pyspark.sql import SparkSession, DataFrame

def read_pheonix_table(
        spark: SparkSession,
        table: str,
        columns: Sequence,
        zk_url: str,
) -> DataFrame:
    df = spark.read.format('pheonix') \
            .option('table', table) \
            .option('zkurl', zk_url) \
            .load()
    
    return df.select(*columns)