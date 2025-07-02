from collections.abc import Sequence
from pyspark.sql import SparkSession, DataFrame

def read_phoenix_table(
        spark: SparkSession,
        table: str,
        columns: Sequence,
        zk_url: str,
) -> DataFrame:
    df = spark.read.format('phoenix') \
            .option('table', table) \
            .option('zkurl', zk_url) \
            .load()
    
    return df.select(*columns)


def read_phoenix_table_1(
        spark: SparkSession,
        table: str,
        columns: Sequence,
        zk_url: str,
) -> DataFrame:
    df = spark.read.jdbc(url=zk_url,
                         table=table,
                         properties={
                             "user": "root",
                             "password": "Hari@@14@@09",
                             "driver": "com.mysql.cj.jdbc.Driver"})
    
    return df.select(*columns)
    
    
def full_table_name(
        db_name: str,
        table_name: str
) -> str:
    """Create fully qualified name for database and table name"""
    return f'{db_name}.{table_name}'