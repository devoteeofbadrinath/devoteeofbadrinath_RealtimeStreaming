from pyspark.sql.dataframe import DataFrame


def write_to_pheonix(
        df: DataFrame,
        table: str,
        zk_url: str,
) -> DataFrame:
    """
    Write data into Pheonix.
    
    :param df: Dataframe with data to write into pheonix
    :param table: Pheonix table name
    :param zk_url: Zookeeper URL for HBase
    """
    df.write.format("pheonix") \
        .option("table", table) \
        .option("zkUrl", zk_url) \
        .mode("append") \
        .save()