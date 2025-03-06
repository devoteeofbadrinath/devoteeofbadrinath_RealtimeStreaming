from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def read_from_pheonix_original(
        spark: SparkSession,
        table: str,
        zk_url: str
) -> DataFrame:
    """
    Read data from HBase via Pheonix.

    :param spark: SparkSession instance
    :param table: HBase table name
    :param zk_url: Zookeeper URL for HBase
    :return: DataFrame containing the data read from Pheonix
    """
    return spark.read.format('pheonix') \
        .option('table', table) \
        .option('zkUrl', zk_url) \
        .load()

def read_from_pheonix(
        spark: SparkSession,
        table: str,
        zk_url: str
) -> DataFrame:
    """
    Read data from HBase via Pheonix.

    :param spark: SparkSession instance
    :param table: HBase table name
    :param zk_url: Zookeeper URL for HBase
    :return: DataFrame containing the data read from Pheonix
    """
    mysql_url = "jdbc:mysql://localhost:3306/ods"
    mysql_properties = {
    "user": "root",
    "password": "Hari@@14@@09",
    "driver": "com.mysql.cj.jdbc.Driver"
    }
      
    return spark.read.jdbc(url=mysql_url, table=table, properties=mysql_properties)


def read_from_pheonix_with_jdbc(
        spark: SparkSession,
        query: str,
        zk_url: str,
        driver: str
) -> DataFrame:
    """
    Read data from HBase via Pheonix.
    
    :param spark: SparkSession instance
    :param query: SQL query to getch data from pheonix table
    :param zk_url: Zookeeper URL for HBase
    :param driver: Pheonix driver name
    :return: DataFrame containing the data read from Pheonix
    """
    
    return spark.read.jdbc(url=zk_url,
                           table=query,
                           properties={"user": "root",
                                     "password": "Hari@@14@@09",
                                     "driver": "com.mysql.cj.jdbc.Driver"})

    

def read_from_pheonix_with_jdbc1(
        spark: SparkSession,
        query: str,
        zk_url: str,
        driver: str
) -> DataFrame:
    """
    Read data from HBase via Pheonix.
    
    :param spark: SparkSession instance
    :param query: SQL query to getch data from pheonix table
    :param zk_url: Zookeeper URL for HBase
    :param driver: Pheonix driver name
    :return: DataFrame containing the data read from Pheonix
    """
    jdsbc_url = f"jdbc:pheonix:{zk_url}:/hbase"
    return spark.read.format("jdbc") \
        .option("dbtable", query) \
        .option("url", jdsbc_url) \
        .option("driver", driver) \
        .load()