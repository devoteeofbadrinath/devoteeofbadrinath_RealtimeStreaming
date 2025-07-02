from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def read_from_phoenix(
        spark: SparkSession,
        table: str,
        zk_url: str
) -> DataFrame:
    """
    Read data from HBase via Phoenix.

    :param spark: SparkSession instance
    :param table: HBase table name
    :param zk_url: Zookeeper URL for HBase
    :return: DataFrame containing the data read from Phoenix
    """  
    return spark.read.format('phoenix') \
        .option('table', table) \
        .option('zkUrl', zk_url) \
        .load()

    
def read_from_phoenix_with_jdbc(
        spark: SparkSession,
        query: str,
        zk_url: str,
        driver: str
) -> DataFrame:
    """
    Read data from HBase via Phoenix.
    
    :param spark: SparkSession instance
    :param query: SQL query to getch data from phoenix table
    :param zk_url: Zookeeper URL for HBase
    :param driver: Phoenix driver name
    :return: DataFrame containing the data read from Phoenix
    """
    jdbc_url = f"jdbc:phoenix:{zk_url}:/hbase"
    return spark.read.format("jdbc") \
        .option("dbtable", query) \
        .option("url", jdbc_url) \
        .option("driver", driver) \
        .load()

def read_from_phoenix_1(
        spark: SparkSession,
        table: str,
        zk_url: str
) -> DataFrame:
    """
    Read data from HBase via phoenix.

    :param spark: SparkSession instance
    :param table: HBase table name
    :param zk_url: Zookeeper URL for HBase
    :return: DataFrame containing the data read from Phoenix
    """
    mysql_url = "jdbc:mysql://localhost:3306/BRDJ_USECASE_REFERENCE_FILES"
    mysql_properties = {
    "user": "root",
    "password": "Hari@@14@@09",
    "driver": "com.mysql.cj.jdbc.Driver"
    }
      
    return spark.read.jdbc(url=mysql_url, table=table, properties=mysql_properties)


def read_from_phoenix_with_jdbc_1(
        spark: SparkSession,
        query: str,
        zk_url: str,
        driver: str
) -> DataFrame:
    """
    Read data from HBase via Phoenix.
    
    :param spark: SparkSession instance
    :param query: SQL query to getch data from phoenix table
    :param zk_url: Zookeeper URL for HBase
    :param driver: Phoenix driver name
    :return: DataFrame containing the data read from Phoenix
    """
    
    return spark.read.jdbc(url=zk_url,
                           table=query,
                           properties={"user": "root",
                                     "password": "Hari@@14@@09",
                                     "driver": "com.mysql.cj.jdbc.Driver"})



