from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from brdj_stream_processing.stream.stream_config import StreamConfig
from brdj_stream_processing.stream.stream_types import ApplicationContext


def write_to_phoenix(
        df: DataFrame,
        table: str,
        zk_url: str,
) -> DataFrame:
    """
    Write data into phoenix.
    
    :param df: Dataframe with data to write into phoenix
    :param table: phoenix table name
    :param zk_url: Zookeeper URL for HBase
    """
    df.write.format("phoenix") \
        .option("table", table) \
        .option("zkUrl", zk_url) \
        .mode("append") \
        .save()
    
    
def write_to_kafka(df: DataFrame, conf: StreamConfig, kafka_topic: str):
    """
    Write records to the kafka topic.

    :param rejected: Spark dataframe with rejected records.
    :param ctx: Application context.
    :param kafka_topic: Kafka topic name.
    """
    df.write.format("kafka") \
        .option("kafka.bootstrap.servers", conf.kafka_bootstrap_servers) \
        .option("topic", kafka_topic) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule '
            f'required username="{conf.user_id}" '
            f'password="{conf.workload_password}";'
        ) \
        .save()
    
def process_micro_batch_dlq(ctx: ApplicationContext, df: DataFrame, batch_id: int):
    """
    Process the micro-batch of streaming data and push to kafka topic.
    
    :param ctx: Application context variable.
    :param df: DataFrame containing the current batch if streaming data to be processed.
    :param batch_id: The unique identifier for the current batch of streaming data.
    """
    if not df.isEmpty():
        df = df.select(F.col("partition").cast("string").alias("key"), "offset", "value")
        write_to_kafka(df, ctx.config, ctx.config.kafka_topic_dlq)