import json
import logging
from typing import Sequence
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from brdj_stream_processing.stream.stream_config import StreamConfig
from brdj_stream_processing.utils.pheonix import read_from_pheonix_with_jdbc
from brdj_stream_processing.stream.transform_module import transform_data, transform_data_structure

logger = logging.getLogger(__name__)

def parse_avro_data(df: DataFrame, schema: dict) -> DataFrame:
    """
    Parse application data in Avro format from a Spark stream.
    
    :param df: Dataframe with application data in Avro format.
    :param schema: Dictionary representing Avro schema.
    :return DataFrame with parsed data.
    """
    #schema = json.dumps(schema)
    schema = json.dumps({"type":"record","name":"BKK_AccountBalanceUpdate","namespace":"xml","fields":[{"name":"MessageHeader","type":{"type":"record","name":"MessageHeader","fields":[{"name":"Timestamp","type":{"type":"long","logicalType":"timestamp-millis"},"xmlkind":"element"},{"name":"BusCorID","type":"string","xmlkind":"element"},{"name":"LocRefNum","type":"string","xmlkind":"element"},{"name":"RetAdd","type":"string","xmlkind":"element"},{"name":"SeqNum","type":"string","xmlkind":"element"},{"name":"ReqRef","type":"string","xmlkind":"element"},{"name":"OriSou","type":"string","xmlkind":"element"},{"name":"EventAction","type":"string","xmlkind":"element"}]},"xmlkind":"element"},{"name":"MessageBody","type":{"type":"record","name":"MessageBody","fields":[{"name":"AccArr","type":{"type":"record","name":"AccArr","fields":[{"name":"AccNum","type":"string","xmlkind":"element"},{"name":"Balance","type":{"type":"bytes","logicalType":"decimal","precision":32,"scale":6},"xmlkind":"element"},{"name":"BalanceStatus","type":"string","xmlkind":"element"}]},"xmlkind":"element"},{"name":"Branch","type":{"type":"record","name":"Branch","fields":[{"name":"NSC","type":"string","xmlkind":"element"}]},"xmlkind":"element"}]},"xmlkind":"element"}]})
    # Parse the Avro data using the shema provided
    
    return df \
        .select(from_avro(F.col("value"),schema).alias("data"))\
        .select("data.*")


def read_stream_from_kafka(
        spark: SparkSession,
        conf: StreamConfig,
        micro_batch_fn,
        starting_offsets: str,
        options: dict = None
) -> DataFrame:
    """
    Read data as stream from a Kafka topic.
    
    :param spark: SparkSession instance.
    :param conf: Stream config variables.
    :param micro_batch_fn: Micro batch function.
    :param starting_offsets: Starting offset of the topic.
    :param options: Additional optional configuration options.
    :return DataFrame containing the data read from Kafka.
    """
    # TODO - THE used_id-password APPROACH MUST BE REPLACED WITH keytabl
    # AFTER POC IS DONE, AS PART OF THE NEXT SPRINT

    logger.info("read_stream_from_kafka STARTING_OFFSETS = %s",starting_offsets)
    df_reader = (
        spark.readStream.format("kafka")
        .option("subscribe", conf.kafka_topic)
        .option("kafka.bootstrap.servers", conf.kafka_bootstrap_servers)
        .option("startingOffsets", starting_offsets)
        # .option("kafka.security.protocol", "SASL_SSL")
        # .option("kafka_sasl_mechanism", "PLAIN")
        # .option(
        #     "kafka.sasl.jass.config",
        #     f'org.apache.kafka.common.security.plain.PlainLoginModule '
        #     f'required username="{conf.user_id}"'
        #     f'password="{conf.workload_password}";')
        .option("value.deserializer",
                "io.confluent.kafka.serializers.KafkaAvroDeserializer")
        #.load()
    )

    #parsed_df = parse_avro_data(df_reader, schema)
    #parsed_df = parse_avro_data(df_reader, {})
    #df_reader = transform_data_structure(parsed_df)
    #df_reader.createTempView("source_df")

    # The additional options ared added if passed.
    if options is not None:
        for key, value in options.items():
            df_reader = df_reader.option(key, value)

    return df_reader.load().writeStream.foreachBatch(micro_batch_fn).start()
    #return df_reader.writeStream.foreachBatch(micro_batch_fn).start()

def read_stream_from_kafka1(
        spark: SparkSession,
        conf: StreamConfig,
        micro_batch_fn,
        starting_offsets: str,
        options: dict = None
) -> DataFrame:
    """
    Read data as stream from a Kafka topic.
    
    :param spark: SparkSession instance.
    :param conf: Stream config variables.
    :param micro_batch_fn: Micro batch function.
    :param starting_offsets: Starting offset of the topic.
    :param options: Additional optional configuration options.
    :return DataFrame containing the data read from Kafka.
    """
    # TODO - THE used_id-password APPROACH MUST BE REPLACED WITH keytabl
    # AFTER POC IS DONE, AS PART OF THE NEXT SPRINT

    df_reader = (
        spark.readStream.format("kafka")
        .option("subscribe", conf.kafka_topic)
        .option("kafka.bootstrap.servers", conf.kafka_bootstrap_servers)
        .option("starting_offsets", starting_offsets)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka_sasl_mechanism", "PLAIN")
        .option(
            "kafka.sasl.jass.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule '
            f'required username="{conf.user_id}"'
            f'password="{conf.workload_password}";')
        .option("value.deserializer",
                "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    )

    # The additional options ared added if passed.
    
    if options is not None:
        for key, value in options.items():
            df_reader = df_reader.option(key, value)

    return df_reader.load().writeStream.foreachBatch(micro_batch_fn).start()

def read_from_kafka(
        conf: StreamConfig,
        spark: SparkSession,
        starting_offsets: dict,
        micro_batch_fn
) -> DataFrame:
    """
    Read streaming data from specified Kafka partitions using schema registry.
    
    :param conf: Stream config variables.
    :param spark: Spark session.
    :param starting_offsets: Kafka offsets value.
    :param micro_batch_fn: Micro batch function.
    :return: DataFrame containing the Kafka data.
    """
        
    starting_offsets = json.dumps(
        {
            conf.kafka_topic: {
                part: offset
                for part, offset in starting_offsets.items()
            }
        }
    )
    logger.info("read_from_kafka STARTING_OFFSETS = %s",starting_offsets)
    logger.debug("starting_offsets: %s", starting_offsets)
    return read_stream_from_kafka(
        spark,
        conf,
        micro_batch_fn,
        starting_offsets
    )


def read_data_from_source(
        spark: SparkSession,
        phoenix_data_source: dict,
        columns: Sequence,
        incoming_df: DataFrame
) -> DataFrame:
    """
    Read data from source Pheonix table.
    
    :param spark: Spark session.
    :param pheonix_data_source: Dictionary containing the Pheonix data source configuration.
    :param columns: List of columns to read from tje table.
    :param incoming_df: DataFrame containing the incoming data to filter by.
    :return: DataFrame containing the selected columns from the Pheonix table.
    """

    # TODO - THE BELOW LINDS OF CODE SHOULD BE TAKEN AS A SQL QUERY
    # FROM THE CONFIG FILE AS PART OF THE NEXT SPRINT

    table = f"{phoenix_data_source['job_schema']}.{phoenix_data_source['table']}"

    # Collect the IDs from the incoming dataframe to use in the WHERE clause
    id_list = (
        incoming_df.select(phoenix_data_source["src_id_col"])
        .distinct().rdd.flatMap(lambda x: x)
        .collect()
    )

    # Read data from Pheonix table
    df = read_from_pheonix_with_jdbc(
        spark,
        table,
        phoenix_data_source["zkurl"],
        phoenix_data_source["phoenix_driver"]
    )

    if len(id_list) > 0:
        df = df.filter(F.col(f"{phoenix_data_source['tgt_id_col']}").isin(id_list))

    # Select specified columns from the DataFrame
    return df.select(*columns)
    
def read_data_from_source1(
        spark: SparkSession,
        phoenix_data_source: dict,
        columns: Sequence,
        incoming_df: DataFrame
) -> DataFrame:
    """
    Read data from source Pheonix table.
    
    :param spark: Spark session.
    :param pheonix_data_source: Dictionary containing the Pheonix data source configuration.
    :param columns: List of columns to read from tje table.
    :param incoming_df: DataFrame containing the incoming data to filter by.
    :return: DataFrame containing the selected columns from the Pheonix table.
    """

    # TODO - THE BELOW LINDS OF CODE SHOULD BE TAKEN AS A SQL QUERY
    # FROM THE CONFIG FILE AS PART OF THE NEXT SPRINT

    table = f"{phoenix_data_source['job_schema']}.{phoenix_data_source['table']}"

    # Collect the IDs from the incoming dataframe to use in the WHERE clause
    id_list = (
        incoming_df.select(phoenix_data_source["src_id_col"])
        .distinct().rdd.flatMap(lambda x: x)
        .collect()
    )

    # Read data from Pheonix table
    df = read_from_pheonix_with_jdbc(
        spark,
        table,
        phoenix_data_source["zkurl"],
        phoenix_data_source["pheonix_driver"]
    )

    if len(id_list) > 0:
        df = df.filter(F.col(f"{phoenix_data_source['tgt_id_col']}").isin(id_list))

    # Select specified columns from the DataFrame
    return df.select(*columns)
    
