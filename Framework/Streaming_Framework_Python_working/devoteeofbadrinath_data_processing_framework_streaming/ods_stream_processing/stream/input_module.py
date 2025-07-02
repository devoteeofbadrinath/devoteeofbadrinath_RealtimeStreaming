from __future__ import annotations

import json
import logging
from functools import partial
from typing import Sequence
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro
from brdj_stream_processing.utils.phoenix import read_from_phoenix_with_jdbc
from brdj_stream_processing.stream.output_module import process_micro_batch_dlq
from brdj_stream_processing.stream.stream_types import ApplicationContext

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
    df = df \
         .select("partition", "offset", "value",
                 from_avro(F.col("value"), schema, options={"mode": "PERMISSIVE"}).alias("data")) \
         
    valid_df = df.filter(F.col("data.MessageBody").isNotNull()) \
 
    invalid_df = df.filter(F.col("data.MessageBody").isNull()) \

    return valid_df, invalid_df

def read_stream_from_kafka(
        ctx: ApplicationContext,
        micro_batch_fn,
        starting_offsets: str,
        schema: str,
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

    conf = ctx.config
    df_reader = (
        ctx.spark.readStream.format("kafka")
        .option("subscribe", conf.kafka_topic)
        .option("kafka.bootstrap.servers", conf.kafka_bootstrap_servers)
        .option("startingOffsets", starting_offsets)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka_sasl_mechanism", "PLAIN")
        .option(
            "kafka.sasl.jass.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule '
            f'required username="{conf.user_id}" '
            f'password="{conf.workload_password}";')
        .option("value.deserializer",
                "io.confluent.kafka.serializers.KafkaAvroDeserializer")
        )

    #parsed_df = parse_avro_data(df_reader, schema)
    #parsed_df = parse_avro_data(df_reader, {})
    #df_reader = transform_data_structure(parsed_df)
    #df_reader.createTempView("source_df")

    # The additional options ared added if passed.
    if options is not None:
        for key, value in options.items():
            df_reader = df_reader.option(key, value)

    df = df_reader.load()
    valid_df, invalid_df = parse_avro_data(df, schema)
    invalid_df.writeStream.foreachBatch(partial(process_micro_batch_dlq, ctx)).start()
    valid_df.writeStream.foreachBatch(micro_batch_fn).start()
    

def read_stream_from_kafka_1(
        ctx: ApplicationContext,
        micro_batch_fn,
        starting_offsets: str,
        schema: str,
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

    conf = ctx.config
    df_reader = (
        ctx.spark.readStream.format("kafka")
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

    df = df_reader.load()
    valid_df, invalid_df = parse_avro_data(df, schema)
    invalid_df.writeStream.foreachBatch(partial(process_micro_batch_dlq, ctx)).start()
    valid_df.writeStream.foreachBatch(micro_batch_fn).start()


def read_from_kafka(
        ctx: ApplicationContext,
        starting_offsets: dict,
        schema: dict,
        micro_batch_fn
) -> None:
    """
    Read streaming data from specified Kafka partitions using schema resgistry.

    :param ctx: Streaming application context.
    :param starting_offsets: Kafka offsets value.
    :param schema: Avro schema froms schema registry.
    :param micro_batch_fn: Micro batch function.
    :return DataFrame containing the Kafka data.
    """
    starting_offsets = json.dumps(
        {
            ctx.config.kafka_topic: {
                part: offset
                for part, offset in starting_offsets.items()
            }
        }
    )
    logger.debug("starting_offsets: %s", starting_offsets)
    read_stream_from_kafka(
        ctx,
        micro_batch_fn,
        starting_offsets,
        schema,
    )
    
def read_data_from_source(
        spark: SparkSession,
        phoenix_data_source: dict,
        columns: Sequence,
        incoming_df: DataFrame
) -> DataFrame:
    """
    Read data from source phoenix table.
    
    :param spark: Spark session.
    :param phoenix_data_source: Dictionary containing the phoenix data source configuration.
    :param columns: List of columns to read from tje table.
    :param incoming_df: DataFrame containing the incoming data to filter by.
    :return: DataFrame containing the selected columns from the phoenix table.
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

    # Read data from phoenix table
    df = read_from_phoenix_with_jdbc(
        spark,
        table,
        phoenix_data_source["zkurl"],
        phoenix_data_source["phoenix_driver"]
    )

    if len(id_list) > 0:
        df = df.filter(F.col(f"{phoenix_data_source['tgt_id_col']}").isin(id_list))

    # Select specified columns from the DataFrame
    return df.select(*columns)
    