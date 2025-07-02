import json
import logging
import dataclasses as dtc
from datetime import datetime, timedelta, timezone
from typing import Dict
import queue
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from brdj_stream_processing.stream.stream_types import ApplicationContext, RecordMetrics
from brdj_stream_processing.stream.stream_config import StreamConfig
from brdj_stream_processing.stream.output_module import write_to_phoenix
from brdj_stream_processing.stream.error import OffsetNotFoundError
from brdj_stream_processing.utils.phoenix import read_from_phoenix


logger = logging.getLogger(__name__)


STATS_SCHEMA = StructType([
    StructField("airflow_workflow_name", StringType(), True),
    StructField("airflow_task_name", StringType(), True),
    StructField("controlm_workflow_name", StringType(), True),
    StructField("controlm_processed_name", StringType(), True),
    StructField("spark_cde_run_id", StringType(), True),
    StructField("topic_name", StringType(), True),
    StructField("json_data", StringType(), True)
])

OFFSETS_SCHEMA = StructType([
    StructField("topic_partition", StringType(), True),
    StructField("topic_offset", LongType(), True),
    StructField("topic_name", StringType(), True)
])


def metrics_as_dict(
        metrics: RecordMetrics
) -> dict:
    """
    Convert statistical metrics to a JSON-compatible dictionary.
    
    :param metrics: Dictionary containing batch data counts.
    :return: A Dictionary containing the statistical metrics.
    """
    return {
        "topic_count": metrics.total,
        "metrics_window_start_time": metrics.metrics_window_start_time.isoformat(),
        "metrics_window_end_time": metrics.metrics_window_end_time.isoformat(),
        "metrics_stages": {
            "01_validation": metrics.validation,
            "02_transformation": metrics.transformation,
            "03_exception": metrics.exception,
            "04_dlq": metrics.dlq,
            "05_target_write": metrics.target_write
        }
    }


def create_stats_df(
        spark: SparkSession,
        kafka_topic: str,
        json_data: dict,
        dag_pipeline_args: dict
) -> DataFrame:
    """
    Create a Spark DataFrame for statistical metrics.
    
    :param spark: The Spark session.
    :param kafka_topic: The Kafka topic name.
    :param json_data: The JSON data to be included in the DataFrame.
    :param dag_pipeline_args: The dag pipeline variables.
    :return: A Spark DataFrame containing the statistical metrics.
    """
    data = [
        (
            dag_pipeline_args["airflow_workflow_name"],
            dag_pipeline_args["airflow_task_name"],
            dag_pipeline_args["controlm_workflow_name"],
            dag_pipeline_args["controlm_processed_date"],
            dag_pipeline_args["spark_cde_run_id"],
            kafka_topic,
            json.dumps(json_data)
        )
    ]
    return spark.createDataFrame(data, STATS_SCHEMA)


def read_kafka_offsets(spark: SparkSession, conf: StreamConfig) -> Dict[str, int]:
    """
    Retrive the latest offsets from phoenix table for a specified kafka topic.
    
    :param spark: The SparkSession object.
    :param conf: Stream config variables.
    :return: A dictionary mapping topic partitions to their offsets.
    """
    table = f"{conf.phoenix_data_source['generic_schema']}.{conf.phoenix_data_source['offset_table']}"
    offsets_df = read_from_phoenix(
        spark,
        table,
        conf.phoenix_data_source["zkurl"]
    )
    offsets = offsets_df.select('topic_partition', 'topic_offset') \
        .filter(F.col('topic_name') == conf.kafka_topic)
    offsets = {r.topic_partition: r.topic_offset for r in offsets.collect()}
    if not offsets:
        raise OffsetNotFoundError(f"No offsets found for {conf.kafka_topic}")
    return offsets


def write_offsets(
        ctx: ApplicationContext,
        offsets: dict,
        latest_offsets: dict
) -> dict:
    """
    This function creates a DataFrame from the global offsets
    and writes it to the specified phoenix table.
    It ensures that the offsets are recorded
    in the phoenix table for future reference.

    :param ctx: Application context variables.
    :param offsets: Existing offsets dict value.
    :param latest_offsets: Latest offsets dict value.
    """
    logger.info("Inside the process write offsets %s",latest_offsets)
    if latest_offsets:
        offset_df = ctx.spark.createDataFrame(
            [
                (k, v + 1, ctx.config.kafka_topic)
                for k, v in latest_offsets.items()
                if offsets.get(k) != v
            ],
            OFFSETS_SCHEMA
        )
        logger.info("Inside the process write offsets %s",latest_offsets)
        write_to_phoenix(
            offset_df,
            f"{ctx.config.phoenix_data_source['generic_schema']}."
            f"{ctx.config.phoenix_data_source['offset_table']}",
            ctx.config.phoenix_data_source["zkurl"]
        )
        # Updates the existing offsets dict with changes values from latest offsets
        # Python 3.9: return offsets | latest offsets
        updated_offsets = offsets.copy()
        updated_offsets.update(latest_offsets)
        logger.info("Writing offsets is done")
        return updated_offsets
    

def write_metrics(ctx: ApplicationContext, metrics: RecordMetrics):
    """
    Write stats to db with the given metrics and timestampes.

    :param ctx: Application context variables.
    :param metrics: Metrics data.
    """
    logger.info("Inside the process write metrics ")
    if metrics.total > 0:
        json_data = metrics_as_dict(metrics)
        conf = ctx.config
        stats_df = create_stats_df(
            ctx.spark,
            conf.kafka_topic,
            json_data,
            conf.dag_pipeline_args
        )
        stats_df = stats_df.withColumn("logged_timestamp", F.current_timestamp())
        write_to_phoenix(
            stats_df,
            f"{conf.phoenix_data_source['generic_schema']}."
            f"{conf.phoenix_data_source['stats_table']}",
            conf.phoenix_data_source["zkurl"]
        )
        logger.info("Writing metrics is done")


def update_metrics(total_metrics: RecordMetrics, metrics: RecordMetrics) -> RecordMetrics:
    """
    Updates the metrics with latest values.
    
    :param total_metrics: Existing metrics.
    :param metrics: New incoming metrics.
    
    """
    logger.info("Inside the process update metrics ")
    total_metrics = dtc.replace(
        total_metrics,
        metrics_window_start_time = min(total_metrics.metrics_window_start_time, metrics.metrics_window_start_time),
        metrics_window_end_time = max(total_metrics.metrics_window_end_time, metrics.metrics_window_end_time),
        total = total_metrics.total + metrics.total,
        target_write = total_metrics.target_write + metrics.target_write
    )
    return total_metrics


def process_metrics_and_offsets(ctx: ApplicationContext, offsets: dict):
    """
    Process and write the Kafka topic offsets and record metrics.
    
    :param ctx: Application context variables.
    :param offsets: Kafka topic offsets.
    """
    logger.info("Inside process metrics and offsets")
    m_queue = ctx.state.metrics_queue
    total_metrics = RecordMetrics()
    latest_offsets = {}
    last_updated_time = datetime.now(timezone.utc)
    while ctx.state.is_running:
        try:
            msg = m_queue.get(timeout = ctx.config.offset_sleeping_time)
        except queue.Empty:
            pass
        else:
            metrics: RecordMetrics = msg["metrics"]
            latest_offsets = msg["offsets"]
            total_metrics = update_metrics(total_metrics, metrics)

        if datetime.now(timezone.utc) - last_updated_time > timedelta(seconds=ctx.config.offset_sleeping_time):
            offsets = write_offsets(ctx, offsets, latest_offsets)

            #write_metrics(ctx, total_metrics)
            total_metrics = RecordMetrics()
            last_updated_time = datetime.now(timezone.utc)

    write_offsets(ctx, offsets, latest_offsets)
    #write_metrics(ctx, total_metrics)
    logger.info("Finished writing offsets and metrics")
            

