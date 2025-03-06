from __future__ import annotations

import logging
from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from brdj_stream_processing.stream.stream_types import ApplicationContext, RecordMetrics
from brdj_stream_processing.stream.input_module import read_data_from_source, parse_avro_data
from brdj_stream_processing.stream.transform_module import transform_data, transform_data_structure
from brdj_stream_processing.stream.output_module import write_to_pheonix


logger = logging.getLogger(__name__)


def read_latest_offsets(df: DataFrame) -> dict[str, int]:
    """
    Read the latest offsets per Kafka topic partition form the incoming dataframe.
    
    :param_df: Incoming batch dataframe.
    :returns: Dictionary with offset per partition of Kafka topic.
    """
    # Extract and update offsets
    window_spec = Window.partitionBy("partition").orderBy(F.desc("offset"))

    # Use row_number to get the latest offset for each partition
    offset_rows = df.withColumn("row_num", F.row_number().over(window_spec)) \
        .filter(F.col("row_num") == 1) \
        .collect()
    logger.info("offset_rows =%s", offset_rows)
    return {r.partition: r.offset for r in offset_rows}


def process_data(ctx: ApplicationContext, batch_df: DataFrame, schema: dict):
    """
    Process the batch parsing and transformation and writes to the target table.

    :param ctx: Application context variables.
    :param batch_df: DataFrame containing the current batch of streaming data to be processed.
    :param schema: Avro schema from schema registry.
    """
    
    phoenix_data_source = ctx.config.phoenix_data_source
    
    # Parse and transform the Kafka data
    parsed_df = parse_avro_data(batch_df, schema)
    parsed_df.createOrReplaceGlobalTempView("parsed_df")
    
    # This line has been commented as exploding of the avro dataframe is being done by using reference config files. 
    #parsed_df = transform_data_structure(parsed_df)
    
    # Perform data transformation
    transformed_df = transform_data(ctx, parsed_df)
    final_df_count = transformed_df.count()
    print(final_df_count)
    

    # Write the final DataFrame back to the pheonix table
    # write_to_pheonix(
    #     transformed_df,
    #     f"{phoenix_data_source['job_schema']}.{phoenix_data_source['table']}",
    #     phoenix_data_source["zkurl"]
    # )
    return final_df_count


def process_stream_batch(ctx: ApplicationContext, schema: dict, batch_df: DataFrame, batch_id: int):
    """
    Process the micro-batch of streaming data and updates the metrics.
    
    :param ctx: Application context variables.
    :param schema: Avro schema from schema registry.
    :param batch_df: DataFrame containing the current batch of streaming data to be processed.
    :param batch_id: The unique identifier for the current batch of streaming data.
    """
    
    metrics_window_start_time = datetime.now(timezone.utc)

    latest_offsets = read_latest_offsets(batch_df)

    batch_count = batch_df.count()
    
    final_df_count = process_data(ctx, batch_df, schema)

    metrics_window_end_time = datetime.now(timezone.utc)

    metrics = RecordMetrics(
        metrics_window_start_time = metrics_window_start_time,
        metrics_window_end_time = metrics_window_end_time,
        total = batch_count,
        target_write = final_df_count
    )

    ctx.state.metrics_queue.put({
        "offsets": latest_offsets,
        "metrics": metrics
    })

    logger.debug(
        "latest_offsets=%s, batch_count=%s, target_write_count=%s, "
        "metrics_window_start_time=%s, metrics_window_end_time=%s",
        latest_offsets,
        batch_count,
        final_df_count,
        metrics_window_start_time,
        metrics_window_end_time
    )



