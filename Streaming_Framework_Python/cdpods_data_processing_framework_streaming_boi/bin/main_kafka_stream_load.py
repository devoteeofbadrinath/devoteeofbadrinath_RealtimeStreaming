"""Main script for stream processing jobs"""
import sys
import os
import logging
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ods_stream_processing.stream.app import application_context
from ods_stream_processing.stream.input_module import read_from_kafka
from ods_stream_processing.stream.stream_process import process_stream_batch
from ods_stream_processing.utils.schema_registry import get_schema_from_registry
from ods_stream_processing.stream.audit import read_kafka_offsets, process_metrics_and_offsets


logger = logging.getLogger(__name__)

current_dir = os.path.dirname((os.path.abspath(__file__)))
parent_dir = os.path.abspath(os.path.join(current_dir, '../../'))
sys.path.insert(0, parent_dir)
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.0,mysql:mysql-connector-java:8.0.33,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


def spark_query_wait(ctx, query, timeout: int):
    try:
        # Shitdown the stream after {application_shutdown_time} seconds
        status = query.awaitTermination(timeout = timeout)
        logger.info("Query termination status: %s", status)
    finally:
        ctx.state_is_running = False


if __name__ == "__main__":
    with application_context() as ctx:
        #starting_offsets = read_kafka_offsets(ctx.spark, ctx.config)
        starting_offsets = {"0":0}
        
        # Retrieve schema from schema Registry
        sr_schema = get_schema_from_registry(
            ctx.config.schema_registry_details,
            ctx.config.user_id,
            ctx.config.workload_password
        )
        sr_schema = {"type":"record","name":"BKK_AccountBalanceUpdate","namespace":"xml","fields":[{"name":"MessageHeader","type":{"type":"record","name":"MessageHeader","fields":[{"name":"Timestamp","type":{"type":"long","logicalType":"timestamp-millis"},"xmlkind":"element"},{"name":"BusCorID","type":"string","xmlkind":"element"},{"name":"LocRefNum","type":"string","xmlkind":"element"},{"name":"RetAdd","type":"string","xmlkind":"element"},{"name":"SeqNum","type":"string","xmlkind":"element"},{"name":"ReqRef","type":"string","xmlkind":"element"},{"name":"OriSou","type":"string","xmlkind":"element"},{"name":"EventAction","type":"string","xmlkind":"element"}]},"xmlkind":"element"},{"name":"MessageBody","type":{"type":"record","name":"MessageBody","fields":[{"name":"AccArr","type":{"type":"record","name":"AccArr","fields":[{"name":"AccNum","type":"string","xmlkind":"element"},{"name":"Balance","type":{"type":"bytes","logicalType":"decimal","precision":32,"scale":6},"xmlkind":"element"},{"name":"BalanceStatus","type":"string","xmlkind":"element"}]},"xmlkind":"element"},{"name":"Branch","type":{"type":"record","name":"Branch","fields":[{"name":"NSC","type":"string","xmlkind":"element"}]},"xmlkind":"element"}]},"xmlkind":"element"}]}
        
        query = read_from_kafka(
            ctx.config,ctx.spark,starting_offsets,sr_schema,
            partial(process_stream_batch, ctx)
        )
        
        logger.info("Application shutdown: %s seconds", ctx.config.application_shutdown_time)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(process_metrics_and_offsets, ctx, starting_offsets),
                executor.submit(spark_query_wait, ctx, query, ctx.config.application_shutdown_time)
            ]
            try:
                for f in as_completed(futures):
                    f.result()
            finally:
                ctx.spark.stop()
                logger.info("Stream is terminated")