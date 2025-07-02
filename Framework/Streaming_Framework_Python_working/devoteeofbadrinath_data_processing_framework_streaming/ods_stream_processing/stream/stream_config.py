from __future__ import annotations

from brdj_stream_processing.stream.stream_types import StreamConfig, TransformParams, ValidationParams
from brdj_stream_processing.stream.transform_module import read_transform_config
def create_stream_config(
        generic_args,
        job_args,
        dag_pipeline_args,
        user_id,
        workload_password
) -> StreamConfig:
    """Function returns an object which stores configurations"""

    kafka_topic = job_args["kafka_topic"]
    kafka_topic_dlq = job_args["kafka_topic_dlq"]
    application_shutdown_time = job_args.get("application_shutdown_time", 0)
    offset_sleeping_time = job_args["offset_sleeping_time"]

    kafka_bootstrap_servers = generic_args["kafka_bootstrap_servers"]
    s3_log_bucket = generic_args["s3_log_bucket"]
    s3_endpoint = generic_args["s3_endpoint"]
    schema_registry_details = {
        "subject": job_args["schema_registry_details"]["subject"],
        "url": generic_args["schema_registry_details"]["url"]
    }
    phoenix_data_source = {
        "generic_schema": generic_args["phoenix_data_source"]["schema"],
        "offset_table": generic_args["phoenix_data_source"]["offset_table"],
        "stats_table": generic_args["phoenix_data_source"]["stats_table"],
        "zkurl": generic_args["phoenix_data_source"]["zkurl"],
        "phoenix_driver": generic_args["phoenix_data_source"]["phoenix_driver"],
        "job_schema": job_args["phoenix_data_source"]["schema"],
        "table": job_args["phoenix_data_source"]["table"],
        "src_id_col": job_args["phoenix_data_source"]["src_id_col"],
        "tgt_id_col": job_args["phoenix_data_source"]["tgt_id_col"]
    }
    log_params = {
        "log_level": job_args["log_params"]["log_level"],
        "log_path": job_args["log_params"]["log_path"],
        "run_id": job_args["log_params"]["run_id"]
    }

    validation_params = ValidationParams(
        config_path = dag_pipeline_args["config_path"]
    )

    transform_params = TransformParams(
        reference_data=generic_args['transformation_reference_data_path'],
        source_data=generic_args['transformation_source_data_path'],
        mapping=generic_args['transformation_mapping_path'],
        reference_schema_mapping=job_args['reference_schema_mapping']
    )

    transform_cfg = read_transform_config(transform_params, dag_pipeline_args["airflow_task_name"])
    
    return StreamConfig(
        # TODO: pipeline_task_id to be revisied as part of next sprint
        pipeline_task_id = dag_pipeline_args["airflow_task_name"],
        pipeline_name=dag_pipeline_args["airflow_task_name"],
        stream_process_name=generic_args["stream_process_name"],
        user_id=user_id,
        workload_password=workload_password,
        kafka_topic=kafka_topic,
        kafka_topic_dlq=kafka_topic_dlq,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        s3_log_bucket=s3_log_bucket,
        s3_endpoint=s3_endpoint,
        application_shutdown_time=application_shutdown_time,
        offset_sleeping_time=offset_sleeping_time,
        schema_registry_details=schema_registry_details,
        transform_params=transform_params,
        phoenix_data_source=phoenix_data_source,
        dag_pipeline_args=dag_pipeline_args,
        log_params=log_params,
        validation_params=validation_params,
        transform_cfg=transform_cfg
    )