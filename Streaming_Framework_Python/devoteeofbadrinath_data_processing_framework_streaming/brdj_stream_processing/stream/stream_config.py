from brdj_stream_processing.stream.stream_types import StreamConfig, TransformParams


def create_stream_config(
        generic_args,
        job_args,
        dag_pipeline_args,
        user_id,
        workload_password
) -> StreamConfig:
    """Function returns an object which stores configurations"""

    print("generic_args =",generic_args)
    print("job_args =",job_args)
    print("dag_pipeline_args =",dag_pipeline_args)
    
    kafka_topic = job_args["kafka_topic"]
    application_shutdown_time = job_args.get("application_shutdown_time", 0)
    offset_sleeping_time = job_args["offset_sleeping_time"]

    kafka_bootstrap_servers = generic_args["kafka_bootstrap_servers"]
    schema_registry_details = {
        "subject": job_args["schema_registry_details"]["subject"],
        "url": generic_args["schema_registry_details"]["url"]
    }
    phoenix_data_source = {
        "aEE": generic_args["phoenix_data_source"]["schema"],
        "offset_table": generic_args["phoenix_data_source"]["offset_table"],
        "stats_table": generic_args["phoenix_data_source"]["stats_table"],
        "zkurl": generic_args["phoenix_data_source"]["zkurl"],
        "phoenix_driver": generic_args["phoenix_data_source"]["phoenix_driver"],
        "job_schema": job_args["phoenix_data_source"]["schema"],
        "table": job_args["phoenix_data_source"]["table"],
        "src_id_col": job_args["phoenix_data_source"]["src_id_col"],
        "tgt_id_col": job_args["phoenix_data_source"]["tgt_id_col"]
    }

    transform_params = TransformParams(
        reference_data=generic_args['transformation_reference_data_path'],
        source_data=generic_args['transformation_source_data_path'],
        mapping=generic_args['transformation_mapping_path'],
        reference_schema_mapping=job_args['reference_schema_mapping'],
        transformation_type=job_args['transformation_type']
    )
    
    return StreamConfig(
        # TODO: pipeline_task_id to be revisied as part of next sprint
        pipeline_task_id = dag_pipeline_args["airflow_task_name"],
        user_id=user_id,
        workload_password=workload_password,
        kafka_topic=kafka_topic,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        application_shutdown_time=application_shutdown_time,
        offset_sleeping_time=offset_sleeping_time,
        schema_registry_details=schema_registry_details,
        transform_params=transform_params,
        phoenix_data_source=phoenix_data_source,
        dag_pipeline_args=dag_pipeline_args
    )