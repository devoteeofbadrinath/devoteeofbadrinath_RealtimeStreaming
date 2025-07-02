from brdj_stream_processing.stream.stream_config import create_stream_config
from brdj_stream_processing.stream.stream_types import StreamConfig


def test_create_stream_config(
        generic_args,
        job_args,
        dag_pipeline_args
):
    user_id = "test_user"
    workload_password = "test_password"

    result = create_stream_config(
        generic_args,
        job_args,
        dag_pipeline_args,
        user_id,
        workload_password
    )


    assert isinstance(result, StreamConfig)
    assert result.kafka_topic == job_args["kafka_topic"]
    assert result.kafka_bootstrap_servers == generic_args["kafka_bootstrap_servers"]
    assert result.s3_log_bucket == generic_args["s3_log_bucket"]
    assert result.s3_endpoint == generic_args["s3_endpoint"]
    assert result.application_shutdown_time == job_args["application_shutdown_time"]
    assert result.schema_registry_details == {
        "subject": job_args["schema_registry_details"]["subject"],
        "url": generic_args["schema_registry_details"]["url"]
    }
    assert result.phoenix_data_source == {
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
    assert result.pipeline_task_id == dag_pipeline_args["airflow_task_name"]
    assert result.user_id == user_id
    assert result.workload_password == workload_password
    assert result.log_params == {
        "log_level": job_args["log_params"]["log_level"],
        "log_path": job_args["log_params"]["log_path"],
        "run_id": job_args["log_params"]["run_id"]
    }