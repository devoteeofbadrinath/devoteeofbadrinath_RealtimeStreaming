"""
Unit tests for application context related functions.
"""
from brdj_stream_processing.stream.app import application_context


def test_application_context(stream_args):
    with application_context() as ctx:
        assert ctx is not None
        stream_args.assert_called_once()
        assert ctx.config.kafka_topic == "test_topic"
        assert ctx.config.application_shutdown_time == 60
        assert ctx.config.offset_sleeping_time == 10
        assert ctx.config.kafka_bootstrap_servers == 'localhost:9092'
        assert ctx.config.s3_log_bucket == "test_log_bucket"
        assert ctx.config.s3_endpoint == "s3_endpoint"
        assert ctx.config.schema_registry_details == {
            "subject": "test_subject",
            "url": "http://localhost:8081"
        }
        assert ctx.config.phoenix_data_source == {
            "generic_schema": "test_generic_schema",
            "offset_table": "test_offset_table",
            "stats_table": "test_stats_table",
            "zkurl": "http://zkurl",
            "phoenix_driver": "org.apache.phoenix.jdbc.PhoenixDriver",
            "job_schema": "test_job_schema",
            "table": "test_table",
            "src_id_col": "src_id",
            "tgt_id_col": "tgt_id"
        }
        assert ctx.generic_args == {
            "kafka_bootstrap_servers": "localhost:9092",
            "s3_endpoint": "s3_endpoint",
            "s3_log_bucket": "test_log_bucket",
            "schema_registry_details": {"url": "http://localhost:8081"},
            "phoenix_data_source": {
                "schema": "test_generic_schema",
                "offset_table": "test_offset_table",
                "stats_table": "test_stats_table",
                "zkurl": "http://zkurl",
                "phoenix_driver": "org.apache.phoenix.jdbc.PhoenixDriver"
            },
            "stream_process_name":"brdj-dev-stream-process",
            "transformation_reference_data_path": ctx.generic_args["transformation_reference_data_path"],
            "transformation_source_data_path": ctx.generic_args["transformation_source_data_path"],
            "transformation_mapping_path": ctx.generic_args["transformation_mapping_path"]
        }
        assert ctx.job_args == {
            "kafka_topic": "test_topic",
            "kafka_topic_dlq": "test_topic_dlq",
            "application_shutdown_time": 60,
            "offset_sleeping_time": 10,
            "schema_registry_details": {"subject": "test_subject"},
            "phoenix_data_source": {
                "schema": "test_job_schema",
                "table": "test_table",
                "src_id_col": "src_id",
                "tgt_id_col": "tgt_id"
            },
            "log_params": {
                "log_level": "INFO",
                "log_path": "test_logpath",
                "run_id": "test_runid"
            },
            "reference_schema_mapping":{"BRDJ_USECASE_REFERENCE_FILES":"brdj_usecase_reference_files"}
        }
        assert ctx.dag_pipeline_args == {
            "airflow_task_name": "test_task",
            "config_path": "config_path",
            "airflow_workflow_name":"cde_bkkg_acntbal_brdj_stream",
            "controlm_workflow_name":"controlm_workflow_name",
            "controlm_processed_date":"2025-01-01",
            "spark_cde_run_id":"spark_cde_run_id",
            "kafka_topic":"test_topic"
        }