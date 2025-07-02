"""Module contains fixtures for use throughout test suite"""

import io
import argparse
import json
import typing as tp
from pyspark.sql import SparkSession, DataFrame
from queue import Queue

from brdj_stream_processing.stream.stream_config import StreamConfig
from brdj_stream_processing.stream.stream_types import ApplicationContext, ValidationParams

import pytest
from unittest import mock

# Define SOURCE_DATA_CONFIG as a string
SOURCE_DATA_CONFIG = """\
pipeline_task_id,pipeline_name,pipeline_task_name,sql_query,primary_key,join_type,merge_key,sort_key,audit_column,version,last_updated,last_updated_by
test_task,test,task,"select CASE WHEN OriSou = 'BKK' THEN concat('BKKG', '^', AccNum, '^', NSC) END as ACNT_ID_NUM, case when BALANCESTATUS = 'POSTED' then BALANCE ELSE LDGR_BAL_AMT END as LDGR_BAL_AMT_NEW,case when BALANCESTATUS = 'POSTED' then SeqNum ELSE LDGR_BAL_SEQ END as LDGR_BAL_SEQ_NEW,case when BALANCESTATUS = 'POSTED' then TIMESTAMP ELSE LDGR_BAL_DTTM END as LDGR_BAL_DTTM_NEW,case when BALANCESTATUS = 'POSTED' then SHDW_BAL_AMT ELSE BALANCE END as SHDW_BAL_AMT_NEW,case when BALANCESTATUS = 'SHADOW' then SeqNum ELSE SHDW_BAL_SEQ END as SHDW_BAL_SEQ_NEW,case when BALANCESTATUS = 'SHADOW' then TIMESTAMP ELSE SHDW_BAL_DTTM END as SHDW_BAL_DTTM_NEW,ACNT_ID_NUM_KEY,ACNT_ID_TYP_CD,ACNT_PRTY_REL,ACNT_NUM,PARNT_NSC_NUM,ACNT_OPN_DT,ACNT_NAME,ACNT_STS_CD,ACNT_CLSED_IND,JURIS_CD,SRC_SYS_ACNT_TYP,SRC_SYS_ACNT_SUB_TYP,ACNT_CCY_REF,LDGR_BAL_EOD,ACNT_MRKT_CLAS_CD,CUST_CR_CNTRL_STRTGY_CD,BUS_CTR_CNTRLD_IND,UNQ_ACNT_GRP_NUM,ACNT_SUB_OFC_CD,DR_INT_RATE_TYP,DR_INT_RATE,ACNT_DATA_SRC_CD,BATCH_PROCESS_NAME,START_DATE,LAST_UPDT_DATE,RECORD_DELETED_FLAG,ACNT_ROLE_UPDT_SEQ,ACNT_ROLE_UPDT_DTTM from source_df_with_latest_accounts left join existing_df_full on CASE WHEN OriSou = 'BKK' THEN concat('BKKG', '^', AccNum, '^', NSC) = existing_df_full.ACNT_ID_NUM END",,none,,,,0.24,2024-11-04,c138147
"""

# Define REFERENCE_DATA_CONFIG as a string
REFERENCE_DATA_CONFIG = """\
pipeline_task_id,pipeline_name,pipeline_task_name,query_order,transformation_step,database_type,sql_query,temp_table_name,read_in_batches,version,last_updated,last_updated_by
test_task,test,task,1,normalise,sparksql,"SELECT Data.MessageHeader.Timestamp,Data.MessageHeader.BusCorID,Data.MessageHeader.LocRefNum,Data.MessageHeader.RetAdd,Data.MessageHeader.SeqNum,Data.MessageHeader.ReqRef,Data.MessageHeader.OriSou,Data.MessageHeader.EventAction,Data.MessageBody.AccArr.AccNum,Data.MessageBody.AccArr.Balance,Data.MessageBody.AccArr.BalanceStatus, Data.MessageBody.Branch.NSC FROM Global_Temp.stream_df where Data.MessageHeader.OriSou='BKK'",parsed_df,,1,2023--11-04,c138147
test_task,test,task,2,transform,sparksql,"select * from ( select *, row_number() over (partition by AccNum, NSC order by timestamp desc) as rn from parsed_df ) tmp where rn = 1",source_df_with_latest_accounts,,1,2023--11-04,c138147
test_task,test,task,3,transform,phoenix,BRDJ_USECASE_REFERENCE_FILES.ACNT,existing_df_full,,1,2023--11-04,c138147
"""

MAPPING_CONFIG = """\
pipeline_task_id,pipeline_name,pipeline_task_name,source_col_name,target_col_name,column_order,target_data_type,transformation_rule,version,last_updated,last_updated_by
test_task,test,task,ACNT_ID_NUM,ACNT_ID_NUM_KEY,1,string,passthrough,0.24,2025-03-05,c138147
test_task,test,task,ACNT_ID_NUM,ACNT_ID_NUM,2,string,passthrough,0.24,2025-03-05,c138147
test_task,test,task,SHDW_BAL_AMT_NEW,SHDW_BAL_AMT,30,"decimal(23,4)",passthrough,0.24,2025-03-05,c138147
test_task,test,task,SHDW_BAL_DTTM_NEW,SHDW_BAL_DTTM,31,timestamp,passthrough,0.24,2025-03-05,c138147
test_task,test,task,SHDW_BAL_SEQ_NEW,SHDW_BAL_SEQ,32,string,passthrough,0.24,2025-03-05,c138147
test_task,test,task,LDGR_BAL_AMT_NEW,LDGR_BAL_AMT,33,"decimal(23,4)",passthrough,0.24,2025-03-05,c138147
test_task,test,task,LDGR_BAL_DTTM_NEW,LDGR_BAL_DTTM,34,timestamp,passthrough,0.24,2025-03-05,c138147
test_task,test,task,LDGR_BAL_SEQ_NEW,LDGR_BAL_SEQ,35,string,passthrough,0.24,2025-03-05,c138147
"""

@pytest.fixture
def sample_reference_file(tmp_path_factory):
    """Creates a temporary file with a string and returns its path."""
    temp_dir = tmp_path_factory.mktemp("reference_data")
    file_path = temp_dir / "test_file.csv"  # Create a temporary CSV file
    file_path.write_text(REFERENCE_DATA_CONFIG)  # Write REFERENCE_DATA_CONFIG to file
    return str(file_path)  # Return the file path as a string

@pytest.fixture
def sample_source_file(tmp_path_factory):
    """Creates a temporary file with a string and returns its path."""
    temp_dir = tmp_path_factory.mktemp("source_data")
    file_path = temp_dir / "test_file.csv"  # Create a temporary CSV file
    file_path.write_text(SOURCE_DATA_CONFIG)  # Write REFERENCE_DATA_CONFIG to file
    return str(file_path)  # Return the file path as a string

@pytest.fixture
def sample_mapping_file(tmp_path_factory):
    """Creates a temporary file with a string and returns its path."""
    temp_dir = tmp_path_factory.mktemp("mapping_data")
    file_path = temp_dir / "test_file.csv"  # Create a temporary CSV file
    file_path.write_text(MAPPING_CONFIG)  # Write REFERENCE_DATA_CONFIG to file
    return str(file_path)  # Return the file path as a string

@pytest.fixture
def spark_session() -> tp.Iterator[SparkSession]:
    """
    Spark session unit test fixture.
    """
    spark = SparkSession.builder.master('local[2]').getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


@pytest.fixture
def generic_args(sample_reference_file, sample_source_file, sample_mapping_file):
    """Fixture that provides generic arguments including transformation reference data path."""
    return {
        "kafka_bootstrap_servers": "localhost:9092",
        "s3_log_bucket": "test_log_bucket",
        "s3_endpoint": "s3_endpoint",
        "schema_registry_details": {"url": "http://localhost:8081"},
        "phoenix_data_source": {
            "schema": "test_generic_schema",
            "offset_table": "test_offset_table",
            "stats_table": "test_stats_table",
            "zkurl": "http://zkurl",
            "phoenix_driver": "org.apache.phoenix.jdbc.PhoenixDriver"
        },
        "stream_process_name":"brdj-dev-stream-process",
        "transformation_reference_data_path": sample_reference_file,
        "transformation_source_data_path": sample_source_file,
        "transformation_mapping_path": sample_mapping_file
    }


@pytest.fixture
def job_args():
    return {
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


@pytest.fixture
def dag_pipeline_args():
    return {
        "airflow_task_name": "test_task",
        "config_path": "config_path",
        "airflow_workflow_name":"cde_bkkg_acntbal_brdj_stream",
        "controlm_workflow_name":"controlm_workflow_name",
        "controlm_processed_date":"2025-01-01",
        "spark_cde_run_id":"spark_cde_run_id",
        "kafka_topic":"test_topic"
    }


@pytest.fixture
def ctx(spark_session, generic_args, job_args, dag_pipeline_args):
    config = mock.MagicMock()
    config.kafka_topic = "test_topic"
    config.kafka_topic_dlq = "dead_letter_queue_topic"
    config.phoenix_data_source = {
        "generic_schema": "test_schema",
        "job_schema": "job_schema",
        "offset_table": "test_offset_table",
        "stats_table": "test_stats_table",
        "table": "test_table",
        "zkurl": "localhost:2181"
    }
    config.validation_params = ValidationParams(
        config_path=dag_pipeline_args["config_path"]
    )
    config.dag_pipeline_args = dag_pipeline_args
    state = mock.MagicMock()
    state.metrics_queue = Queue()
    state.is_running = True
    return ApplicationContext(spark=spark_session, config=config, state=state,
                              generic_args=generic_args, job_args=job_args,
                              dag_pipeline_args=dag_pipeline_args)


@pytest.fixture
def record_schema():
    return {
        "type": "record",
        "name": "TestRecord",
        "fields": [
            {
                "name": "MessageHeader",
                "type": {
                    "type": "record",
                    "name": "MessageHeaderRecord",
                    "fields": [
                        {"name": "header_key", "type": "string"}
                    ]
                }
            },
            {
                "name": "MessageBody",
                "type": {
                    "type": "record",
                    "name": "MessageBodyRecord",
                    "fields": [
                        {"name": "body_key", "type": "string"},
                        {
                            "name": "AccArr",
                            "type": {
                                "type": "record",
                                "name": "AccArrRecord",
                                "fields": [
                                    {"name": "acc_key", "type": "string"}
                                ]
                            }
                        },
                        {
                            "name": "Branch",
                            "type": {
                                "type": "record",
                                "name": "BranchRecord",
                                "fields": [
                                    {"name": "branch_key", "type": "string"}
                                ]
                            }
                        }
                    ]
                }
            }
        ]
    }


@pytest.fixture
def stream_args(generic_args, job_args, dag_pipeline_args):
    with mock.patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(
        user_id='mock_user_id',
        workload_password='mock_password',
        generic_vars=json.dumps(generic_args),
        job_vars=json.dumps(job_args),
        dag_pipeline_args=json.dumps(dag_pipeline_args)
    )) as p:
        yield p


@pytest.fixture
def stream_config():
    return StreamConfig(
        pipeline_task_id="TestReadStreamFromKafka",
        pipeline_name="TestReadStreamFromKafka",
        stream_process_name="brdj-dev-stream-process",
        user_id="dummy_id",
        workload_password="dummy_password",
        kafka_topic="test_topic",
        kafka_topic_dlq="test_topic_dlq",
        kafka_bootstrap_servers="localhost:9092",
        s3_log_bucket="test_log_bucket",
        s3_endpoint="s3_endpoint",
        application_shutdown_time=None,
        offset_sleeping_time=None,
        schema_registry_details=None,
        phoenix_data_source=None,
        dag_pipeline_args=None,
        log_params=None,
        validation_params=None,
        transform_params=None,
        transform_cfg=None
    )


def df_column(df:DataFrame, column: str, sorted: bool = False) -> list[tp.Any]:
    """
    Unit tetst utility function to extract list of values for specific
    column of a dataframe.
    """
    df = df.select(column)
    if sorted:
        df = df.orderBy(column)
    return [row[0] for row in df.collect()]