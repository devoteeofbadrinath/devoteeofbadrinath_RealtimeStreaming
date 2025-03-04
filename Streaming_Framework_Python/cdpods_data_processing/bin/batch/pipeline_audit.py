import sys
import json
import html
from ods_data_processing.batch.logging_module import configure_logger
from ods_data_processing.batch.app import application_stage
from ods_data_processing.batch.audit import job_execution
from ods_data_processing.utils import spark_session
from ods_data_processing.batch.batch_types import AppStage, LogParams

if __name__ == "__main__":
    audit_args = json.loads(html.unescape(sys.argv[1]))
    job_vars = json.loads(html.unescape(sys.argv[2]))
    generic_vars = json.loads(html.unescape(sys.argv[3]))
    dag_pipeline_vars = json.loads(html.unescape(sys.argv[4]))
    with spark_session("spark session for job execs task", generic_vars, job_vars) as spark:
        job_args = {**audit_args, **job_vars, **generic_vars, **dag_pipeline_vars}
        log_params = LogParams(job_args.get('log_level', 'INFO'), None, job_args['job_id'])
        configure_logger(log_params, spark)
        with application_stage(AppStage.AUDIT_TASK):
            job_execution(spark, job_args)