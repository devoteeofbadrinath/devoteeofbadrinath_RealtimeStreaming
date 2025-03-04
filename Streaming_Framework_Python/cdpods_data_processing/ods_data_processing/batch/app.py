import argparse
import json
import html
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from ods_data_processing.batch import batch_config, logging_module
from ods_data_processing.batch.audit import procs_execution
from ods_data_processing.batch.batch_types import ApplicationContext, AppStage, CSVDataSource, RecordValidationError
from ods_data_processing.utils import spark_session, hadoop_move

logger = logging.getLogger(__name__)

STAGE_DISABLED = "Stage %s is disabled"
STAGE_START = "The {} has started"
STAGE_END = "The {} has completed"
STAGE_ERROR = "The {} has failed: {}"

@contextmanager
def application_context() -> Iterator:

    parser = argparse.ArgumentParser()

    parser.add_argument('generic_vars', help = 'Generic variables')
    parser.add_argument('job_vars', help = 'Job variables')
    parser.add_argument('dag_peipeline_vars', help = 'DAG variables')

    args = parser.parse_args()

    generic_args = json.loads(html.unescape(args.generic_vars))
    job_args = json.loads(html.unescape(args.job_vars))
    dag_args = json.loads(html.unescape(args.dag_pipeline_vars))

    conf = batch_config.create_batch_config(generic_args, job_args, dag_args)

    with spark_session(conf.pipeline_task_id, generic_args, job_args) as spark:
        root_logger = logging_module.configure_logger(conf.log_params, spark)
        try:
            ctx = ApplicationContext(conf, spark, generic_args, job_args, dag_args)
            procs_execution(ctx, "start_task", "started")
            yield ctx
        except RecordValidationError as ex:
            logger.error("Pipeline execution failed: %s", ex)
            post_process(ctx, True)
            procs_execution(ctx, "end_task", "failed")
            raise
        except BaseException as ex:
            logger.error("Pipeline execution failed: %s", ex)
            procs_execution(ctx, "end_task", "failed")
            raise
        else:
            post_process(ctx, False)
            procs_execution(ctx, "end_task", "success")
        finally:
            for handler in root_logger.handlers:
                handler.flush()

@contextmanager
def application_stage(stage: AppStage) -> Iterator:
    logger.info(STAGE_START.format(stage.value))
    try:
        yield
    except BaseException as ex:
        logger.error(STAGE_ERROR.format(stage.value, ex))
        raise
    else:
        logger.info(STAGE_END.format(stage.value))

def post_process(
        ctx: ApplicationContext,
        failed: bool,
) -> None:
    conf = ctx.config
    spark = ctx.spark
    if conf.feature_enabled[AppStage.POST_PROCESSING.value]:
        assert isinstance(conf.data_source, CSVDataSource)
        if failed:
            data_file_path = conf.data_source.rejected_path
            ctl_file_path = conf.data_source.rejected_ctl_path
        else:
            data_file_path = conf.data_source.processed_path
            ctl_file_path = conf.data_source.processed_ctl_path

        with application_stage(AppStage.POST_PROCESSING):
            logger.info(f'moving data source files to: data={data_file_path}, control_file = {ctl_file_path}')
            hadoop_move(spark, conf.data_source.path, data_file_path)
            hadoop_move(spark, conf.data_source.ctl_path, ctl_file_path)

    else:
        logger.info(STAGE_DISABLED, AppStage.POST_PROCESSING.value)
        


