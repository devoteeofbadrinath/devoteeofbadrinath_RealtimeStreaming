import operator
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
from pyspark.sql import SparkSession, DataFrame
from ods_data_processing.batch.batch_types import ApplicationContext, SourceType
from ods_data_processing.utils.s3 import s3_source_path
from ods_data_processing.utils.spark import save_spark_table

JOBS_SCHEMA = StructType([StructField("job_id", StringType(), True),
                          StructField("job_name", StringType(), True),
                          StructField("job_user", StringType(), True),
                          StructField("start_time", TimestampType(), True),
                          StructField("end_time", TimestampType(), True),
                          StructField("src_cd", StringType(), True),
                          StructField("src_file", StringType(), True),
                          StructField("src_loc", StringType(), True),
                          StructField("tgt_cd", StringType(), True),
                          StructField("tgt_tbl", StringType(), True),
                          StructField("data_ent", StringType(), True),
                          StructField("status", StringType(), True),
                          StructField("run_date", DateType(), True),
                          StructField("task_type", StringType(), True),
                          ])

PROCS_SCHEMA = StructType([StructField("task_id", StringType(), True),
                           StructField("task_name", StringType(), True),
                           StructField("task_user", StringType(), True),
                           StructField("start_time", TimestampType(), True),
                           StructField("end_time", TimestampType(), True),
                           StructField("job_id", StringType(), True),
                           StructField("data_ent", StringType(), True),
                           StructField("status", StringType(), True),
                           StructField("run_date", DateType(), True),
                           StructField("logs_name", StringType(), True),
                           StructField("task_type", StringType(), True)
                           ])

def job_execution(spark: SparkSession, job_args: dict):
    data = update_audit_data(job_args)
    data['src_cd'] = data['source_cd']
    data['src_loc'] = s3_source_path(data, data, data, data['src_file'])
    df = create_audit_dataframe(spark, data, JOBS_SCHEMA)
    save_spark_table(df, f"{job_args[audit_schema]}.{job_args['job_execution_tbl']}","run_date")

def proce_execution(ctx: ApplicationContext, task_type: str, status: str):
    data = {
        "logs_name": ctx.config.log_params.log_path,
        "task_name": ctx.dag_args['pipeline_task_name'],
        "task_type": task_type,
        "status": status,
        **ctx.job_args,
        **ctx.generic_args,
        **ctx.dag_args,
    }

    data = update_audit_data(data)
    df = create_audit_dataframe(ctx.spark, data, PROCS_SCHEMA)
    save_spark_table(df, f"{ctx.generic_args['audit_schema']}.{ctx.generic_args['procs_execution_tbl']}", "run_date")

def update_source_data(data: dict) -> dict:
    data = data.copy()
    source_type = data.get('source_type')
    if source_type is None:
        raise ValueError('Data source type not specified')
    if source_type == SourceType.FILE.value:
        data['src_loc'] = s3_source_path(data, data, data, data['src_file'])
    elif source_type == SourceType.DATABASE.value:
        data['src_file'] = f"{data['source_db_name']}.{data['source_table_name']}"
        data['src_loc'] = data["source_db_type"]
    else:
        raise ValueError(f'Unknown data source type: {source_type}')
    
    return data
    
def update_audit_data(data: dict) -> dict:
    data = data.copy()
    current_time = datetime.now(timezone.utc)
    current_date = datetime.today().date()
    if data["task_type"] == "start_task":
        data['start_time'] == current_time
        data['end_time'] == None
    elif data["task_type"] == "end_task":
        data['start_time'] = None
        data['end_time'] = current_time
    data['run_date'] = current_date
    data['src_cd'] = data['source_cd']

    return data

def create_audit_dataframe(spark: SparkSession, data: dict, schema: StructType) -> DataFrame:
    columns = [field.name for field in schema.fields]
    get_data = operator.itemgetter(*columns)
    df = spark.createDataFrame([get_data(data)], schema=schema)
    return df
