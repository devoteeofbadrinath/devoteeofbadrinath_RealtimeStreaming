from collections.abc import Iterator
from contextlib import contextmanager
from pyspark.sql import SparkSession


def create_spark_properties(generic_args: dict, job_args: dict) -> dict:
    args = generic_args.copy()
    args.update(job_args)
    # TODO: THE BELOW return OBJECT MUST BE MODIFIED WHEN
    # AIRFLOW IS INTEGRATED AND THE CONFIGS MUST BE TAKEN
    # FROM AIRFLOW VARIABLES
    return {
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        'f3.s3a.endpoint': args['s3_endpoint'],
        'spark.hadoop.fs.s3a.endpoint': args['s3_endpoint'],
        'spark.kerberos.access.hadoopFileSystem': args['s3_log_bucket'],
    }


@contextmanager
def spark_session(
    pipeline_name: str,
    vars_generic: dict,
    job_args: dict
) -> Iterator:
    """ Create Spark session with a pipeline name.

    The context manager enables hive on Spark Session level.
    :param pipeline_name: Pipeline name (Spark application name)
    :param job_args: dictionary containing job arguments.
    """
    spark_properties = create_spark_properties(vars_generic,job_args)
    spark = SparkSession.builder.appName(pipeline_name)
    for ck, cv in spark_properties.items():
        spark = spark.config(ck, cv)
    spark = spark.enableHiveSupport().getOrCreate()
    yield spark
