from collections.abc import Iterator
from contextlib import contextmanager
from pyspark.sql import SparkSession


def create_spark_properties(generic_args: dict, job_args: dict) -> dict:
    args = generic_args.copy()
    args.update(job_args)



    return {
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }


@contextmanager
def spark_session(
    pipeline_name: str,
    vars_generic: dict,
    job_args: dict
) -> Iterator:
    """
    
    
    
    
    """
    spark_properties = create_spark_properties(vars_generic,job_args)
    spark = SparkSession.builder.appName(pipeline_name)
    for ck, cv in spark_properties.items():
        spark = spark.config(ck, cv)
    spark = spark.enableHiveSupport().getOrCreate()
    yield spark
