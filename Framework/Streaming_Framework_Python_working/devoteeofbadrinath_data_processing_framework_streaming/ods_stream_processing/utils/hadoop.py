import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def hadoop_move(spark: SparkSession, source: str, target: str) -> None:
    """
    Copy a file from source_path  to target_path in S3 and then delete the target file.
    
    :param spark: SparkSession object
    :param source: Source file path in S3
    :param target: Target file path in S3
    """

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop = spark.sparkContext._jvm.org.apache.hadoop
    futil = spark._sc.jvm.org.apache.hadoop.fs.FileUtil

    # TODO: Create function to pass the path string and return the FileSystem and the Path object.
    source_path = hadoop.fs.Path(source)
    fs_source = hadoop.fs.FileSystem.get(source_path.toUri(), hadoop_conf)

    target_path = hadoop.fs.Path(target)
    fs_target = hadoop.fs.FileSystem.get(target_path.toUri(), hadoop_conf)

    futil.copy(fs_source, source_path, fs_target, target_path, False, hadoop_conf)

    futil.fullyDelete(fs_source, source_path)


def hadoop_upload_file(spark: SparkSession, file_path: str, target: str) -> None:
    """
    Copy a local file from to target in Hadoop filesystem.
    
    :param spark: SparkSession object
    :param file_path: Path to local file
    :param target: Target in Hadoop filesystem
    """
    hadoop_conf = spark._jsc.hadoopConfiguration()
    s3_endpoint = spark.conf.get('spark.hadoop.f3.s3a.endpoint')
    logger.info("S3 Endpoint for log upload: %s", s3_endpoint)
    hadoop = spark.sparkContext._jvm.org.apache.hadoop
    futil = spark._sc._jvm.org.apache.hadoop.fs.FileUtil

    source_path = hadoop.fs.Path(f'file://{file_path}')
    fs_source = hadoop.fs.FileSystem.get(source_path.toUri(), hadoop_conf)
    
    target_path = hadoop.fs.Path(target)
    fs_target = hadoop.fs.FileSystem.get(target_path.toUri(), hadoop_conf)

    futil.copy(fs_source, source_path, fs_target, target_path, False, hadoop_conf)
