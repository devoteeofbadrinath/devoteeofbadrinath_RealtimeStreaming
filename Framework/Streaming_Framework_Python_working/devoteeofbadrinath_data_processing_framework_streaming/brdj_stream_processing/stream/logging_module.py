"""Module contains reusable components logging purposes"""
import logging
import os.path
from datetime import datetime, timezone
from logging import Logger
from tempfile import mkdtemp
from pyspark.sql import SparkSession
from brdj_stream_processing.utils.hadoop import hadoop_upload_file
from .stream_types import LogParams


class HadoopHandler(logging.Handler):
    """
    Hadoop handler to save logs in S3 bucket.
    """
    def __init__(self, file_path: str, log_params: LogParams, spark: SparkSession) -> None:
        super().__init__()
        self.file_path = file_path
        self.log_params = log_params
        self.spark = spark

    def flush(self) -> None:
        """Function writes logs to s3 using hadoop s3 client"""
        path = self.log_params["log_path"] + "/{:%Y%m%d%H%M%S}.log".format(datetime.now(timezone.utc))
        with self.lock:
            hadoop_upload_file(self.spark, self.file_path, path)

    def emit(self, __):
             # This method is required to be implemented by the logging.Handler base class.
             # It is intentionally left empty because the actual logic for 
             # sending the log record to Hadoop is defined in the flush method.
             pass
             

class RunIDFilter(logging.Filter):
     """
     This is a filter which injects run_id into the log.
     """
     def __init__(self, run_id):
          super().__init__()
          self.run_id = run_id

     def filter(self, record):
          record.run_id = self.run_id
          return True
     

def configure_logger(log_params: LogParams, spark: SparkSession) -> Logger:
     """
     Configure application logger.

     :param log_params: Logging module parameters.
     """
     log_level = logging.getLevelName(log_params["log_level"])
     formatter = logging.Formatter(
          '%(levelname)s:%(name)s:%(message)s:%(asctime)s:%(run_id)s',
          datefmt='%Y-%m-%d %H:%M:%S'
     )
     run_id_filter = RunIDFilter(log_params["run_id"])

     stream_handler = logging.StreamHandler()
     stream_handler.setFormatter(formatter)
     stream_handler.addFilter(run_id_filter)

     root_logger = logging.getLogger()
     root_logger.setLevel(log_level)
     root_logger.addHandler(stream_handler)

     if log_params["log_path"]:
          file_path = os.path.join(mkdtemp(), log_params["log_path"].split('/')[-1])
          hadoop_handler = HadoopHandler(file_path, log_params, spark)
          file_handler = logging.FileHandler(file_path)
          file_handler.setFormatter(formatter)
          file_handler.addFilter(run_id_filter)
          root_logger.addHandler(hadoop_handler)
          root_logger.addHandler(file_handler)

     return root_logger

