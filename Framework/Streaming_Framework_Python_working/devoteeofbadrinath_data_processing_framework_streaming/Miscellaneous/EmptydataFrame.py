import csv
import typing as tp
from collections.abc import Sequence, Iterable
import logging
from enum import auto, Enum
from operator import itemgetter, attrgetter
from dataclasses import dataclass
import dataclasses as dtc
import os
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Column, DataFrame, functions as F


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages mysql:mysql-connector-java:8.0.33,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


sparkSession = SparkSession.builder.appName('StreamingStaticJoin').getOrCreate()
#sparkSession = SparkSession.builder.config("spark.jars.packages","mysql:mysql-connector-java:8.0.33").appName('StreamingStaticJoin').getOrCreate()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("EmptyDataFrame").getOrCreate()

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create empty DataFrame
empty_df = spark.createDataFrame([], schema)

# Show schema and contents
empty_df.printSchema()
empty_df.show()