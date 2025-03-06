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

sparkSession.sparkContext.setLogLevel('ERROR')



logger = logging.getLogger(__name__)
    
static_data = sparkSession.read\
                        .format("csv")\
                        .option("header", "true")\
                        .load('/Users/shivammittal/Desktop/Deloitte/BOI_ODS_Python/cdpods_data_processing_framework_streaming/Miscellaneous/SampleData/sample_account_data_corrected.csv')

static_data.printSchema()

mysql_properties = {
        "user": "root",
        "password": "Hari@@14@@09",

        "driver": "com.mysql.cj.jdbc.Driver"
    }

static_data.write.jdbc(url="jdbc:mysql://localhost:3306/ODS_USECASE_REFERENCE_FILES",
                    table = "ACNT",
                    mode = "append",
                    properties=mysql_properties)


