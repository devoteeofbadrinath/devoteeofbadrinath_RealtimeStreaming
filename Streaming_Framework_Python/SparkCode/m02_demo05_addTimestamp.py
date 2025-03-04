from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import time
import datetime

if __name__ == "__main__":
	
	sparkSession = SparkSession.builder.master("local")\
								.appName("SparkStreamingAddTimestamp")\
								.getOrCreate()

	sparkSession.sparkContext.setLogLevel("ERROR")

	schema = StructType([StructField("lsoa_code", StringType(), True),\
						StructField("borough", StringType(), True),\
						StructField("major_category", StringType(), True),\
						StructField("minor_category", StringType(), True),\
						StructField("value", StringType(), True),\
						StructField("year", StringType(), True),\
						StructField("month", StringType(), True)])

	fileStreamDF = sparkSession.readStream\
								.option("header", "true")\
								.option("maxFilesPerTrigger", 2)\
								.schema(schema)\
								.csv("/Users/shivammittal/Desktop/Deloitte/projects/sparkstreaming/datasets/droplocation")


	def add_timestamp():
		ts = time.time()
		timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
		return timestamp

	add_timestamp_udf = udf(add_timestamp, StringType())

	fileStreamWithTS = fileStreamDF.withColumn("timestamp", add_timestamp_udf())

	trimmedDF = fileStreamWithTS.select("borough",
										"major_category",
										'value',
										"timestamp")

	query = trimmedDF.writeStream\
						.outputMode('append')\
						.format('console')\
						.option("truncate", "false")\
						.start()\
						.awaitTermination()

