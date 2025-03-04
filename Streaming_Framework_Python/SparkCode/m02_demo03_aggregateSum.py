from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__=="__main__":
	sparkSession = SparkSession.builder.master("local")\
								.appName("SparkStreamingAggregate")\
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
								

	convictionsPerBorough = fileStreamDF.groupBy("borough")\
										.agg({"value": "sum"})\
										.withColumnRenamed("sum(value)", "convictions")\
										.orderBy("convictions", ascending=False)


	query = convictionsPerBorough.writeStream\
								.outputMode("complete")\
								.format("console")\
								.option("truncate", "false")\
								.option("numRows", 30)\
								.start()\
								.awaitTermination()

