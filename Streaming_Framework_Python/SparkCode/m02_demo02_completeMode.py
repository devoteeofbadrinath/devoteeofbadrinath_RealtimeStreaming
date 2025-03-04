from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == "__main__":
	sparkSession = SparkSession.builder.master("local")\
							.appName("SparkStreaminCompleteMode")\
							.getOrCreate()

	sparkSession.sparkContext.setLogLevel("ERROR")

	schema = StructType([StructField("lsoa_code", StringType(), True),\
						 StructField("borough", StringType(), True),\
						 StructField("major_category", StringType(), True),\
						 StructField("minor_Category", StringType(), True),\
						 StructField("value", StringType(), True),\
						 StructField("year", StringType(), True),\
						 StructField("month", StringType(), True)])

	fileStreamDF = sparkSession.readStream\
							   .option("header", "true")\
							   .option("maxFilesPerTrigger", 1)\
							   .schema(schema)\
							   .csv("/Users/shivammittal/Desktop/Deloitte/projects/sparkstreaming/datasets/droplocation")

	print(" ")
	print("Is the stream ready")
	print(fileStreamDF.isStreaming)

	print(" ")
	print("Schema of the input stream: ")
	print( fileStreamDF.printSchema)

	recordsPerBorough = fileStreamDF.groupBy("borough")\
									.count()\
									.orderBy("count", ascending=False)


	query = recordsPerBorough.writeStream\
					 .outputMode("complete")\
					 .format("console")\
					 .option("truncate", "false")\
					 .option("numRows", 30)\
					 .start()\
					 .awaitTermination()