from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import datetime

if __name__ == "__main__":
	sparkSession = SparkSession\
					.builder\
					.appName("WindowAggregation")\
					.getOrCreate()


	sparkSession.sparkContext.setLogLevel("ERROR")

	personal_details_schema = StructType([StructField('Customer_ID', StringType(), True),\
										StructField('Gender', StringType(), True),\
										StructField('Age', StringType(), True)])

	customerDF = sparkSession.read\
								.format("csv")\
								.option("header", "true")\
								.schema(personal_details_schema)\
								.load("/Users/shivammittal/Desktop/Deloitte/projects/sparkstreaming/datasets/customerDatasets/static_datasets/join_static_personal_details.csv")

	customerDF.show()
	
	transaction_details_schema = StructType([StructField('Customer_ID', StringType(), True),\
											StructField('Transaction_Amount', StringType(), True),\
											StructField('Transaction_Rating', StringType(), True)])

	fileStreamDf = sparkSession.readStream\
								.option("header", "true")\
								.option("maxFilesPerTrigger", 1)\
								.schema(transaction_details_schema)\
								.csv("/Users/shivammittal/Desktop/Deloitte/projects/sparkstreaming/datasets/customerDatasets/streaming_datasets/join_streaming_transaction_details")


	joinedDF = customerDF.join(fileStreamDf, "Customer_ID")

	
	def add_timestamp():
		ts = time.time()
		timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
		return timestamp

	add_timestamp_udf = udf(add_timestamp, StringType())
	df_with_timestamp = joinedDF.withColumn(
									"timestamp",
									add_timestamp_udf()
									)

	def add_age_group(age):
		if int(age)<25: return "Less than 25"
		elif int(age)<40: return "25 to 40"
		else: return "More than 40"


	add_age_group_udf = udf(add_age_group, StringType())

	df_with_agegroup = df_with_timestamp.withColumn("Age_Group",
													add_age_group_udf(joinedDF.Age)
													)

	

	windowed_transactions = df_with_agegroup.groupBy(
													window("timestamp","2 minutes","1 minutes"),
													df_with_agegroup.Age_Group
													)\
											.agg({"Transaction_Amount":"avg"})



	def round_func(amount):
		return ("%.2f" % amount)

	round_func_udf = udf(
						round_func,
						StringType()
						)

	windowed_transactions = windowed_transactions.withColumn("Average_Transaction_Amount",
															round_func_udf("avg(Transaction_Amount)")
															)\
												.drop("avg(Transaction_Amount)")

	query = windowed_transactions.writeStream\
									.outputMode('complete')\
									.format('console')\
									.start()\
									.awaitTermination()


