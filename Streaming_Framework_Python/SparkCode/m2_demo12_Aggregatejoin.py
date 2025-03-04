from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
	sparkSession = SparkSession\
		.builder\
		.master("local")\
		.appName("AgreegateJoin")\
		.getOrCreate()


	sparkSession.sparkContext.setLogLevel("ERROR")

	personal_details_schema = StructType([StructField('Customer_ID',StringType(), True),\
										  StructField('Gender', StringType(), True),\
										  StructField('Age', StringType(), True)])

	customerDF = sparkSession.read\
								.format("csv")\
								.option("header", "true")\
								.schema(personal_details_schema)\
								.load("/Users/shivammittal/desktop/deloitte/projects/sparkstreaming/datasets/customerDatasets/static_datasets/join_static_personal_details.csv")


	transaction_details_schema = StructType([StructField('Customer_ID', StringType(), True),\
											StructField('Transaction_Amount', StringType(), True),\
											StructField('Transaction_Rating', StringType(), True)])

	fileStreamDf = sparkSession.readStream\
								.option("header", "true")\
								.option("maxFilesPerTrigger", 1)\
								.schema(transaction_details_schema)\
								.csv("/Users/shivammittal/desktop/deloitte/projects/sparkstreaming/datasets/customerDatasets/streaming_datasets/join_streaming_transaction_details")

	joinedDF = customerDF.join(fileStreamDf, "Customer_ID")

	spending_per_gender = joinedDF.groupBy('Gender')\
									.agg({"Transaction_Amount":"avg"})

	def round_func(amount):
		return("%.2f" % amount)

	round_udf = udf(
					round_func,
					StringType()
					)


	spending_per_gender = spending_per_gender.withColumn(
														"Average_Transaction_Amount",
														round_udf("avg(Transaction_Amount)")
														)\
												.drop("avg(Transaction_Amount)")

	query = spending_per_gender\
			.writeStream\
			.outputMode('complete')\
			.format('console')\
			.start()\
			.awaitTermination()