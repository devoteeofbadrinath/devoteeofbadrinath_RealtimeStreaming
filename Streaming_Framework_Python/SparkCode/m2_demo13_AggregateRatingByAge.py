from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

if __name__ == "__main__":
	sparkSession = SparkSession\
					.builder\
					.appName("AggregateRatingByAge")\
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

	transaction_details_schema = StructType([StructField('Customer_ID', StringType(), True),\
											StructField('Transaction_Amount', StringType(), True),\
											StructField('Transaction_Rating', StringType(), True)])


	fileStreamDf = sparkSession.readStream\
								.option("header", True)\
								.option("maxFilesPerTrigger", 1)\
								.schema(transaction_details_schema)\
								.csv("/Users/shivammittal/Desktop/Deloitte/projects/sparkstreaming/datasets/customerDatasets/streaming_datasets/join_streaming_transaction_details")

	joinedDF = customerDF.join(fileStreamDf, "Customer_ID")

	def add_age_group(age):
		if int(age) < 25 : return 'less than 25'
		elif int(age) < 40: return '25 to 40'
		else: return 'more than 40'

	add_age_group_udf = udf(
							add_age_group,
							StringType()
							)

	ratings_with_agegroup = joinedDF.withColumn(
												"Age_Group",
												add_age_group_udf(joinedDF.Age)
												)

	ratings_per_agegroup = ratings_with_agegroup.groupBy('Age_Group')\
												.agg({"Transaction_Rating":"avg"})




	def round_func(amount):
		return ("%.2f" % amount)

	round_udf = udf(
					round_func,
					StringType()
					)

	ratings_per_agegroup = ratings_per_agegroup.withColumn(
															"Average_Transaction_Rating",
															round_udf("avg(Transaction_Rating)")
															)\
														.drop("avg(Transaction_Rating)")


	query = ratings_per_agegroup\
				.writeStream\
				.outputMode('complete')\
				.format('console')\
				.start()\
				.awaitTermination()


