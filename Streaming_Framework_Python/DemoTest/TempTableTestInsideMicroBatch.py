from pyspark.sql import SparkSession
from pyspark.sql.functions import max
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages mysql:mysql-connector-java:8.0.33,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

# Initialize Spark Session
spark = SparkSession.builder.appName("StreamingWithTempTable").getOrCreate()

# Read streaming data (Example: from Kafka)
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Account_MonetaryAmount_BKK_DbtshpDbkacp_raw_avro") \
    .option('startingOffsets', 'earliest')\
    .load()

# Convert binary Kafka values to string
from pyspark.sql.functions import col, expr

streaming_df = streaming_df.selectExpr("CAST(value AS STRING)")

def process_batch(df, batch_id):
    print(f"Processing batch {batch_id}...")

    # ✅ Get the SparkSession inside foreachBatch
    batch_spark = SparkSession.builder.getOrCreate()

    # ✅ Check if DataFrame is empty before proceeding
    if df.isEmpty():
        print(f"⚠️ Batch {batch_id} is empty. Skipping processing.")
        return  

    print("Schema of batch data:")
    df.printSchema()

    # ✅ Create temp view using the same Spark session
    df.createOrReplaceGlobalTempView("source_table")
    print("Temp table `source_table` created successfully!")

    # ✅ Print available tables to check if `source_table` exists
    tables = batch_spark.catalog.listTables()
    print("Available tables:", [table.name for table in tables])

    # ✅ Ensure using the same session to run SQL
    latest_df = batch_spark.sql("SELECT * FROM global_temp.source_table")

    print("Query executed successfully. Writing results...")

    # ✅ Write results to storage
    latest_df.write.mode("append").format("parquet").save("/path/to/output")

    print(f"Batch {batch_id} processing complete.")

# ✅ Apply foreachBatch to process the stream
query = streaming_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()