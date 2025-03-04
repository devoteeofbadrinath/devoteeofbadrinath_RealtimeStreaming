from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AvroTest") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:3.5.4") \
    .getOrCreate()

data = [("John", 30), ("Alice", 28)]
df = spark.createDataFrame(data, ["name", "age"])
df.write.format("avro").save("/tmp1/test.avro")

df_read = spark.read.format("avro").load("/tmp/test.avro")
df_read.show()