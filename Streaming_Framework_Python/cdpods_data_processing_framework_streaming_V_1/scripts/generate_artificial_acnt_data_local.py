import time
import random
import json
import argparse
import logging
import sys
import os
from datetime import datetime
from decimal import Decimal
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.types import StructType

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.0,mysql:mysql-connector-java:8.0.33,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def generate_faker_data(
        batch_size: int,
        spark: SparkSession,
        table: str,
        zk_url: str
) -> list:
    """
    Generate a list of fake records using Faker.
    
    :param batch_size: Number of fake records to generate.
    :param spark: SparkSession instance.
    :param table: HBase table name.
    :param zk_url: Zookeeper URL for HBase.
    :return: List of fake records.
    """
 
    acnt_df = spark.read.jdbc(url=zk_url, 
                         table=table, 
                         properties={"user": "root",
                                     "password": "Hari@@14@@09",
                                     "driver": "com.mysql.cj.jdbc.Driver"})

    # acnt_df1 = (
    #     spark.read.format('pheonix')
    #     .option('table', table)
    #     .option('zkurl', zk_url)
    #     .load()
    # )

    acnt_df.createOrReplaceTempView("acnt_df")
    acnt_sample_df = spark.sql("""select
                               `LDGR_BAL_AMT`,
                               `ACNT_ID_NUM` as acnt_id_num
                               from acnt_df""")
    acnt_count_df = spark.sql("select count(*) from acnt_df")
    acnt_id_num = (
        acnt_sample_df.select("acnt_id_num")
        .rdd.flatMap(lambda x: x).collect()
    )

    acnt_table_count = acnt_count_df.collect()[0][0]
    balance = Decimal(acnt_sample_df.collect()[0][0])

    fake = Faker()
    data = []
    reference_number = fake.random_number(digits=10)
    for i in range(batch_size):
        if batch_size > acnt_table_count:
            raise ValueError("Batch size is too large for the available data")
        
        record = {
            "MessageHeader": {
                "Timestamp": int(datetime.now().timestamp() * 1000),
                "BusCorID": str(fake.random_number(digits=16)),
                "LocRefNum": str(fake.random_number(digits=16)),
                "RetAdd": fake.address(),
                "SeqNum": str(fake.random_number(digits=3)),
                "ReqRef": f"REQ{reference_number}",
                "OriSou": "BKK",
                "EventAction": "CREATE"
            },
            "MessageBody": {
                "AccArr": {
                    "AccNum": str(acnt_id_num[i].split("^")[-2]),
                    "Balance": balance + Decimal(0.0000001),
                    "BalanceStatus": random.choice(["SHADOW","POSTED"])
                },
                "Branch": {
                    "NSC": str(acnt_id_num[i].split("^")[-1])
                }
            }
        }
        data.append(record)
        
    return data


def read_from_schemas(
        spark_schema_file_path: str,
        avro_schema_file_path: str
) -> tuple:
    """
    Read schema files and return Spark and Avro schemas.
    
    :param spark_schema_file_path: Path to the Spark schema file.
    :param avro_schema_file_path: Path to the Avro schema file.
    :return: Tuple containing spark schema and Avro schema string.
    """
    with open(spark_schema_file_path, 'r') as f:
        spark_schema_json = json.load(f)
    with open(avro_schema_file_path,'r') as file:
        avro_schema_json = json.load(file)

    avro_schema_string = json.dumps(avro_schema_json)
    spark_schema = StructType.fromJson(spark_schema_json)

    return spark_schema, avro_schema_string


def main(json_config):
    """
    Main function to run the Kafka producer script.
    
    :param json_config: JSON configuration file passed by the user.
    """
    # Load configuration
    config = json.loads(json_config)
    kafka_topic_name = config.get('kafka_topic_name')
    batch_size = config.get('batch_size')
    batch_interval = config.get('batch_interval')
    stopping_timeout = config.get('stopping_timeout')
    input_table = config.get('input_table')
    num_partition = config.get('num_partition')
    zk_url = config.get('zk_url')
    kafka_bootstrap_servers = config.get('kafka_bootstrap_servers')
    spark_schema_name = config.get('spark_schema_name')
    avro_schema_name = config.get('avro_schema_name')
    user_id = config.get('user_id')
    workload_password = config.get('workload_password')
    cde_resource_name = config.get('cde_resource_name')

    # Create a Spark session
    spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()
    
    # Define schema paths
    spark_schema_path = f"/Users/shivammittal/Desktop/Deloitte/BOI_ODS_Python/cdpods_data_processing_framework_streaming/ods_stream_processing/schema/spark/{spark_schema_name}"
    avro_schema_path = f"/Users/shivammittal/Desktop/Deloitte/BOI_ODS_Python/cdpods_data_processing_framework_streaming/ods_stream_processing/schema/avro/{avro_schema_name}"

    # Read schemas
    spark_schema, avro_schema = read_from_schemas(
        spark_schema_path, avro_schema_path
        )
    
    # Partition list for distributiong records
    partition_list = [i for i in range(num_partition)]

    # Function to generate and write data
    def generate_and_write_data():
        data = generate_faker_data(batch_size, spark, input_table, zk_url)
        df = spark.createDataFrame(data, spark_schema)
        # Distribute records across partitions using a random partition key
        partition_df = (df.withColumn("partition",
                                      F.lit(random.choice(partition_list))
                                      .cast("string")))
        
        # Serialize DataFrame to Avro format
        data_df = (
            partition_df.select(
                to_avro(
                    F.struct("MessageHeader","MessageBody"),
                    avro_schema
                ).alias("value"),
                F.col("partition").alias("key")
            )
        )
        # Write data to the Kafka topic
        (
            data_df.write.format("kafka")
            .option('kafka.bootstrap.servers', kafka_bootstrap_servers)
            .option('topic', kafka_topic_name)
            #.option("kafka.security.protocol","SASL_SSL")
            #.option("kafka.sasl.mechanism","PLAIN")
            #.option("kafka.sasl.jass.config",
            #        f'org.apache.kafka.common.security.plain.PlainLoginModule \
            #            required username="{user_id}" \
            #                password="{workload_password}";')
            .save()
        )

    # Start the streaming process
    counter = 0
    start_time = time.time()
    while (time.time() - start_time) < stopping_timeout:
        logger.info("Processing batch %s", counter)
        generate_and_write_data()
        counter += 1
        time.sleep(stopping_timeout)

    # Stop the Spark session after timeout
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("json_config",
                        help = "User input to trigger Kafka Producer script")
    args = parser.parse_args()
    main(args.json_config)
    