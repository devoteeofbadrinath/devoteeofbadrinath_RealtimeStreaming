import json
import time
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os 

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages mysql:mysql-connector-java:8.0.33,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

def sparkSessionTest(spark: SparkSession):
    
    static_schema = StructType([StructField('ACNT_ID_NUM', StringType(), True),
                         StructField('SHDW_BAL_AMT', StringType(), True),
                         StructField('SHDW_BAL_DTTM', StringType(), True),
                         StructField('SHDW_BAL_SEQ', StringType(), True),
                         StructField('LDGR_BAL_AMT', StringType(), True),
                         StructField('LDGR_BAL_DTTM', StringType(), True),
                         StructField('LDGR_BAL_SEQ', StringType(), True)]
                         )
    
    static_data1 = spark.read\
                        .format("csv")\
                        .option("header", "true")\
                        .schema(static_schema)\
                        .load('/Users/shivammittal/Desktop/Deloitte/SparkStreaming/windowing-join-operations-apache-spark-databricks/final_code/demo-04/datasets/ods/ODS.ACNT.csv')
    
    static_data1.createTempView("static_data1")
    print("Narayan 2= ",spark.catalog.listTables())



def main():
    
    sparkSession = SparkSession.builder.appName('StreamingStaticJoin').getOrCreate()

    sparkSession.sparkContext.setLogLevel('ERROR')

    static_schema = StructType([StructField('ACNT_ID_NUM', StringType(), True),
                         StructField('SHDW_BAL_AMT', StringType(), True),
                         StructField('SHDW_BAL_DTTM', StringType(), True),
                         StructField('SHDW_BAL_SEQ', StringType(), True),
                         StructField('LDGR_BAL_AMT', StringType(), True),
                         StructField('LDGR_BAL_DTTM', StringType(), True),
                         StructField('LDGR_BAL_SEQ', StringType(), True)]
                         )
    
    static_data = sparkSession.read\
                        .format("csv")\
                        .option("header", "true")\
                        .schema(static_schema)\
                        .load('/Users/shivammittal/Desktop/Deloitte/SparkStreaming/windowing-join-operations-apache-spark-databricks/final_code/demo-04/datasets/ods/ODS.ACNT.csv')
                        
    mysql_url = "jdbc:mysql://localhost:3306/ods"
    mysql_properties = {
            "user": "root",
            "password": "Hari@@14@@09",
            "driver": "com.mysql.cj.jdbc.Driver"
            }


    table_name = "account"   
    mySQLDf = sparkSession.read.jdbc(url=mysql_url, table=table_name, properties=mysql_properties)
    
    mySQLDf.show()
    
    static_data.show(40,False)

    streaming_schema = StructType([StructField("ACNT_ID_NUM", StringType(), True),
                                   StructField("BALANCE", StringType(), True),
                                   StructField("BALANCE_STATUS", StringType(), True),
                                   StructField("SEQ_NUM", StringType(), True),
                                   StructField("TIMESTAMP", TimestampType(), True)])
    
    streamingDataDf = sparkSession.readStream\
                                    .format('kafka')\
                                    .option('kafka.bootstrap.servers','localhost:9092')\
                                    .option('subscribe', 'account')\
                                    .option('startingOffsets', 'earliest')\
                                    .load()
    
    streamingDataCastDf = streamingDataDf.selectExpr("CAST(value as STRING)")

    streamingData = streamingDataCastDf.select(from_json(col("value").cast("string"),streaming_schema))\
                                            .withColumnRenamed("from_json(CAST(value as String))", "data")

    streamingData = streamingData.select(col('data.*'))
    #streamingData.printSchema()


    #outer_join = static_data.join(streamingData)
    #outer_join.printSchema()

    #right_outer_join = static_data.join(streamingData, on=["ACNT_ID_NUM"], how="right_outer")
    #right_outer_join.printSchema()

    #left_outer_join = static_data.join(streamingData, on=["name"], how="left_outer")
    #left_outer_join.printSchema()

    left_outer_join = streamingData.join(static_data, on=["ACNT_ID_NUM"], how="left_outer")
    
    left_outer_join.createTempView("left_join_data")
    
    print("Narayan1 = ",sparkSession.catalog.listTables())

    left_join_data_df = sparkSession.sql("SELECT case when BALANCE_STATUS = 'POSTED' then BALANCE ELSE LDGR_BAL_AMT END as LDGR_BAL_AMT_NEW, \
                                                 case when BALANCE_STATUS = 'POSTED' then SEQ_NUM ELSE LDGR_BAL_SEQ END as LDGR_BAL_SEQ_NEW, \
                                                 case when BALANCE_STATUS = 'POSTED' then TIMESTAMP ELSE LDGR_BAL_DTTM END as LDGR_BAL_DTTM_NEW, \
                                                 case when BALANCE_STATUS = 'SHADOW' then BALANCE ELSE SHDW_BAL_AMT END as SHDW_BAL_AMT_NEW, \
                                                 case when BALANCE_STATUS = 'SHADOW' then SEQ_NUM ELSE SHDW_BAL_SEQ END as SHDW_BAL_SEQ_NEW, \
                                                 case when BALANCE_STATUS = 'SHADOW' then TIMESTAMP ELSE SHDW_BAL_DTTM END as SHDW_BAL_DTTM_NEW,* \
                                            FROM left_join_data \
									").withColumn("is_matched", (col("SHDW_BAL_AMT").isNotNull()).cast("boolean"))
                                    #.drop('SHDW_BAL_AMT','SHDW_BAL_DTTM','SHDW_BAL_SEQ','LDGR_BAL_AMT','LDGR_BAL_DTTM','LDGR_BAL_SEQ')
    
    unmatched_df = left_join_data_df.filter(col("is_matched") == False)\
                    .drop('SHDW_BAL_AMT','SHDW_BAL_DTTM','SHDW_BAL_SEQ','LDGR_BAL_AMT','LDGR_BAL_DTTM','LDGR_BAL_SEQ','LDGR_BAL_AMT_NEW','LDGR_BAL_SEQ_NEW','LDGR_BAL_DTTM_NEW','SHDW_BAL_AMT_NEW','SHDW_BAL_SEQ_NEW','SHDW_BAL_DTTM_NEW','is_matched')\
                    .selectExpr("ACNT_ID_NUM as key", "cast(concat(ACNT_ID_NUM,BALANCE,BALANCE_STATUS,SEQ_NUM,TIMESTAMP) as STRING) as value")
    
    matched_df = left_join_data_df.filter(col("is_matched") == True)

    #shivam = left_join_data_df.drop('SHDW_BAL_AMT','SHDW_BAL_DTTM','SHDW_BAL_SEQ','LDGR_BAL_AMT','LDGR_BAL_DTTM','LDGR_BAL_SEQ')

    #inner_join = static_data.join(streamingData, on=['ACNT_ID_NUM'], how="inner")

    #selected_join = static_data.join(streamingData, on=['ACNT_ID_NUM'], how='inner')\
    #                            .select("name", "director", "star", "score")

    # query_unmatched = unmatched_df.writeStream\
    #             .format('console')\
    #             .outputMode('append')\
    #             .option('truncate', False)\
    #             .option('numRows', 40)\
    #             .start()
    #             #\
    #             #.awaitTermination()  
    #             #  
    # 
    
    query_unmatched = unmatched_df.writeStream\
                                .format('kafka')\
                                .option('kafka.bootstrap.servers','localhost:9092')\
                                .option('topic', 'account_dlq')\
                                .outputMode('append')\
                                .option('checkpointLocation', '/Users/shivammittal/Desktop/Deloitte/SparkStreaming/windowing-join-operations-apache-spark-databricks/final_code/demo-04/datasets/ods/checkpoint/' )\
                                .start()
    sparkSessionTest(sparkSession)

    print("Narayan3 = ",sparkSession.catalog.listTables())


    
    query_matched = matched_df.writeStream\
                .format('console')\
                .outputMode('append')\
                .option('truncate', False)\
                .option('numRows', 40)\
                .start()
                #\
                #.awaitTermination()   
    
    query_unmatched.awaitTermination()
    query_matched.awaitTermination()
    #time.sleep(60)
    #query.stop()


if __name__ == '__main__':
    main()
