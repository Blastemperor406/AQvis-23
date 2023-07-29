import findspark
findspark.init()
import time

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, split, get_json_object # col -> Returns a Column based on the given column name.
from pyspark.sql.types import TimestampType, StructType, IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.functions import decode, regexp_extract, trim

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Kafka-Spark-1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
        .config("spark.cassandra.connection.host", "192.168.130.66") \
        .getOrCreate()
    
    kafka_broker = "localhost:9093"
    topic_name_kafka = "sensor_data" 

    def write_to_cass(df,id):
        df.write.format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("checkpointLocation", "checkpoint") \
            .options(table="aqvis_testing", keyspace="test") \
            .save()


    schema = StructType([ \
        StructField("pm1", IntegerType(), True),\
        StructField("pm2_5",IntegerType(), True), \
        StructField("pm10", IntegerType(), True), \
        StructField("timestamp_last", TimestampType(), True) \
        ])

    #Read data from Kafka as a streaming DataFrame
    kafka_df: DataFrame = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic_name_kafka) \
        .option("startingOffsets", "earliest") \
        .load()
    

    print(kafka_df.isStreaming, kafka_df.printSchema())
    
    kafka_df = kafka_df.withColumn('value',decode(kafka_df.value, "UTF-8"))
    kafka_df = kafka_df.withColumn('pm1', regexp_extract(col('value'), r'\\"pm1\\":(\d+)', 1)) \
                   .withColumn('pm2_5', regexp_extract(col('value'), r'\\"pm2_5\\":(\d+)', 1)) \
                   .withColumn('pm10', regexp_extract(col('value'), r'\\"pm10\\":(\d+)', 1)) \
                   .withColumn('timestamp_last', col('timestamp')) 
                  
    kafka_df = kafka_df.filter(
        col('timestamp_last').isNotNull() &
        col('pm1').isNotNull() & (col('pm1') != "") &
        col('pm2_5').isNotNull() & (col('pm2_5') != "") &
        col('pm10').isNotNull() & (col('pm10') != "")
    )
    cassandra_df = kafka_df.select("timestamp_last","pm1", "pm2_5", "pm10")
    

    #Write data to Cassandra
    cassandra_df.writeStream\
        .format("org.apache.spark.sql.cassandra") \
        .foreachBatch(write_to_cass)\
        .start().awaitTermination()

    #cassandra_df.writeStream.format("console").outputMode("append").start().awaitTermination()
          

