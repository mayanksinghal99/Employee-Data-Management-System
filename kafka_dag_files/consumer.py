from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, expr, udf, coalesce, lit, count, when, window
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, LongType
import json
import boto3
import os
import sys
from pyspark.sql.utils import *


bootstrap_servers = '54.226.107.7:9092'

# Fetch the arguments from environment variables
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_DATABASE')

# Initialize Spark session
spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
    .config("spark.sql.shuffle.partitions", 8) \
    .getOrCreate()

# PostgreSQL connection parameters
jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
jdbc_properties = {
    "user": user,
    "password": password,
    "driver": "org.postgresql.Driver"
}

s3_bucket = 'ttn-de-bootcamp-2024-bronze-us-east-1'
marked_word_key = 'insha.danish/data/message_data/marked_word.json'
vocab_key = 'insha.danish/data/message_data/vocab.json'


# Function to load words from S3
def load_words_from_s3(bucket, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8')
    return set(json.loads(data))


def load_words():
    marked_words = load_words_from_s3(s3_bucket, marked_word_key)
    vocab_words = load_words_from_s3(s3_bucket, vocab_key)
    return marked_words.intersection(vocab_words)


flagged_words = list(load_words())


# Define UDF to flag messages
def is_flagged_udf(message):
    if message is None:
        return False
    words = set(message.split())
    return any(word in words for word in flagged_words)


is_flagged = udf(is_flagged_udf, BooleanType())


def main():
    # Define schema for incoming JSON messages
    schema = StructType([
        StructField("sender", StringType(), True),
        StructField("receiver", StringType(), True),
        StructField("message", StringType(), True)
    ])

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", "kaf_topic") \
        .option("failOnDataLoss", "false") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse the JSON messages
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json", "timestamp") \
        .select(from_json(col("json"), schema).alias("data"), col("timestamp").alias("kafka_timestamp")) \
        .select("data.*", "kafka_timestamp")
    # json_df.printSchema()

    # Process the DataFrame to match the required schema
    df = json_df.withColumn("sender_id", col("sender").cast(LongType())) \
        .withColumn("receiver_id", col("receiver").cast(LongType())) \
        .withColumn("msg_text", col("message").cast(StringType())) \
        .withColumn("msg_timestamp", col("kafka_timestamp").cast(TimestampType())) \
        .withColumn("is_flagged", is_flagged(col("msg_text")))
    df.printSchema()
    df = df.drop("sender", "receiver", "message", "kafka_timestamp")

    # Filter out rows where sender_id, receiver_id, or msg_text is null
    df = df.filter(df.sender_id.isNotNull() & df.receiver_id.isNotNull() & df.msg_text.isNotNull())

    # Set up the streaming query
    query = df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "checkpoint_dir") \
        .outputMode("append") \
        .trigger(processingTime='30 seconds') \
        .start()
    query.awaitTermination()


def process_batch(batch_df, batch_id):
    # batch_df.cache()
    print(batch_id)
    batch_df.show()

    batch_df.write.jdbc(url=jdbc_url, table="public.employee_messages", mode="append",
                            properties=jdbc_properties)


if __name__ == "__main__":
    main()