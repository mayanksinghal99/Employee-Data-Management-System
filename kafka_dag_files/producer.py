from pyspark.sql import SparkSession
import json
import boto3
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkProducer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()


def read_json_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    return json.loads(data)


def send_messages_spark(bucket_name, file_key, topic, sleep_time):
    data = read_json_from_s3(bucket_name, file_key)
    for entry in data:
        # entry["msg_id"] = str(uuid.uuid4())
        sender = entry.get("sender")
        receiver = entry.get("receiver")
        message = entry.get("message")

        json_entry = json.dumps(entry)

        # Create a DataFrame
        df = spark.createDataFrame([(json_entry,)], ["value"])

        # Write the DataFrame to Kafka
        df.selectExpr("CAST(value AS STRING)").write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", topic) \
            .save()

        print("Sent message:", entry)
        time.sleep(sleep_time)


if __name__ == "__main__":
    send_messages_spark('ttn-de-bootcamp-2024-gold-us-east-1', 'insha.danish/data/kafka_files/messages.json', 'kaf_topic', sleep_time=5)
