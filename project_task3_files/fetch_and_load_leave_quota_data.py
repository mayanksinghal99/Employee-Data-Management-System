from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import boto3
import os
import sys

# Fetch the arguments from environment variables
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_DATABASE')


def fetch_and_load_data(bucket_name, prefix):
    print("Starting Spark session...")
    spark = SparkSession.builder \
        .appName("Airflow-Spark-Integration") \
        .getOrCreate()

    print("Connecting to S3...")
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response.get('Contents', [])

    if objects:
        # Extract keys and last modified times
        keys_last_modified = [(obj['Key'], obj['LastModified']) for obj in objects]
        # Sort by last modified time in descending order
        keys_last_modified.sort(key=lambda x: x[1], reverse=True)
        latest_key, _ = keys_last_modified[0]

        file_path = f"s3://{bucket_name}/{latest_key}"
        print(f"Latest file to process: {file_path}")

        df = spark.read.option("Header", True).csv(file_path)

        df = df.withColumn("emp_id", col("emp_id").cast("bigint"))
        df = df.withColumn("leave_quota", col("leave_quota").cast("integer"))
        df = df.withColumn("year", col("year").cast("integer"))

        # Database connection properties
        url = f"jdbc:postgresql://{host}:{port}/{database}"
        properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver"
        }

        df.write.jdbc(url=url, table="emp_leave_quota_table", mode="append", properties=properties)

        # Indicate success
        return "yes"
    else:
        print("No files from today to process.")
        # Indicate no files to process
        return "no"


if __name__ == "__main__":
    bucket_name = 'ttn-de-bootcamp-2024-gold-us-east-1'
    prefix = 'insha.danish/data/emp_leave_quota/'
    result = fetch_and_load_data(bucket_name, prefix)
    sys.stdout.write(result)
