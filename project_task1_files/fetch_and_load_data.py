from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit , col
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

    print("Calculating date ranges...")
    today = datetime.now(timezone.utc)
    today_start = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=timezone.utc)
    today_end = datetime(today.year, today.month, today.day, 7, 0, 0, tzinfo=timezone.utc)

    print("Connecting to S3...")
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    keys = [obj['Key'] for obj in response.get('Contents', []) if today_start <= obj['LastModified'] <= today_end]

    print(f"Found keys: {keys}")

    files = [f"s3://{bucket_name}/{key}" for key in keys]
    print(f"Files to process: {files}")

    df = spark.read.option("Header", True).csv(files)
        
    # Cast the "age" column to integer
    df = df.withColumn("age", col("age").cast("integer"))
        
    # Cast the "emp_id" column to bigint
    df = df.withColumn("emp_id", col("emp_id").cast("bigint"))

    df = df.withColumn('processed_date', lit(today.date()))

    # Database connection properties
    url = f"jdbc:postgresql://{host}:{port}/{database}"
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    df.write.jdbc(url=url, table="emp_staging_table", mode="overwrite", properties=properties)


if __name__ == "__main__":
    bucket_name = 'ttn-de-bootcamp-2024-gold-us-east-1'
    prefix = 'insha.danish/data/emp_data/'
    fetch_and_load_data(bucket_name, prefix)
