from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, count, max, round, lit, coalesce
import boto3
import os
import sys

# Fetch the arguments from environment variables
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_DATABASE')


def fetch_and_load_data():
    print("Starting Spark session...")
    spark = SparkSession.builder \
        .appName("Airflow-Spark-Integration") \
        .getOrCreate()

    emp_messages_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
        .option("dbtable", "employee_messages") \
        .option("user", user) \
        .option("password", password) \
        .load() 
    
    # Filter messages for the current day
    today_messages_df = emp_messages_df.filter((col("msg_timestamp").cast("date") == current_date()) & (col("is_flagged") == True))

    # Aggregate strikes by employee
    stg_strikes_df = today_messages_df.groupBy("sender_id") \
        .agg(count("*").alias("strike_count"), max("msg_timestamp").alias("last_strike_date")) \
        .withColumnRenamed("sender_id", "employee_id")
   
    # Read salary information from timeframe table where end_date is null
    timeframe_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
        .option("dbtable", "emp_timeframe_final_table") \
        .option("user", user) \
        .option("password", password) \
        .load() 
    
    active_timeframe_df = timeframe_df.filter(col("end_date").isNull())

    # Merge salary information with strikes
    staging_df = stg_strikes_df.join(
        active_timeframe_df, stg_strikes_df["employee_id"] == active_timeframe_df["emp_id"], "inner"
    ).select(
        stg_strikes_df.employee_id,
        stg_strikes_df.strike_count,
        stg_strikes_df.last_strike_date,
        round(active_timeframe_df.salary, 2).alias("salary")
    )

    # Write to staging table
    staging_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
            .option("dbtable", "employee_messages_staging") \
            .option("user", user) \
            .option("password", password) \
            .mode("overwrite") \
            .save()


if __name__ == "__main__":
    fetch_and_load_data()
