from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count, expr, round
from datetime import date
import boto3
import os
import sys

# Fetch the arguments from environment variables
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_DATABASE')


def calculate_leave_quota_usage():

    # Start Spark session
    spark = SparkSession.builder \
        .appName("CalculateLeaveQuotaUsage") \
        .getOrCreate()

    # Read leave applications data
    df_leaves = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
        .option("dbtable", "leave_applications") \
        .option("user", user) \
        .option("password", password) \
        .load()
    df_leaves.cache()

    # Read leave quota data
    leave_quota_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
        .option("dbtable", "emp_leave_quota_table") \
        .option("user", user) \
        .option("password", password) \
        .load()
    leave_quota_df.persist(StorageLevel.MEMORY_AND_DISK)

    # Filter active leaves
    active_leaves_df = df_leaves.filter(col('status') == 'ACTIVE')

    # Calculate total leaves taken by each employee for the current year
    current_year = date.today().year
    total_leaves_taken_df = active_leaves_df.filter(year("date") == current_year) \
                                            .groupBy("emp_id") \
                                            .agg(count("*").alias("total_leaves_taken"))

    # Join total leaves taken with leave quota data
    leave_data_df = leave_quota_df.join(total_leaves_taken_df, ["emp_id"], "left")

    # Calculate percentage of leave quota used
    leave_data_df = leave_data_df.withColumn("percentage_used", expr("(total_leaves_taken / leave_quota) * 100")) \
                                 .filter(col("total_leaves_taken").isNotNull())

    # Round off percentage to 2 decimal places
    leave_data_df = leave_data_df.withColumn("percentage_used", round(col("percentage_used"), 2))

    # Identify employees whose availed leave quota exceeds 80%
    employees_to_notify_df = leave_data_df.filter(col("percentage_used") > 80)

    # Coalesce DataFrame to a single partition
    employees_to_notify_df = employees_to_notify_df.coalesce(1)

    # Define S3 details
    s3_bucket = "ttn-de-bootcamp-2024-gold-us-east-1"
    s3_file_path = f"insha.danish/report/employees_to_notify_{current_year}.csv"
    s3_output_path = f"s3://{s3_bucket}/{s3_file_path}"

    # Write the results to S3 as a CSV file
    employees_to_notify_df.select("emp_id", "total_leaves_taken", "leave_quota", "percentage_used") \
                          .write.csv(s3_output_path, header=True, mode="overwrite")

    print(f"Results successfully saved to {s3_output_path}")

    spark.stop()


if __name__ == "__main__":
    calculate_leave_quota_usage()