from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, date_format, dayofweek, rank
from pyspark.sql.types import DateType
from pyspark.sql.window import Window
from datetime import date, timedelta
import os
import sys

# Fetch the arguments from environment variables
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
database = os.getenv('DB_DATABASE')


def calculate_potential_leaves():

    # Start Spark session 
    spark = SparkSession.builder \
        .appName("CalculatePotentialLeaves") \
        .getOrCreate()

    # Read leave data
    df_leaves = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
        .option("dbtable", "leave_applications") \
        .option("user", user) \
        .option("password", password) \
        .load() \
        .filter(col("status") == 'ACTIVE')
    df_leaves.cache()

    # Read leave calendar data
    df_leave_calendar = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
        .option("dbtable", "leave_calendar_table") \
        .option("user", user) \
        .option("password", password) \
        .load()

    # Define the current year's time frame
    current_date = date.today()
    end_of_year = date(current_date.year, 12, 31)
    start_of_year = date(current_date.year, 1, 1)

    # Filter the DataFrame to include only holidays after the current date
    upcoming_holidays = df_leave_calendar.filter(col("date") > current_date)

    # Exclude holidays that fall on weekends (Saturday and Sunday)
    upcoming_weekday_holidays = upcoming_holidays.filter(~dayofweek(col("date")).isin([1, 7]))
    total_holidays = upcoming_weekday_holidays.count()

    # Calculate total working days, excluding holidays and weekends
    total_days_in_year = (end_of_year - start_of_year).days + 1
    date_range = [(start_of_year + timedelta(days=x)) for x in range(total_days_in_year)]
    date_df = spark.createDataFrame(date_range, DateType()).toDF("date")

    # Filter out weekends (Saturday and Sunday)
    weekends = date_df.filter(dayofweek(col("date")).isin([1, 7]))
    num_weekends = weekends.count()

    total_working_days = total_days_in_year - total_holidays - num_weekends

    # Calculate the leave threshold
    threshold_percentage = 0.08
    leave_threshold = total_working_days * threshold_percentage

    # Identify employees with potential leave exceeding the threshold
    potential_leaves = df_leaves.groupBy("emp_id") \
        .agg(countDistinct("date").alias("upcoming_leaves")) \
        .filter(col("upcoming_leaves") > leave_threshold) \
        .select("emp_id", "upcoming_leaves")

    # Write the result to PostgreSQL
    potential_leaves.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{port}/{database}") \
            .option("dbtable", "potential_leave_count") \
            .option("user", user) \
            .option("password", password) \
            .mode("overwrite") \
            .save()
    
    spark.stop()


if __name__ == "__main__":
    calculate_potential_leaves()
