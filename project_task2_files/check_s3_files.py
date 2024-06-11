import boto3
from datetime import datetime, timedelta, timezone

def check_s3_files():
    bucket_name = 'ttn-de-bootcamp-2024-gold-us-east-1'
    prefix = 'insha.danish/data/emp_timeframe_data/'

    print("Checking S3 for files...")
    today = datetime.now(timezone.utc)
    today_start = datetime(today.year, today.month, today.day, 0, 0, 0, tzinfo=timezone.utc)
    today_end = datetime(today.year, today.month, today.day, 7, 0, 0, tzinfo=timezone.utc)
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    keys = [obj['Key'] for obj in response.get('Contents', []) if today_start <= obj['LastModified'] <= today_end]
    if keys:
        print("Files found within the time interval.")
        return True
    else:
        print("No files found within the time interval.")
        return False
