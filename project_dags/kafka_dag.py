from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ms',
    'start_date': datetime.combine(datetime.today(), datetime.min.time()),  # Start from today at midnight
    'depends_on_past': False,
    'email':'inshadanish17@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'kafka_dag',
    default_args=default_args,
    description='Load data from S3 to staging and then append to final table in PostgreSQL',
    schedule_interval='0 0 * * *',  # Schedule to run daily at 12 AM
    catchup=False
)

# Define SSH Operator to run spark-submit command on EMR cluster
run_spark_submit = SSHOperator(
    task_id='run_spark_submit',
    ssh_conn_id='my_ssh',  
    command="""
        spark-submit \
            --deploy-mode client \
            --master yarn \
            --conf spark.executor.memory=4g \
            --conf spark.executor.cores=2 \
            --conf spark.executor.instances=2 \
            --jars postgresql-42.7.3.jar \
            s3://ttn-de-bootcamp-2024-gold-us-east-1/insha.danish/scripts/kafka/fetch_and_load_data.py
    """,
    dag=dag,
)

# Define PostgresOperator to execute upsert_strikes.sql
upsert_strikes = PostgresOperator(
    task_id='upsert_strikes',
    postgres_conn_id='my_postgres',
    sql='kafka_dag_files/upsert_strikes.sql',
    dag=dag,
)

scd_upsert = PostgresOperator(
    task_id='scd_upsert',
    postgres_conn_id='my_postgres',
    sql='kafka_dag_files/scd_upsert.sql',
    dag=dag,
)

scd_update = PostgresOperator(
    task_id='scd_upsert',
    postgres_conn_id='my_postgres',
    sql='kafka_dag_files/scd_update.sql',
    dag=dag,
)

cooldown = PostgresOperator(
    task_id='cooldown',
    postgres_conn_id='my_postgres',
    sql='kafka_dag_files/cooldown.sql',
    dag=dag,
)

update_tf = PostgresOperator(
    task_id='update_tf',
    postgres_conn_id='my_postgres',
    sql='kafka_dag_files/update_timeframe.sql',
    dag=dag,
)


# Set task dependencies
run_spark_submit >> upsert_strikes >> scd_upsert >> scd_update >> cooldown >> update_tf
