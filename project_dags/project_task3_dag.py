from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ms',
    'start_date': datetime.combine(datetime.today() , datetime.min.time()),
    'depends_on_past': False,
    'email':'inshadanish17@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'emp_leave_quota_and_calendar_dag',
    default_args=default_args,
    description='Load data from S3 to table in PostgreSQL',
    schedule_interval='@yearly',
    catchup=False
)


start = DummyOperator(task_id='task_start', dag=dag)

# Define SSH Operator to run spark-submit command on EMR cluster
run_spark_submit_leave_quota = SSHOperator(
    task_id='run_spark_submit_leave_quota',
    ssh_conn_id='my_ssh',
    command="""
    spark-submit \
        --deploy-mode client \
        --master yarn \
        --conf spark.executor.memory=4g \
        --conf spark.executor.cores=2 \
        --conf spark.executor.instances=2 \
        --jars postgresql-42.7.3.jar \
        s3://ttn-de-bootcamp-2024-gold-us-east-1/insha.danish/scripts/emp_leave_quota_and_calendar_dag/fetch_and_load_leave_quota_data.py
    """,
    conn_timeout=600,
    cmd_timeout=600,
    dag=dag,
)

# Define SSH Operator to run spark-submit command on EMR cluster
run_spark_submit_calendar = SSHOperator(
    task_id='run_spark_submit_calendar',
    ssh_conn_id='my_ssh',
    command="""
    spark-submit \
        --deploy-mode client \
        --master yarn \
        --conf spark.executor.memory=4g \
        --conf spark.executor.cores=2 \
        --conf spark.executor.instances=2 \
        --jars postgresql-42.7.3.jar \
        s3://ttn-de-bootcamp-2024-gold-us-east-1/insha.danish/scripts/emp_leave_quota_and_calendar_dag/fetch_and_load_calendar_data.py
    """,
    conn_timeout=600,
    cmd_timeout=600,
    dag=dag,
)


dummy_operator = DummyOperator(task_id='task_complete', dag=dag)

start >> [run_spark_submit_leave_quota, run_spark_submit_calendar] >> dummy_operator

