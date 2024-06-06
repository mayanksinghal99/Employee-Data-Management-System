from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ms',
    'start_date': datetime.combine(datetime.today(), datetime.min.time()),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'leave_quota_dag',
    default_args=default_args,
    description='leave_quota_usage',
    schedule_interval='0 0 1 * *',
    catchup=False
)

# Define the start task
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# SSH Operator to run spark-submit command on EMR cluster
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
        s3://ttn-de-bootcamp-2024-gold-us-east-1/insha.danish/scripts/leave_quota_dag/leave_quota_usage.py
    """,
    conn_timeout=600,
    cmd_timeout=600,
    dag=dag,
)

# Define the end task
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define the task sequence
start_task >> run_spark_submit >> end_task
