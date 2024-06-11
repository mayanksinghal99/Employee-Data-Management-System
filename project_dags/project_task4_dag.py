from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from project_task4_files.check_s3_files import check_s3_files

default_args = {
    'owner': 'ms',
    'start_date': datetime.combine(datetime.today(), datetime.min.time()),
    'depends_on_past': False,
    'email':'inshadanish17@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'emp_leave_data_dag',
    default_args=default_args,
    description='Load data from S3 to staging and then append to final table in PostgreSQL and calculate potential leaves',
    schedule_interval='0 7 * * *',
    catchup=False
)

start_operator = DummyOperator(
    task_id='start',
    dag=dag,
)

short_circuit_operator = ShortCircuitOperator(
    task_id='check_file_exists',
    python_callable=check_s3_files,
    provide_context=True,
    dag=dag,
)

# Define SSH Operator to run spark-submit command on EMR cluster
run_spark_submit_fetch_load = SSHOperator(
    task_id='run_spark_submit_fetch_load',
    ssh_conn_id='my_ssh',
    command="""
    spark-submit \
        --deploy-mode client \
        --master yarn \
        --conf spark.executor.memory=4g \
        --conf spark.executor.cores=2 \
        --conf spark.executor.instances=2 \
        --jars postgresql-42.7.3.jar \
        s3://ttn-de-bootcamp-2024-gold-us-east-1/insha.danish/scripts/emp_leave_data_dag/fetch_and_load_data.py
    """,
    conn_timeout=600,
    cmd_timeout=600,
    dag=dag,
)

update_final_table_operator = PostgresOperator(
    task_id='update_final_table',
    postgres_conn_id='my_postgres',
    sql='project_task4_files/update_final_table.sql',
    dag=dag,
)

# Define SSH Operator to run spark-submit command on EMR cluster
run_spark_submit_potential_leaves = SSHOperator(
    task_id='run_spark_submit_potential_leaves',
    ssh_conn_id='my_ssh',
    command="""
    spark-submit \
        --deploy-mode client \
        --master yarn \
        --conf spark.executor.memory=4g \
        --conf spark.executor.cores=2 \
        --conf spark.executor.instances=2 \
        --jars postgresql-42.7.3.jar \
        s3://ttn-de-bootcamp-2024-gold-us-east-1/insha.danish/scripts/emp_leave_data_dag/calculate_potential_leaves.py
    """,
    conn_timeout=600,
    cmd_timeout=600,
    dag=dag,
)

end_operator = DummyOperator(
    task_id='end',
    dag=dag,
)

#task sequence
start_operator >> short_circuit_operator >> run_spark_submit_fetch_load >> update_final_table_operator >> run_spark_submit_potential_leaves >> end_operator
