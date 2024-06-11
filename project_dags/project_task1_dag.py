from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from project_task1_files.check_and_push_to_xcom import check_and_push_to_xcom
from project_task1_files.decide_which_path import decide_which_path
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'ms',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email':'inshadanish17@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'emp_data_dag',
    default_args=default_args,
    description='Load data from S3 to staging and then append to final table in PostgreSQL',
    schedule_interval='0 7 * * *',
    catchup=False
)


check_file_exists_operator = PythonOperator(
    task_id='check_file_exists',
    python_callable=check_and_push_to_xcom,
    provide_context=True,
    dag=dag,
)

branching_operator = BranchPythonOperator(
    task_id='decide_which_path',
    python_callable=decide_which_path,
    provide_context=True,
    dag=dag,
)

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
        s3://ttn-de-bootcamp-2024-gold-us-east-1/insha.danish/scripts/emp_data_dag/fetch_and_load_data.py
    """,
    conn_timeout=600,
    cmd_timeout=600,
    dag=dag,
)

update_final_table_operator = PostgresOperator(
    task_id='update_final_table',
    postgres_conn_id='my_postgres',
    sql='project_task1_files/update.sql',
    dag=dag,
)

end_operator = DummyOperator(
    task_id='end',
    dag=dag,
)


# task sequence
check_file_exists_operator >> branching_operator
branching_operator >> run_spark_submit >> update_final_table_operator
branching_operator >> end_operator
