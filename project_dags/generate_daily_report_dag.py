from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'ms',
    'start_date': datetime.combine(datetime.today(), datetime.min.time()),
    'depends_on_past': False,
    'email': 'inshadanish17@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'generate_daily_report_dag',
    default_args=default_args,
    description='Generate daily active employee report',
    schedule_interval='0 7 * * *',  # Schedule to run daily at 7 AM
    catchup=False
)

start_operator = DummyOperator(
    task_id='start',
    dag=dag,
)

generate_daily_report_operator = PostgresOperator(
    task_id='generate_daily_active_employee_report',
    postgres_conn_id='my_postgres',
    sql='project_task2_files/generate_daily_report.sql',
    dag=dag,
)

end_operator = DummyOperator(
    task_id='end',
    dag=dag,
)


# Task sequence
start_operator >> generate_daily_report_operator >> end_operator