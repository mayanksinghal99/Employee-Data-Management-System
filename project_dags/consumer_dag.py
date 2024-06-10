from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator

consumerStep = [
    {
        'Name': 'Consumer Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'client',
                '--packages',
                'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
                's3://ttn-de-bootcamp-2024-gold-us-east-1/insha.danish/scripts/kafka/consumer.py'
            ],
        },
    }
]


CLUSTER_ID = 'j-20V1XKOG8FDOD'

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

# Define the DAG
dag = DAG(
    'kafka_consumer_dag',
    default_args=default_args,
    description='Run consumer',
    schedule_interval='0 0 * * *',  # Schedule to run daily at 12 AM
    catchup=False
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Add the step to the EMR cluster
consumer_start = EmrAddStepsOperator(
    task_id='consumer_start',
    job_flow_id=CLUSTER_ID,
    steps=consumerStep,
    dag=dag,
)

end = DummyOperator(
        task_id='end',
        dag=dag,
    )

start >> consumer_start >> end