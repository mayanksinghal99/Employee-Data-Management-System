from airflow.operators.python import BranchPythonOperator
from airflow.models import XCom

def decide_which_path(**kwargs):
    ti = kwargs['ti']
    check_result = ti.xcom_pull(task_ids='check_file_exists', key='file_found')
    print(check_result)

    if check_result == 'yes':
        return 'run_spark_submit_fetch_load'
    else:
        return 'end'
