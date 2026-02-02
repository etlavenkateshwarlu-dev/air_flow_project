from airflow import DAG
from datetime import timedelta,datetime
from airflow.operators.python import PythonOperator

def push_data(**context):
    context['it'].xcom_push(key='file_size_count',value=100)
    print("push .....")


def pull_data(**context):
    context['it'].xcom_pull('file_size_count')
    print("push .....")

dag=DAG(
    'XCOMExmapleDAG',
    schedule=None,
    star_date=datetime(2026,1,5)
)
task1=PythonOperator(
    task_id = 'push_date',
    python_callable =push_data

)