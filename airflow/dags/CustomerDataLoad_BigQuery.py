from datetime import  datetime,timedelta
from airflow import DAG
import airflow
import traceback
from  airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

email=['vte558@gmail.com','etla.venkateshwarlu@gamil.com']
PROJECT_ID='vctbatch-dev45'

def getBigQueryConnection():
    try:
        client = bigquery.Client(PROJECT_ID)
        print(f'#########getBigQueryConnection obj######....{client}')
    except Exception as e:
        e.with_traceback()
        print(f'Exception--->{e}')
        #traceback.print_stack()


default_arguments={
    'start_date': airflow.utils.dates.days_ago(0),
    'retry': 3,
    'retry_delay': timedelta(minutes=2)
}
dag = DAG(
    'CustomerDataLoadBigQuery',
    default_args=default_arguments,
    schedule_interval='5 * * * *'
)

start_task = EmptyOperator(
    task_id='start_task',
    dag=dag
)

bigquery_connect_task = PythonOperator(
    task_id='bigquery_connect_task',
    python_callable=getBigQueryConnection,
    dag=dag
)
create_bucket_task = BashOperator(
    task_id='create_bucket_task',
    bash_command='gsutil mb gs://airflow/source'
)
create_dataset_task = BashOperator(
    task_id='create_dataset_task',
    bash_command='bq mk -d vctbatch-dev45.airflow_cust_dataset '
)


end_task = EmptyOperator(
    task_id='end_task',
    dag=dag
)


start_task >> create_bucket_task >> create_dataset_task >> end_task






