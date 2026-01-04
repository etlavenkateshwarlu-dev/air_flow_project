from datetime import  datetime,timedelta
from airflow import DAG
import airflow
import traceback
from  airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google


from google.cloud import bigquery

email=['vte558@gmail.com','etla.venkateshwarlu@gamil.com']

def loadDateIntoBigQuery():
    print("Loading dato Big Query")

def copy_data_to_bucket():
    print("copy_data_to_bucket")


def check_file():
    file_exist=True
    if(file_exist):
        return 'load_data_bigquery_task'
    else:
        return 'copy_data_to_bucket_task'

default_arguments={
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}
dag = DAG(
    'PythonAndBranPythonDemo',
    default_args=default_arguments,
    schedule_interval='10 * * * *'
)


start_task = EmptyOperator(task_id='start_task',dag=dag)
branch_file_check_task = BranchPythonOperator(task_id='check_file',python_callable=check_file,dag=dag)
load_data_bigquery_task = PythonOperator(task_id='load_data_bigquery_task',python_callable=loadDateIntoBigQuery,dag=dag)

copy_data_to_bucket_task= PythonOperator(task_id='copy_data_to_bucket_task',python_callable=copy_data_to_bucket,dag=dag)

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag
)
# join_task = EmptyOperator(
#     task_id='join_task',
#     trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
#     dag=dag
# )


start_task >> branch_file_check_task
branch_file_check_task >> [ load_data_bigquery_task , copy_data_to_bucket_task] >> end_task
