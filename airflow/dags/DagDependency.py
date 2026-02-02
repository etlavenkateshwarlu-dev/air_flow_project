import time
from datetime import datetime,timedelta
from datetime import date
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

email_list=['vte558@gmail.com','etla.venkateshwarlu@gmail.com']
default_arguments={
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': email_list,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}
def sleep_fun(**kwarg):
    time.sleep(60)

dag = DAG(
    'DAGDependencyParent',
    description='Test Dag Demo',
    schedule_interval=timedelta(days=5),
    default_args=default_arguments,
    start_date=datetime(2025, 1, 11),
    catchup=False,
    tags=['DEV']
)
start_task = DummyOperator(
    task_id='start_Task',
    dag=dag
)
bash_task1 = BashOperator(
    task_id='echo_task1',
    bash_command='echo Hello Welcome to Airflow',
    dag=dag
)
sleep_task=PythonOperator(
    task_id='sleep_task',
    python_callable=sleep_fun,
    provider_context=True,
    dag=dag
)
bash_task2 = BashOperator(
    task_id='echo_task2',
    bash_command='echo Hi How are you',
    dag=dag
)

end_task = DummyOperator(
    task_id='End_Task',
    dag=dag
)

start_task >> bash_task1 >> sleep_task >>bash_task2 >> end_task