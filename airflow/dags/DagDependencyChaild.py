import time
from datetime import datetime,timedelta
from datetime import date
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

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
    'My_first_airflow_application',
    description='Test Dag Demo',
    schedule_interval=timedelta(days=5),
    default_args=default_arguments,
    start_date=datetime(2025, 1, 11),
    catchup=False,
    tags=['DEV']
)
start_task = DummyOperator(
    task_id='start_Task_chaild',
    dag=dag
)
bash_task1 = BashOperator(
    task_id='echo_task1_chaild',
    bash_command='echo Hello Welcome to Airflow',
    dag=dag
)
wait_for_parent_task=ExternalTaskSensor(
    task_id='wait_for_parent_tsk',
    external_dag_id='DAGDependencyParent',
    external_task_id='sleep_task',
    mode='poke',
    poke_interval=60,
    timeout=600,
    dag=dag
)
bash_task2 = BashOperator(
    task_id='echo_task2_chailed',
    bash_command='echo Hi How are you',
    dag=dag
)

end_task = DummyOperator(
    task_id='End_Task_chaild'
)

start_task >> bash_task1 >> wait_for_parent_task >>bash_task2 >> end_task