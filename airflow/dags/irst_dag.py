from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="simple_3_task_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    description="A simple DAG with 3 sequential tasks",
) as dag:

    task_1 = BashOperator(
        task_id="task_1_start",
        bash_command="echo 'Task 1: Start'"
    )

    task_2 = BashOperator(
        task_id="task_2_process",
        bash_command="echo 'Task 2: Processing'"
    )

    task_3 = BashOperator(
        task_id="task_3_end",
        bash_command="echo 'Task 3: End'"
    )

    task_1 >> task_2 >> task_3
