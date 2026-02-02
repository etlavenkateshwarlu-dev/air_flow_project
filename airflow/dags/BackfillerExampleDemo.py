from airflow import DAG
import airflow
from datetime import timedelta, datetime
from datetime import  time
from airflow.operators.python import  PythonOperator
from airflow.operators.empty import  EmptyOperator

def gc_to_bq_load(ds,**kwargs):
    gcs_path = f"gs://sales-bucket/daily_sales/date={ds}/sales.csv"
    print(f"✅ Reading file from: {gcs_path}")
    print(f"✅ Loading into BigQuery partition for date: {ds}")

with DAG(
    dag_id='BackFileExampleDemo',
    start_date=datetime(2026,1,1),
    #schedule='@daily',
    #catchup=True,
    catchup=False,
    max_active_runs=2
) as dag:
    start_task=EmptyOperator(
        task_id='start_taskd'
    )
    load_task=PythonOperator(
        task_id='load_task',
        python_callable=gc_to_bq_load
    )
    end_task=EmptyOperator(
        task_id='end_task'
    )

start_task >> load_task >> end_task