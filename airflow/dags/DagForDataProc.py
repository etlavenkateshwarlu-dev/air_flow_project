from airflow import  DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime,timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import  BashOperator
PROJECT_ID='vctbatch-45'
CLUSTER_NAME='vctbatch-45-dataproc-cluster'
REGION='us-central1'
PYSPARK_FILE='gs://vctbatch-45-scripts/GCSToBigQueryDataloadPipeline.py'

default_arguments={
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
dag=DAG(
    'DataProcSubmitDAG',
    default_args=default_arguments,
    start_date=datetime(2026, 1, 29),
    catchup=True,
    max_active_runs=10,
    schedule_interval='36 13 * * *'
)
start_task = EmptyOperator(task_id="start_task",dag=dag)
# submit_pyspark_job = DataprocSubmitJobOperator(
#         task_id="submit_pyspark_job_to_dataproc",
#         project_id=PROJECT_ID,
#         region=REGION,
#         job={
#             "placement": {
#                 "cluster_name": CLUSTER_NAME
#             },
#             "pyspark_job": {
#                 "main_python_file_uri": PYSPARK_FILE
#             }
#         }
#     )

submit_pyspark_job=BashOperator(
    task_id='submit_pyspark_job_to_dataproc',
    bash_command=f"""
    gcloud dataproc jobs submit pyspark gs://vctbatch-45-scripts/GCSToBigQueryDataloadPipeline.py \
    --cluster=vctbatch-45-dataproc-cluster \
    --region=us-central1
""",
    dag=dag
)

end_task = EmptyOperator(task_id="end_task",dag=dag)

start_task >> submit_pyspark_job >> end_task