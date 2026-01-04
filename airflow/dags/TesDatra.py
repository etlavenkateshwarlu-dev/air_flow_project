from datetime import datetime ,timedelta
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
default_args= {
'retries':3,
'retry_delay':timedelta(minutes=5)
}
dag=DAG(
'GCStoBigQuerryOperatorDattadag',
default_args=default_args,
start_date=datetime(2026,1,4),
schedule_interval = None,
catchup = False
)
start_task=EmptyOperator(task_id='started_task',dag=dag)
create_dataset_task=BigQueryCreateEmptyDatasetOperator(
task_id='create_dataset_task',
project_id='dsp-b45',
dataset_id='airflow_emp_dataset',
location='US',
exists_ok= True,
dag=dag
)
gcs_to_bigdata_data_load=GCSToBigQueryOperator(
task_id='gcs_to_bigdata',
bucket='xyzbucket_uniquename_123',
source_objects=["employ.csv"],
destination_project_dataset_table='dsp-b45.airflow_emp_dataset.small_emp',
source_format="CSV",
skip_leading_rows=1,
gcp_conn_id='google_cloud_default',
schema_fields=[
        {'name': 'empid', 'type': 'INT64', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'dep', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
dag=dag
)
end_task=EmptyOperator(task_id='end_task',dag=dag)
start_task>>create_dataset_task>>gcs_to_bigdata_data_load>>end_task