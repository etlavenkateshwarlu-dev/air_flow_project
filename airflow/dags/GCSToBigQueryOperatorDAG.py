from datetime import  datetime,timedelta
from airflow import  DAG
import json
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
SCHEMA_BUCKET = "vct-dev45-schemas"
SCHEMA_OBJECT = "employe_schema.json"
SCHEMA_BUCKET = "vct-dev45-schemas"
SCHEMA_OBJECT = "employe_schema.json"


def read_schema_from_gcs(bucket_name, schema_path):
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    schema_content = hook.download(
        bucket_name=bucket_name,
        object_name=schema_path
    )
    return json.loads(schema_content)

employee_schema = read_schema_from_gcs(
        SCHEMA_BUCKET,
        SCHEMA_OBJECT
    )

default_arguments={
    'retries': 0,
    'retry_delay': timedelta(minutes=5),

}
dag=DAG(
    'GCSToBigQueryOperatorDemoDAG',
    default_args=default_arguments,
    start_date=datetime(2026, 1, 4),
    catchup=False,
    schedule_interval='20 * * * *',


)
start_task = EmptyOperator(task_id="start_task",dag=dag)

file_check_sensor_task=GCSObjectsWithPrefixExistenceSensor(
    task_id="wait_for_employee_file",
    bucket='vctbatch45-dev-custdata-bucket-new',
    prefix="landing_zone/employee_1",
    poke_interval=30,
    timeout=60,
    #gcp_conn_id="google_cloud_default"
    mode = "reschedule",
    google_cloud_conn_id = "google_cloud_default"
)
create_dataset_task=BigQueryCreateEmptyDatasetOperator(
    task_id='create_dataset_task',
    project_id='vctbatch-dev45',
    dataset_id='airflow_emp_dataset',
    location='US',
    exists_ok= True,
    dag=dag
)
gcs_to_bigdata_data_load=GCSToBigQueryOperator(
    task_id='gcs_to_bigdata_data_load',
    #gs://vctbatch45-dev-custdata-bucket-new/landing_zone/employee_1.csv
    bucket='vctbatch45-dev-custdata-bucket-new',
    source_objects=["landing_zone/employee_1.csv"],
    destination_project_dataset_table='vctbatch-dev45.airflow_emp_dataset.emp',
    source_format="CSV",
    skip_leading_rows=1,
    gcp_conn_id='google_cloud_default',
    schema_fields=employee_schema,
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)



end_task = EmptyOperator(task_id="end_task",dag=dag)

start_task >> file_check_sensor_task >> create_dataset_task >> gcs_to_bigdata_data_load >>end_task