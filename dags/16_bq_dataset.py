from builtins import range
from datetime import timedelta,datetime
import os
import pandas as pd
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDataOperator,
    BigQueryExecuteQueryOperator
)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import(
    GCSToBigQueryOperator
)

'''
Examples of Operators of Bigquery

gcp_connection_2:  In Airflow, Admin --> Connections 
    - conn id: gcp_connection_2
    - conn type: google_cloud_platform
    - Keyfile JSON: Service Account's JSON	
'''

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'dataset_example')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'table_example')
PROJECT_NAME = os.environ.get("GCP_PROJECT_NAME", 'jorgegotero')
BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME",'airflow_sdbox_j')
CONNECTION_ID = os.environ.get("CONNECTION_ID_VARIABLE",'gcp_connection_2')

default_args = {
    'owner':'Airflow',                             #User who owns the DAG
    'start_date': datetime(2024,3,24, 10,50,00),   #date and time when the task should be scheduled to run for the first time.
    'catchup' : False,                             #Controls backfilling behavior. When set to False (default in your example), Airflow will only schedule the task for future execution dates, not past dates.
}

with DAG(dag_id='16_bq_dataset',
    default_args=default_args,
    schedule_interval='@once') as dag: 
    start=DummyOperator(task_id='start')

    #Create an Empty Dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    gcp_conn_id=f"{CONNECTION_ID}",
    dataset_id="dataset_example",
    location="us-central1",
    )
    
    #Create a Table
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        gcp_conn_id=f"{CONNECTION_ID}",
        dataset_id=f"{DATASET_NAME}",
        table_id=f"{TABLE_NAME}",
        project_id=f"{PROJECT_NAME}",
        gcs_schema_object="gs://airflow_sdbox_j/schemas/schema_bq.json",
        google_cloud_storage_conn_id=f"{CONNECTION_ID}",
    )
    
    #Load File from GCS
    gcs_to_bq_task = GCSToBigQueryOperator(
        task_id="gcs_to_bq_task",
        gcp_conn_id=f"{CONNECTION_ID}", 
        bucket=f"{BUCKET_NAME}",
        source_objects ="file_example.csv", #URI path
        destination_project_dataset_table=f"{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME}",
        write_disposition="WRITE_TRUNCATE",
        #schema_object="gs://airflow_sdbox_j/schemas/schema_bq.json",
        schema_fields=[
            {
            "name": "qtr",
            "type": "STRING",
            "mode": "REQUIRED",
            "description": "quarter"
            },
            {
            "name": "rep",
            "type": "STRING",
            "mode": "NULLABLE",
            "description": "sales representative"
            },
            {
            "name": "sales",
            "type": "FLOAT",
            "mode": "NULLABLE",
            "defaultValueExpression": "2.55"
            }
        ],
        field_delimiter =",",
        skip_leading_rows=1
    )
    
    #Delete a Table
    delete_table = BigQueryDeleteDatasetOperator(
        task_id="delete_table",
        gcp_conn_id=f"{CONNECTION_ID}", 
        dataset_id=f"{DATASET_NAME}",
        project_id=f"{PROJECT_NAME}",
        delete_contents=True,  # Force the deletion of the dataset as well as its tables (if any).
    )

    get_data = BigQueryGetDataOperator(
            task_id="get_data",
            gcp_conn_id=f"{CONNECTION_ID}", 
            project_id=f"{PROJECT_NAME}",
            dataset_id=f"{DATASET_NAME}",
            table_id=f"{TABLE_NAME}",
            max_results=10,
            selected_fields="qtr,rep",
    )

    get_data_result = BashOperator(
            task_id="get_data_result",
            bash_command="echo resultado: \"{{ task_instance.xcom_pull('get_data') }}\"",
    )

start >> create_dataset >> create_table >> gcs_to_bq_task >> get_data   >> get_data_result