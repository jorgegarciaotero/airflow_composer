import airflow
import logging
from datetime import timedelta,datetime
import os
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator,
)

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook



#world_port_index
ORG_PROJECT_NAME = os.environ.get("GCP_PROJECT_NAME", 'jorgegotero')
ORG_DATASET_NAME= os.environ.get("GCP_DATASET_NAME", 'worldpop')
ORG_TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'population_grid_1km')

DEST_PROJECT_NAME = os.environ.get("GCP_PROJECT_NAME", 'jorgegotero')
DEST_DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'worldpop2')
DEST_TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'popul')

BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME",'airflow_sdbox_j')
BUCKET_FILE = os.environ.get("BUCKET_FILE",'port_data.csv')
CONNECTION_ID_BQ = os.environ.get("CONNECTION_ID_VARIABLE",'gcp_connection_2')
CONNECTION_ID_DP = os.environ.get("CONNECTION_ID_VARIABLE",'gcp_connection_1')
#https://stackoverflow.com/questions/58456094/how-to-run-a-bigquery-query-and-then-send-the-output-csv-to-google-cloud-storage

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,4,24, 10,50,00),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 300,
    'catchup': False
}


with DAG(dag_id='20_bq_copy_table',
    default_args=default_args,
    schedule_interval='@once') as dag: 
    
    start=DummyOperator(task_id='01_start')
    
    #Create an Empty Dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    gcp_conn_id=f"{CONNECTION_ID_BQ}",
    dataset_id=f"{DEST_DATASET_NAME}",
    location="us-central1",
    )
    
    #Create a Table
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        gcp_conn_id=f"{CONNECTION_ID_BQ}",
        project_id=f"{DEST_PROJECT_NAME}",
        dataset_id=f"{DEST_DATASET_NAME}",
        table_id=f"{DEST_TABLE_NAME}",
        gcs_schema_object="gs://airflow_sdbox_j/schemas/schema_worldpop.json",
        google_cloud_storage_conn_id=f"{CONNECTION_ID_BQ}",
    )
 
    copy_bq_data = BigQueryExecuteQueryOperator(
        task_id="query_bq_data",
        gcp_conn_id=f"{CONNECTION_ID_BQ}",
        sql="""
        SELECT 
            country_name, geo_id, population, longitude_centroid, latitude_centroid, alpha_3_code, geog, last_updated
        FROM 
            `%s` 
        WHERE 
            country_name = 'Spain'
        LIMIT 100
        """%f"{ORG_PROJECT_NAME}.{ORG_DATASET_NAME}.{ORG_TABLE_NAME}",
        use_legacy_sql=False,
        destination_dataset_table=f"{DEST_PROJECT_NAME}.{DEST_DATASET_NAME}.{DEST_TABLE_NAME}", 
        write_disposition='WRITE_TRUNCATE',  
    )
    


 # Set dependencies between tasks
start >> create_dataset >> create_table >> copy_bq_data 