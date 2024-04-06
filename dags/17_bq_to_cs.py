from builtins import range
from datetime import timedelta,datetime
import pandas as pd
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable


from airflow.utils.dates import days_ago
# Load email from config.yaml
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCheckOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
)

from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator
)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import(
    GCSToBigQueryOperator
)


'''

    gcp_connection_2:  In Airflow, Admin --> Connections 
        - conn id: gcp_connection_2
        - conn type: google_cloud_platform
        - Keyfile JSON: Service Account's JSON	
'''


DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'test_dataset_2')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'demos1')
PROJECT_NAME = os.environ.get("GCP_PROJECT_NAME", 'project_name')
BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME",'airflow_sdbox_j')
CONNECTION_ID = os.environ.get("CONNECTION_ID_VARIABLE",'gcp_connection_2')


default_args = {
    'owner':'Airflow',                             #User who owns the DAG
    'start_date': datetime(2024,3,24, 10,50,00),   #date and time when the task should be scheduled to run for the first time.
    'catchup' : False,                             #Controls backfilling behavior. When set to False (default in your example), Airflow will only schedule the task for future execution dates, not past dates.
}


with DAG(dag_id='17_bq_to_cs',
    default_args=default_args,
    schedule_interval='@once') as dag: 

    start=DummyOperator(task_id='start')

    #https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/bigquery/index.html#airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator
    query_bq_data = BigQueryExecuteQueryOperator(
        task_id="query_bq_data",
        gcp_conn_id=f"{CONNECTION_ID}",
        sql="SELECT *  FROM `bigquery-public-data.google_trends.top_terms`",
        use_legacy_sql=False, 
    )
    
    #https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/transfers/bigquery_to_gcs/index.html#module-airflow.providers.google.cloud.transfers.bigquery_to_gcs
    bq_to_gcs_task = BigQueryToGCSOperator(
        task_id="bq_to_gcs_task",
        project_id=f"{PROJECT_NAME}",
        gcp_conn_id=f"{CONNECTION_ID}", 
        source_project_dataset_table="jrjames83-1171.sampledata.stock_prices",
        destination_cloud_storage_uris=[f"gs://{BUCKET_NAME}/file.csv"],  # List of GCS URIs
        field_delimiter=';',
    )
    
    #https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/transfers/gcs_to_bigquery/index.html#airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator
    gcs_to_bq_task = GCSToBigQueryOperator(
        task_id="gcs_to_bq_task",
        gcp_conn_id=f"{CONNECTION_ID}",
        bucket=f"{BUCKET_NAME}",
        source_objects ="file.csv", #URI path
        destination_project_dataset_table=f"{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME}",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
        {'name': 'Date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'Close', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ],
        field_delimiter =";",
        skip_leading_rows=1
    )


start  >> query_bq_data >> bq_to_gcs_task >> gcs_to_bq_task