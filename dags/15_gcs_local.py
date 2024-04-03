from builtins import range
from datetime import timedelta,datetime
import pandas as pd
import yaml
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
# Load email from config.yaml
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryExecuteQueryOperator
)
#from airflow.models.variable import Variable


default_args = {
    'owner':'Airflow',                             #User who owns the DAG
    'start_date': datetime(2024,3,24, 10,50,00),   #date and time when the task should be scheduled to run for the first time.
    'catchup' : False,                             #Controls backfilling behavior. When set to False (default in your example), Airflow will only schedule the task for future execution dates, not past dates.
}


with DAG(dag_id='15_gcs_local',
    default_args=default_args,
    schedule_interval='@once') as dag: 
    start=DummyOperator(task_id='start')

    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id="local_to_gcs",
        gcp_conn_id="gcp_connection_2",
        src="/opt/airflow/airflow.cfg", #mi fichero  dentro del docket de airflow
        #src="/Users/jorgegarciaotero/Documents/airflow_composer/README.md",
        dst="airflow.cfg",
        bucket="airflow_sdbox_j", #gc:// does not work
    )

    # Task to download data from GCS
    gcs_to_local = GCSToLocalFilesystemOperator(
        task_id="gcs_to_local",
        gcp_conn_id="gcp_connection_2",
        bucket="airflow_sdbox_j",
        object_name="file.json",  # Specify the actual file name
        filename="file.json"  # Local filename to save to (can be different)
    )

local_to_gcs >> gcs_to_local