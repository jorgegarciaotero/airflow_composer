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

with DAG(dag_id='16_bq_dataset',
    default_args=default_args,
    schedule_interval='@once') as dag: 
    start=DummyOperator(task_id='start')

    create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    gcp_conn_id="gcp_connection_2",
    dataset_id="test_dataset_2",
    location="southamerica-east1",
    )
    


start >>  create_dataset