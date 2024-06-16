from builtins import range
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)

# Obtén las variables desde Airflow
DATASET_NAME = Variable.get("places_schema")
TABLE_NAME = Variable.get("places_table")
PROJECT_NAME = Variable.get("project_name")
SCHEMA_PATH = Variable.get("schema_path")
LOCATION = Variable.get("bq_location")
CONNECTION_ID = "gcp_connection_2"  # Asumimos que esta es tu conexión configurada en Airflow

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 3, 24, 10, 50, 00),
    'catchup': False,
}

def print_variables():
    print(f"DATASET_NAME: {DATASET_NAME}")
    print(f"TABLE_NAME: {TABLE_NAME}")
    print(f"PROJECT_NAME: {PROJECT_NAME}")
    print(f"SCHEMA_PATH: {SCHEMA_PATH}")
    print(f"LOCATION: {LOCATION}")
    print(f"CONNECTION_ID: {CONNECTION_ID}")

with DAG(dag_id='25_create_places_dataset_table',
         default_args=default_args,
         schedule_interval='@once') as dag:

    start = DummyOperator(task_id='start')

    print_vars_task = PythonOperator(
        task_id='print_vars',
        python_callable=print_variables
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        gcp_conn_id=CONNECTION_ID,
        dataset_id=DATASET_NAME,
        project_id=PROJECT_NAME,
        location=LOCATION,
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        gcp_conn_id=CONNECTION_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        project_id=PROJECT_NAME,
        gcs_schema_object=SCHEMA_PATH,
        google_cloud_storage_conn_id=CONNECTION_ID,
    )

    start >> print_vars_task >> create_dataset >> create_table
