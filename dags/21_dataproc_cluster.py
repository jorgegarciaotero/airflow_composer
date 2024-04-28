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

from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator
)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import(
    GCSToBigQueryOperator
)

from airflow.providers.google.cloud.operators.dataproc  import  (
    DataprocSubmitJobOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator
)
 
#world_port_index
ORG_PROJECT_NAME = os.environ.get("GCP_PROJECT_NAME", 'jorgegotero')
ORG_DATASET_NAME= os.environ.get("GCP_DATASET_NAME", 'worldpop')
ORG_TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'population_grid_1km')

DEST_PROJECT_NAME = os.environ.get("GCP_PROJECT_NAME", 'jorgegotero')
DEST_DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'worldpop')
DEST_TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'popul')

BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME",'airflow_sdbox_j')
BUCKET_FILE = os.environ.get("BUCKET_FILE",'port_data.csv')
CONNECTION_ID_BQ = os.environ.get("CONNECTION_ID_VARIABLE",'gcp_connection_2')
CONNECTION_ID_DP = os.environ.get("CONNECTION_ID_VARIABLE",'gcp_connection_1')

#https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataproc/index.html

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


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",  # Cambiar a un tipo de máquina más pequeño
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},  # Aumentar el tamaño del disco de arranque
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",  # Cambiar a un tipo de máquina más pequeño
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},  # Aumentar el tamaño del disco de arranque
    },
    # No necesitas configurar un worker secundario si no lo necesitas
}


PYSPARK_JOB = {
    "reference": {"project_id": f"{ORG_PROJECT_NAME}"},
    "placement": {"cluster_name": 'myclusterjgotero1'},
    "spark_sql_job": {"query_list": {"queries": ["SHOW DATABASES;"]}},
}

with DAG(dag_id='21_dataprepcluster',
    default_args=default_args,
    schedule_interval='@once') as dag: 
    
    start=DummyOperator(task_id='01_start')
    

    # Run Spark job on Dataproc cluster to analyze data and generate report
    create_dataproc_cluster = DataprocCreateClusterOperator(
        dag=dag,
        task_id='create_dataproc_cluster',
        gcp_conn_id=f"{CONNECTION_ID_DP}",
        project_id=f"{ORG_PROJECT_NAME}",
        cluster_config=CLUSTER_CONFIG,
        region='us-central1',
        cluster_name='myclusterjgotero1'
    )

    submit_pyspark_job = DataprocSubmitJobOperator(
        dag=dag,
        task_id='submit_pyspark_job',
        job=PYSPARK_JOB,
        region='us-central1',
        gcp_conn_id=f"{CONNECTION_ID_DP}",
        project_id=f"{ORG_PROJECT_NAME}"
    )
    
    
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=f"{ORG_PROJECT_NAME}",
        gcp_conn_id=f"{CONNECTION_ID_DP}",
        cluster_name="myclusterjgotero1",
        region='us-central1'
    )
    

 # Set dependencies between tasks
start  >> submit_pyspark_job >> delete_cluster