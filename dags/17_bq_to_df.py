from builtins import range
from datetime import timedelta,datetime
import pandas as pd
import yaml
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bigquery_operator import BigQueryOperator

from airflow.utils.dates import days_ago
# Load email from config.yaml
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


def load_to_dataframe(**kwargs):
    bq_task_id = kwargs['templates_dict']['bq_task_id']
    # Obtener los resultados de la consulta de BigQuery
    ti = kwargs['ti']
    print("WKARGS: ",kwargs)
    bq_result = ti.xcom_pull(task_ids=bq_task_id, key='return_value')
    print("bq_result: ",bq_result)
    # Crear un DataFrame con los resultados
    df = pd.DataFrame(bq_result)
    # Imprimir los primeros registros del DataFrame (opcional)
    print(df.head())



with DAG(dag_id='17_bq_to_df',
    default_args=default_args,
    schedule_interval='@once') as dag: 


    start=DummyOperator(task_id='start')


    query_bq_data = BigQueryExecuteQueryOperator(
        task_id="query_bq_data",
        gcp_conn_id="gcp_connection_2",  
        sql="SELECT *  FROM `bigquery-public-data.google_trends.top_terms`",
        use_legacy_sql=False, 
    )

    load_to_dataframe_task = PythonOperator(
        task_id='load_to_dataframe',
        python_callable=load_to_dataframe,
        provide_context=True,
        templates_dict={'bq_task_id': 'query_bq_data'}
    )


start >> query_bq_data  >> load_to_dataframe_task
