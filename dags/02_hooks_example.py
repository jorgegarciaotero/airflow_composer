import pandas as pd
import logging
from airflow.models import DAG
from airflow.utils import dates
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args={
    'start_date':dates.days_ago(1)
}


def obtener_pandas():
    conn=PostgresHook('redshift-produccion')
    df= conn.get_pandas_df("SELECT * FROM TABLE")
    logging.info("Data retrieved")
    df.to_csv('test.csv')
    logging.info("saved")

with DAG(
    dag_id='02_hooks_example',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:
    
    start=DummyOperator(
        task_id='start'
    )
    
    obtener_pandas_operator=PythonOperator(
        task_id='obtener_pandas_operator',
        python_callable=obtener_pandas
    )
    
    fin=DummyOperator(
        task_id='fin'
    )

start >> obtener_pandas_operator >> fin