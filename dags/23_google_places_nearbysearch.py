import pandas as pd
import time
import requests
import os
import yaml
import sys
from airflow import DAG
from datetime import datetime
import json
from urllib.parse import urlencode
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import(
    GCSToBigQueryOperator
)
from airflow.models import Variable

# Obtén las variables desde Airflow
DATASET_NAME = Variable.get("places_schema")
TABLE_NAME = Variable.get("places_table")
PROJECT_NAME = Variable.get("project_name")
SCHEMA_PATH = Variable.get("schema_path")
LOCATION = Variable.get("bq_location")
CONNECTION_ID = "gcp_connection_2"  # Asumimos que esta es tu conexión configurada en Airflow
BUCKET_NAME = Variable.get("bucket_name")


def find_places_from_location():
    '''
    Returns the url to make the request to the API
    '''
    params = get_request_params()
    base_url = 'maps/api/place/nearbysearch/json'
    encoded_params = urlencode(params)
    return f"{base_url}?{encoded_params}"


def fetch_places(params):
    """
    Makes de request to the API
    args:  
        params: dictionary with the parameters to make the request
    returns: 
        json response
    """
    base_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json'
    response = requests.get(base_url, params=params)
    return response.json()

def process_response_places(ti, **kwargs):
    """
    Process the response from the API
    args:
        ti: task instance
        **kwargs: kwargs
    returns:
        places_data_serializable: list of dictionaries with the places data
    """
    connection = BaseHook.get_connection('google_places_api')
    api_key = connection.extra_dejson.get('api_key')
    response = ti.xcom_pull(task_ids='google_places_request')
    data = json.loads(response)
    all_places = data.get('results', [])
    next_page_token = data.get('next_page_token')
    i = 0
    while next_page_token:
        time.sleep(2)  # Tiempo de espera requerido entre solicitudes
        params = {
            "location": "40.4168,-3.7038",
            "radius": 5000,
            "keyword": "restaurant pet friendly",
            "key": api_key,
            "pagetoken": next_page_token
        }
        next_data = fetch_places(params)
        all_places.extend(next_data.get('results', []))
        next_page_token = next_data.get('next_page_token')
        i += 1
    places_data = []
    for place in all_places:
        places_data.append({
            "name": place.get('name'),
            "opening_hours": place.get('opening_hours'),
            "price_level": place.get('price_level'),
            "address": place.get('vicinity'),
            "rating": place.get('rating'),
            "lat": place['geometry']['location']['lat'],
            "lng": place['geometry']['location']['lng'],
            "types": place.get('types'),
            "user_ratings_total": place.get('user_ratings_total')
        })
    df = pd.DataFrame(places_data)
    places_data_serializable = df.to_dict(orient='records')
    ti.xcom_push(key="places_dataframe", value=places_data_serializable)



def print_xcom_value(ti, **kwargs):
    """
    Print the value of places_dataframe pushed to XCom
    """
    places_data = ti.xcom_pull(task_ids='process_response_task', key='places_dataframe')
    print("Places Data:")
    for place in places_data:
        print(place)

def get_request_params():
    connection = BaseHook.get_connection('google_places_api')
    api_key = connection.extra_dejson.get('api_key')
    params = {
        "location": "40.4168,-3.7038",
        "radius": 5000,
        "keyword": "restaurant",
        "key": api_key
    }
    return params

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(dag_id='23_google_places_nearbysearch',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:



    google_places_request = SimpleHttpOperator(
        task_id='google_places_request',
        method='GET',
        http_conn_id='google_places_api',
        endpoint=find_places_from_location(),
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )

    process_response_task = PythonOperator(
        task_id='process_response_task',
        python_callable=process_response_places,
        provide_context=True,
    )

    print_xcom_task = PythonOperator(
        task_id='print_xcom_task',
        python_callable=print_xcom_value,
        provide_context=True,
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
            [
                {
                    "name": "name",
                    "type": "STRING",
                    "mode": "REQUIRED"
                },
                {
                    "name": "opening_hours",
                    "type": "RECORD",
                    "mode": "NULLABLE",
                    "fields": [
                        {
                            "name": "open_now",
                            "type": "BOOLEAN",
                            "mode": "NULLABLE"
                        }
                    ]
                },
                {
                    "name": "price_level",
                    "type": "FLOAT",
                    "mode": "NULLABLE"
                },
                {
                    "name": "address",
                    "type": "STRING",
                    "mode": "NULLABLE"
                },
                {
                    "name": "rating",
                    "type": "FLOAT",
                    "mode": "NULLABLE"
                },
                {
                    "name": "lat",
                    "type": "FLOAT",
                    "mode": "NULLABLE"
                },
                {
                    "name": "lng",
                    "type": "FLOAT",
                    "mode": "NULLABLE"
                },
                {
                    "name": "types",
                    "type": "STRING",
                    "mode": "REPEATED"
                },
                {
                    "name": "user_ratings_total",
                    "type": "INTEGER",
                    "mode": "NULLABLE"
                }
            ]
        ],
        field_delimiter =",",
        skip_leading_rows=1
    )

    google_places_request >> process_response_task >> print_xcom_task
