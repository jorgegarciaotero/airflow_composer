import pandas as pd
import time
import requests
import os
import json
from datetime import datetime
from urllib.parse import urlencode
from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

#AIRFLOW VARIABLES
DATASET_NAME = Variable.get("places_schema")
TABLE_NAME = Variable.get("places_table")
PROJECT_NAME = Variable.get("project_name")
SCHEMA_PATH = Variable.get("schema_path")
LOCATION = Variable.get("bq_location")
CONNECTION_ID = "gcp_connection_2"  # Asumimos que esta es tu conexión configurada en Airflow
BUCKET_NAME = Variable.get("bucket_name")
#PROVISIONAL: PLACE TO SEARCH
PLACE_TO_SEARCH =Variable.get("place_to_search")

def print_variables():
    print(f"DATASET_NAME: {DATASET_NAME}")
    print(f"TABLE_NAME: {TABLE_NAME}")
    print(f"PROJECT_NAME: {PROJECT_NAME}")
    print(f"SCHEMA_PATH: {SCHEMA_PATH}")
    print(f"LOCATION: {LOCATION}")
    print(f"CONNECTION_ID: {CONNECTION_ID}")
    print(f"PLACE_TO_SEARCH: {PLACE_TO_SEARCH}")
    #Load the PLACE_TO_SEARCH string into a dictionary
    # Convertir la cadena JSON PLACE_TO_SEARCH en un diccionario
    try:
        place_to_search_dict = json.loads(PLACE_TO_SEARCH)
        # Acceder al atributo 'location' directamente desde el diccionario
        print("location:", place_to_search_dict["location"])
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")


def find_places_from_location():
    '''
    Returns the url to make the request to the API
    '''
    print("VARIABLE NAME: ",PLACE_TO_SEARCH)
    params = get_request_params()
    base_url = 'maps/api/place/nearbysearch/json'
    encoded_params = urlencode(params)
    return f"{base_url}?{encoded_params}"

def fetch_places(params):
    """
    Makes the request to the API
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
    """
    connection = BaseHook.get_connection('google_places_api')
    api_key = connection.extra_dejson.get('api_key')
    response = ti.xcom_pull(task_ids='google_places_request')
    data = json.loads(response)
    all_places = data.get('results', [])
    next_page_token = data.get('next_page_token')
    place_to_search_dict = json.loads(PLACE_TO_SEARCH)
    while next_page_token:
        time.sleep(2)  # Tiempo de espera requerido entre solicitudes
        params = {
            "location": place_to_search_dict["location"],
            "radius": place_to_search_dict["radius"],
            "keyword": place_to_search_dict["keyword"],
            "key": api_key,
            "pagetoken": next_page_token
        }
        next_data = fetch_places(params)
        all_places.extend(next_data.get('results', []))
        next_page_token = next_data.get('next_page_token')

    file_path = "/tmp/places_data.json"
    with open(file_path, 'w') as f:
        for place in all_places:
            json_record = {
                "name": place.get('name'),
                "opening_hours": place.get('opening_hours'),
                "price_level": place.get('price_level'),
                "address": place.get('vicinity'),
                "rating": place.get('rating'),
                "lat": place['geometry']['location']['lat'],
                "lng": place['geometry']['location']['lng'],
                "types": place.get('types'),
                "user_ratings_total": place.get('user_ratings_total')
            }
            f.write(json.dumps(json_record) + '\n')

    gcs_hook = GCSHook(gcp_conn_id=CONNECTION_ID)
    gcs_hook.upload(bucket_name=BUCKET_NAME, object_name="places_data.json", filename=file_path)

    ti.xcom_push(key="places_dataframe", value=all_places)

def print_xcom_value(ti, **kwargs):
    """
    Print the value of places_dataframe pushed to XCom
    """
    places_data = ti.xcom_pull(task_ids='process_response_task', key='places_dataframe')
    if places_data is None:
        raise ValueError("No data found for 'places_dataframe' in XCom")
    print("Places Data:")
    for place in places_data:
        print(place)

def get_request_params():
    connection = BaseHook.get_connection('google_places_api')
    api_key = connection.extra_dejson.get('api_key')
    place_to_search_dict = json.loads(PLACE_TO_SEARCH)
    params = {
        "location": place_to_search_dict["location"],
        "radius": place_to_search_dict["radius"],
        "keyword": place_to_search_dict["keyword"],
        "key": api_key
    }
    return params

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(dag_id='23_google_places_nearbysearch_',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    print_vars_task = PythonOperator(
        task_id='print_vars',
        python_callable=print_variables
    )

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

    gcs_to_bq_task = GCSToBigQueryOperator(
        task_id="gcs_to_bq_task",
        gcp_conn_id=f"{CONNECTION_ID}", 
        bucket=f"{BUCKET_NAME}",
        source_objects=["places_data.json"],  # URI path
        destination_project_dataset_table=f"{PROJECT_NAME}.{DATASET_NAME}.{TABLE_NAME}",
        write_disposition="WRITE_TRUNCATE",
        source_format="NEWLINE_DELIMITED_JSON",
        schema_fields=[
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
        ],
    )

    print_vars_task >> google_places_request >> process_response_task >> print_xcom_task >> gcs_to_bq_task
