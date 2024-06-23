import pandas as pd
import time
import requests
import os
import sys
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

# AIRFLOW VARIABLES
DATASET_NAME = Variable.get("places_schema")
TABLE_NAME = Variable.get("places_table")
PROJECT_NAME = Variable.get("project_name")
SCHEMA_PATH = Variable.get("schema_path")
LOCATION = Variable.get("bq_location")
CONNECTION_ID = "gcp_connection_2"
BUCKET_NAME = Variable.get("bucket_name")
PLACE_TO_SEARCH = Variable.get("place_to_search")
SCHEMA_BQ = Variable.get("schema_bq")

# Example coordinates for Madrid center
MADRID_CENTER = {"lat": 40.416775, "lng": -3.703790}


def print_variables():
    print(f"DATASET_NAME: {DATASET_NAME}")
    print(f"TABLE_NAME: {TABLE_NAME}")
    print(f"PROJECT_NAME: {PROJECT_NAME}")
    print(f"SCHEMA_PATH: {SCHEMA_PATH}")
    print(f"LOCATION: {LOCATION}")
    print(f"CONNECTION_ID: {CONNECTION_ID}")
    print(f"PLACE_TO_SEARCH: {PLACE_TO_SEARCH}")
    #Load the PLACE_TO_SEARCH string into a dictionary
    try:
        place_to_search_dict = json.loads(PLACE_TO_SEARCH)
        # Acceder al atributo 'location' directamente desde el diccionario
        print("location:", place_to_search_dict["location"])
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")



def divide_area(center, radius_lat, radius_lng, step):
    """
    Divide the area around the center into smaller areas.
    Args:
    - center (dict): A dictionary with 'lat' and 'lng' keys.
    - radius_lat (float): Radius in degrees for latitude.
    - radius_lng (float): Radius in degrees for longitude.
    - step (float): Step size in degrees for the division.
    Returns:
    - List of coordinates (lat, lng) for the smaller areas.
    """
    coordinates = []
    lat_start, lng_start = float(center["lat"]) - radius_lat, float(center["lng"]) - radius_lng
    lat_end, lng_end = float(center["lat"]) + radius_lat, float(center["lng"]) + radius_lng
    lat = lat_start
    while lat <= lat_end:
        lng = lng_start
        while lng <= lng_end:
            coordinates.append({"lat": lat, "lng": lng})
            lng += step
        lat += step
    return coordinates

def get_request_params(lat, lng, radius, keyword, api_key):
    """
    Get the request parameters for the API call.
    Args:
    - lat (float): Latitude of the location.
    - lng (float): Longitude of the location.
    - radius (int): Search radius.
    - keyword (str): Keyword for the search.
    - api_key (str): API key for authentication.
    Returns:
    - Dictionary with the request parameters.
    """
    params = {
        "location": f"{lat},{lng}",
        "radius": radius,
        "keyword": keyword,
        "key": api_key
    }
    return params

def fetch_places(params):
    """
    Makes the request to the API.
    Args:
    - params (dict): Dictionary with the parameters for the request.
    Returns:
    - JSON response.
    """
    base_url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json'
    response = requests.get(base_url, params=params)
    return response.json()


def process_places(places, all_places):
    """
    Process and add places to the all_places dictionary.
    Args:
    - places (list): List of places from the API response.
    - all_places (dict): Dictionary to store unique places.
    """
    for place in places:
        place_id = place.get('place_id')
        if place_id and place_id not in all_places:
            all_places[place_id] = place
            

def process_response_places(ti, **kwargs):
    """
    Process the API response to fetch all places.
    Args:
    - ti (TaskInstance): Airflow task instance.
    """
    connection = BaseHook.get_connection('google_places_api')
    api_key = connection.extra_dejson.get('api_key')
    place_to_search_dict = json.loads(PLACE_TO_SEARCH)
    radius = place_to_search_dict["radius"]
    keyword = place_to_search_dict["keyword"]
    
    all_places = {}  # Dict to save places with place_id as key
    seen_tokens = set()  #Set to store already used page tokens
    
    radius_lat = 0.0135 * 20  # 0.0135 = aprox 1500 m 
    radius_lng = 0.0175 * 20  # 0.0175 = aprox 1500 m  
    step = 0.1
    coordinates_list = divide_area(MADRID_CENTER, radius_lat, radius_lng, step)
    print("coordinates_list: ", coordinates_list)
    
    for coords in coordinates_list:
        lat, lng = coords["lat"], coords["lng"]
        params = get_request_params(lat, lng, radius, keyword, api_key)
        data = fetch_places(params)
        
        # Process first request
        process_places(data.get('results', []), all_places)
        next_page_token = data.get('next_page_token')
        
        # Process next places if a next_page_token exists in the first response
        while next_page_token and next_page_token not in seen_tokens:
            seen_tokens.add(next_page_token)
            time.sleep(2)
            params["pagetoken"] = next_page_token
            next_data = fetch_places(params)
            process_places(next_data.get('results', []), all_places)
            print(f"all_places: {all_places}")
            next_page_token = next_data.get('next_page_token')

    # Saves the places to a json in /tmp
    file_path = "/tmp/places_data.json"
    with open(file_path, 'w') as f:
        for place in all_places.values():
            json_record = {
                "name": place.get('name'),
                "place_id": place.get('place_id'),
                "opening_hours": place.get('opening_hours'),
                "price_level": place.get('price_level'),
                "address": place.get('vicinity'),
                "rating": place.get('rating'),
                "lat": place['geometry']['location']['lat'],
                "lng": place['geometry']['location']['lng'],
                "types": place.get('types'),
                "user_ratings_total": place.get('user_ratings_total'),
                "reviews": place.get('reviews'),
                "website": place.get('website'),
                "formatted_phone_number": place.get('formatted_phone_number'),
                "formatted_address": place.get('formatted_address'),
                "url": place.get('url'),
                "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            f.write(json.dumps(json_record) + '\n')
    
    # Uploads the file to GCS
    gcs_hook = GCSHook(gcp_conn_id=CONNECTION_ID)
    gcs_hook.upload(bucket_name=BUCKET_NAME, object_name="places_data.json", filename=file_path)
    ti.xcom_push(key="places_dataframe", value=list(all_places.values()))
def print_xcom_value(ti, **kwargs):
    """
    Print the value of places_dataframe pushed to XCom.
    """
    places_data = ti.xcom_pull(task_ids='process_response_task', key='places_dataframe')
    if places_data is None:
        raise ValueError("No data found for 'places_dataframe' in XCom")
    for place in places_data:
        print(place)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(dag_id='26_google_places_nearbysearch_new',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    print_vars_task = PythonOperator(
        task_id='print_vars',
        python_callable=print_variables
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
        schema_fields = [
    {
        "name": "name",
        "type": "STRING",
        "mode": "REQUIRED"
    },
    {
        "name": "place_id",
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
            },
            {
                "name": "periods",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {
                        "name": "open",
                        "type": "RECORD",
                        "mode": "NULLABLE",
                        "fields": [
                            {
                                "name": "day",
                                "type": "INTEGER",
                                "mode": "NULLABLE"
                            },
                            {
                                "name": "time",
                                "type": "STRING",
                                "mode": "NULLABLE"
                            }
                        ]
                    },
                    {
                        "name": "close",
                        "type": "RECORD",
                        "mode": "NULLABLE",
                        "fields": [
                            {
                                "name": "day",
                                "type": "INTEGER",
                                "mode": "NULLABLE"
                            },
                            {
                                "name": "time",
                                "type": "STRING",
                                "mode": "NULLABLE"
                            }
                        ]
                    }
                ]
            },
            {
                "name": "weekday_text",
                "type": "STRING",
                "mode": "REPEATED"
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
        "name": "types",
        "type": "STRING",
        "mode": "REPEATED"
    },
    {
        "name": "rating",
        "type": "FLOAT",
        "mode": "NULLABLE"
    },
    {
        "name": "user_ratings_total",
        "type": "INTEGER",
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
        "name": "reviews",
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {
                "name": "author_name",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "author_url",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "language",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "profile_photo_url",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "rating",
                "type": "FLOAT",
                "mode": "NULLABLE"
            },
            {
                "name": "relative_time_description",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "text",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "time",
                "type": "INTEGER",
                "mode": "NULLABLE"
            }
        ]
    },
    {
        "name": "website",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "formatted_phone_number",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "formatted_address",
        "type": "STRING",
        "mode": "NULLABLE"
    },
    {
        "name": "url",
        "type": "STRING",
        "mode": "NULLABLE"
    }
]
    )

    print_vars_task >> process_response_task >> print_xcom_task >> gcs_to_bq_task
