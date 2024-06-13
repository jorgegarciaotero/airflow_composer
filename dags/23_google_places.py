from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pandas as pd

from datetime import datetime
import json
from urllib.parse import urlencode

"""
API Key Config:
    conn_id: google_places_api
    conn_type: http
    host: https://maps.googleapis.com
    extra: 
        {
        api_key: UR_API_KEY
        }
"""
def process_response(ti, **kwargs):
    response = ti.xcom_pull(task_ids='google_places_request')
    data = json.loads(response)
    restaurants = data.get('results', [])
    
    # Extraer datos relevantes y convertirlos a un DataFrame
    restaurants_data = []
    for restaurant in restaurants:
        restaurants_data.append({
            "name": restaurant.get('name'),
            "address": restaurant.get('vicinity'),
            "rating": restaurant.get('rating'),
            "lat": restaurant['geometry']['location']['lat'],
            "lng": restaurant['geometry']['location']['lng']
        })
    
    df = pd.DataFrame(restaurants_data)
    print("DF:  ",df.dtypes)
    
    # Guardar el DataFrame en un archivo CSV (o puedes subirlo a una base de datos)
    #df.to_csv('/path/to/save/restaurants.csv', index=False)
    return    
    

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(dag_id='23_google_places_query_x',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

     
     
    '''
    google_places_request: Requests a place, which is written in the input field of the build_url () function
    '''   
    def find_a_place():
        connection = BaseHook.get_connection('google_places_api')
        api_key = connection.extra_dejson.get('api_key')
        base_url = 'maps/api/place/findplacefromtext/json'
        params = {
            "input": "boxing madrid",
            "inputtype": "textquery",
            "fields": "formatted_address,name,rating,opening_hours,geometry",
            "key": api_key
        }
        encoded_params = urlencode(params)
        print("URL: ",f"{base_url}?{encoded_params}")
        return f"{base_url}?{encoded_params}"


    def find_places_from_location():
        connection = BaseHook.get_connection('google_places_api')
        api_key = connection.extra_dejson.get('api_key')
        base_url = 'maps/api/place/nearbysearch/json'
        params = {
            "location": "40.4168,-3.7038",  # Especifica la latitud y longitud de la ubicación de búsqueda
            "radius": 5000,  # Especifica el radio de búsqueda en metros
            "keyword": "restaurant",  # Palabra clave para buscar restaurantes
            "key": api_key
        }
        encoded_params = urlencode(params)
        return f"{base_url}?{encoded_params}"


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
        python_callable=process_response,
        provide_context=True,
        op_kwargs={'response': '{{ ti.xcom_pull(task_ids="google_places_request") }}'},
    )

    google_places_request >> process_response_task 