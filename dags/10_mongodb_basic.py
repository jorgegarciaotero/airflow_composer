from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient

import constants as cte
import json

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '10_mongodb_basic',
    default_args=default_args,
    schedule_interval=timedelta(hours=111))

def query_mongodb(**kwargs):
    '''
    Connects to MongoClient  and queries a document. 
    Receives from op_kwargs the query, database_name,collection_name and query_limit 
    '''
    database_name=kwargs['database_name']
    collection_name=kwargs['collection_name']
    query=json.loads(kwargs['query'])
    query_limit=kwargs['query_limit']
    print("VALORES: ",database_name,collection_name,query)
    client = MongoClient(f"mongodb+srv://{cte.mongodb_user}:{cte.mongdb_pass}@cluster0.ldeyx.mongodb.net/{cte.mongodb_database}?retryWrites=true&w=majority")
    db=client[database_name]
    collection=db[collection_name]
    results = collection.find(query).limit(int(query_limit))
    for result in results:
        print(result)

def insert_one_mongodb(**kwargs):
    '''
    Connects to MongoClient and inserts a document.
    Receives from op_kwargs the query, database_name,collection_name and query_limit 
    '''   
    database_name=kwargs['database_name']
    collection_name=kwargs['collection_name']
    insertion=json.loads(kwargs['insertion'])
    print("VALORES: ",database_name,collection_name,insertion)
    client = MongoClient(f"mongodb+srv://{cte.mongodb_user}:{cte.mongdb_pass}@cluster0.ldeyx.mongodb.net/{cte.mongodb_database}?retryWrites=true&w=majority")
    db=client[database_name]
    collection=db[collection_name]
    collection.insert_one(insertion)
    
    
mongo_query = PythonOperator(
    task_id='query_mongo',
    python_callable=query_mongodb,
    provide_context=True, #Airflow will pass the context variables to the function as keyword arguments.
    op_kwargs={'query': json.dumps({"watlev":"always under water/submerged"} ),
               'database_name':'sample_geospatial',
               'collection_name':'shipwrecks',
               'query_limit': '15',
               }, #pass the json_data to the function
    dag=dag)

mongo_insert_one = PythonOperator(
    task_id='insert_one_mongodb',
    python_callable=insert_one_mongodb,
    provide_context=True, #Airflow will pass the context variables to the function as keyword arguments.
    op_kwargs={'insertion': json.dumps({"watlev":"always under water/submerged",
                                    "depth":"288",
                                    "history":"test from airflow 3",
                                    "chart":"ES,ES,graph, Chart 288"} ),
               'database_name':'sample_geospatial',
               'collection_name':'shipwrecks'
               }, #pass the json_data to the function
    dag=dag)



mongo_query >> mongo_insert_one
