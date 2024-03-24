from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset("/tmp/my_file.txt")   #define/create a dataset. The URI is the unique identifier of my dataset

with DAG(
    dag_id="11_consumer_dataset",
    schedule=[my_file],               # As soon as my_file dataset is updated, that triggers the DAG consummer
    start_date=datetime(2024,1,1),
    catchup=False
):
    @task()             #what task updates de dataset
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())

    read_dataset()  #execute the task