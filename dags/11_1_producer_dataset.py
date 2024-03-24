from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset("/tmp/my_file.txt") #define/create a dataset. The URI is the unique identifier of my dataset

with DAG(
    dag_id="11_producer_dataset",
    schedule="@daily",
    start_date=datetime(2024,1,1),
    catchup=False
):
    @task(outlets=[my_file])  #what task updates de dataset
    def update_dataset():
        with open(my_file.uri, "a+") as f:
            f.write("Hello, World!")

    update_dataset()  #execute the task