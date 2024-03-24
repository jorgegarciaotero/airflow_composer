import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""
    print("PATH!!!",input_path)
    data_json = pd.read_json("/data/events.json", typ='series')
    Path(output_path).parent.mkdir(exist_ok=True)
    data_json.to_csv(output_path, index=False) 


dag = DAG(
    dag_id="03_unscheduled",
    start_date=dt.datetime(2022, 11, 19),
    schedule_interval=None,
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
    "sudo mkdir -p /data && "
    "sudo curl -o /data/events.json "
    "https://www.codever.land/api/version | jq ."
    ),
    dag=dag,
)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
        op_kwargs={
            "input_path": "/data/events.json",
            "output_path": "/data/stats.csv",
        },
    dag=dag,
)

fetch_events >> calculate_stats