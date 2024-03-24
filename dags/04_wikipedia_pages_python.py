import airflow.utils.dates
from urllib import request
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _get_data(output_path, **context):
    year, month, day, hour, *_ = context["execution_date"].timetuple()
    hour=hour-1
    print("valores:", year, month, day, hour-1, *_)
    url = ("https://dumps.wikimedia.org/other/pageviews/"
           f"{year}/{year}-{month:0>2}/"
           f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz")
    print(url)
    request.urlretrieve(url, output_path)

def _fetch_pageviews(pagenames):
    result=dict.fromkeys(pagenames,0)
    print("RESULTADO!!!: ",result)
    with open(f"/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames: 
                result[page_title] = view_counts


dag = DAG(
    dag_id="04_wikipedia_pages_python",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly",
)


def _print_context(**kwargs):
    print("VALORES: ",kwargs)
    start = kwargs["execution_date"]
    end = kwargs["next_execution_date"]
    print(f"Start: {start}, end: {end}")
        
print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag,
)

get_data=PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
        "output_path": "/tmp/wikipageviews.gz",
    },
    dag=dag,
)

extract_gz=BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force /tmp/wikipageviews.gz",
    dag=dag
)

fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": {
            "Google",
            "Amazon",
            "Apple",
            "Microsoft",
            "Facebook",
        }
    },
    dag=dag
)


print_context >> get_data >> extract_gz >> fetch_pageviews