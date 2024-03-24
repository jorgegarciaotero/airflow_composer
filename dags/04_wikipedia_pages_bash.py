import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _print_context(**kwargs):
    print("VALORES: ",kwargs)
    start = kwargs["execution_date"]
    end = kwargs["next_execution_date"]
    print(f"Start: {start}, end: {end}")
       

def _fetch_pageviews(pageviews):
    result=dict.fromkeys(pagenames,0)
    print("RESULTADO!!!: ",result)
    with open(f"/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames: 
                result[page_title] = view_counts
   




dag = DAG(
    dag_id="04_wikipedia_pages_bash",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly",
)

get_data = BashOperator(
    task_id="get_data",
    bash_command=(
        "curl -o /tmp/wikipageviews.gz https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year }}/"
        "{{ execution_date.year }}-"
        "{{ '{:02}'.format(execution_date.month) }}/"
        "pageviews-{{ execution_date.year }}"
        "{{ '{:02}'.format(execution_date.month) }}"
        "{{ '{:02}'.format(execution_date.day) }}-"
        "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
    ),
    dag=dag,
)


print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
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


print_context >> get_data >> extract_gz 