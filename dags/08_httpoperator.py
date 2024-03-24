from airflow import DAG
from airflow.utils import dates
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import BranchPythonOperator

default_args={
    'start_date':dates.days_ago(177),
    }

def bifurcacion(**context):
    ti = context['task_instance']
    api_resonse=ti.xcom_pull(task_id='api_call')
    response_list=ast.literal_eval(api_response)
    number=response_list[0]['random']
    if number > 5:
        return 'mayor_que_5'
    else:
        return 'menor_que_5'
    
    
with DAG(
    '08_httpoperator',
    default_args=default_args,
    schedule_interval='@daily'
    ) as dag:
    
    start = DummyOperator(
        task_id='start'
    )
    
    api_call=SimpleHttpOperator(
        task_id='api_task',
        http_conn_id='random_number', #admin --> connections
        endpoint='csrng/csrng.php',
        method='GET',
        data={'min':'0','max':'10'},
        do_xcom_push=True  #la peticion devuelve una lista [{"status":"success","min":0,"max":10,"random":4}]
        )
    
    bifurcacion_task=BranchPythonOperator(
        task_id='bifurcacion_task',
        python_callable=bifurcacion,
        provide_context=True
    )
    
    mayor_que_5=DummyOperator(task_id='mayor_que_5')
    
    menor_que_5=DummyOperator(task_id='menor_que_5')

    
    start >> api_call >> bifurcacion_task >> [mayor_que_5,menor_que_5]