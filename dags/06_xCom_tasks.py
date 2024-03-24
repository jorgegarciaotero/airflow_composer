#xComs: Cross Communication. little info shareable between tasks. 
#inserted into metadata db, so its restricted to light data.
#pull and push info

from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args={
    'owner':'jorge_garcia',
    'start_date': datetime(2022,1,1,12,0,0)
}

def hello_world(**context):
    for palabra in ['hello','world']:
        print('word:' ,palabra)
    task_instance=context['task_instance']
    task_instance.xcom_push(key='test_key',value='test_value')
    return 'hi man'
        
def pull_test(**context):
    ti= context['task_instance']
    taken_value=ti.xcom_pull(task_ids="pythonCall") #we want to return "hi man", so we set the task_id given in pythonCall
    print("value: ",taken_value)

with DAG(dag_id='06_xCom_task',
         default_args=default_args,
         schedule_interval='@daily'
    ) as dag:
    
    start=DummyOperator(task_id='start')
    
    pythonCall=PythonOperator(
        task_id='pythonCall',
        python_callable=hello_world,
        do_xcom_push=True,  # when true means that uploads to xCom the return of this operator, in this case the return "hi"
        provide_context=True #sends all objects to python func hello_world
    )
    
    pythonCallPull=PythonOperator( 
        task_id='test_pull',    
        python_callable=pull_test,
        do_xcom_push=True,   #to pull data from another task
        provide_context=True
    )
    
    bashCall=BashOperator(
        task_id='bashCall',
        bash_command='python3 /home/jorgegarciaotero/airflow/dags/xconf.py', #this is the exec of a file with a print("testing_xcom") inside
    )
    
    bashCall2=BashOperator(
        task_id='bashCall2',
        bash_command='echo {{ ti.xcom_pull(task_ids="pythonCall") }}' # use of jinja templates, returns "hi man"
    )
    
    bashCall3=BashOperator(
        task_id='bashCall3',
        bash_command='echo {{ var.value.email}}' # use of jinja using values stored in pythons memory
    )
    
    start >> pythonCall >> pythonCallPull >>  bashCall  >>  bashCall2