from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


def create_dag(dag_id,schedule,default_args,country):
    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args
              )
    
    with dag:  
        start = DummyOperator(task_id='start')
        print_country = BashOperator(
            task_id='print',
            bash_command=f'echo {country}'
        )
        
        start >> print_country
        
    return dag

countries=['es','uk','fr','de']
for country in countries:
    dag_id=f'AAA_{country}'
    default_args={
        'owner':f'airflow_{country}',
        'start_date':days_ago(1)
    }
    
    globals()[dag_id]=create_dag(
        dag_id,
        '@daily',
        default_args=default_args,
        country=country
    )