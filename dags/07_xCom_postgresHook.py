from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import DAG
from datetime import datetime


default_args={
    'owner':'jorge_garcia',
    'start_date': datetime(2022,1,1,12,0,0)
}


def save_date(**context):
    postgres=PostgresHook('normal_redshift')
    fecha_max_df = postgres.get_pandas_df('select max(load_datetime) from users')
    fecha_max_list = fecha_max_df.tolist()
    fecha_max = fecha_max_list[0]


with DAG(dag_id='07_xCom_postgresHook',
         default_args=default_args,
         schedule_interval='@daily'
    ) as dag:
    

    obtainDate = PythonOperator(task_id='obtain_date',
                                python_callable=save_date,
                                provide_context=True)

    printDate = BashOperator(task_id='print_date',
                                bash_command="echo {‌{ ti.xcom_pull(task_ids='obtain_date', key='fecha') }}")

    obtainDate >> printDate