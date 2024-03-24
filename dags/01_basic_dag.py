from builtins import range
from datetime import timedelta,datetime
import yaml
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago


# Load email from config.yaml
with open("config.yaml", "r") as stream:
    '''
    In Composer Environment Variables, set  a new value - key with your email config.
    AIRFLOW_CONFIG = gs://my-bucket/config.yaml
    '''
    try:
        config = yaml.safe_load(stream)
        email = config.get("email")
    except yaml.YAMLError as exc:
        raise Exception(f"Error loading config.yaml: {exc}")
    
    
default_args = {
    'owner':'Airflow',                             #User who owns the DAG
    'start_date': datetime(2024,3,24, 10,50,00),   #date and time when the task should be scheduled to run for the first time.
    'catchup' : False,                             #Controls backfilling behavior. When set to False (default in your example), Airflow will only schedule the task for future execution dates, not past dates.
    'depends_on_past': False,                      #Determines if the task depends on the successful completion of its upstream tasks. Set to False in your example, indicating it doesn't rely on past runs.
    'email': email ,                               #A list of email addresses to notify in case of task failures. Your example includes a single address
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,                                  #Defines the number of times a task should be retried before failing. In your case, it allows for one retry.
    'retry_delay': timedelta(minutes=5),           #Specifies the time interval to wait between retries. Here, it's set to 5 minutes.
    # 'queue': 'bash_queue',                       #Assigns the task to a specific Airflow queue for execution prioritization.
    # 'pool': 'backfill',                          #Links the task to a specific Airflow pool of resources for limiting concurrency.
    # 'priority_weight': 10,                       #Influences task scheduling priority within a queue.
    # 'end_date': datetime(2016, 1, 1),            #Sets the end date for scheduling the task. Tasks won't be scheduled after this date.
    # 'wait_for_downstream': False,                #Controls if the task waits for downstream tasks to complete before starting.
    # 'sla': timedelta(hours=2),                   #Defines a Service Level Agreement (SLA) for task execution time. If the task takes longer, Airflow alerts designated recipients.
    # 'execution_timeout': timedelta(seconds=300), #Sets a maximum execution time for the task. Exceeding this time will trigger a timeout failure.
    # 'on_failure_callback': some_function,        #Defines a custom function to be called when the task fails.
    # 'on_success_callback': some_other_function,  #Similar to on_failure_callback, but for successful task completion.
    # 'on_retry_callback': another_function,       #A function to execute upon task retries.
    # 'sla_miss_callback': yet_another_function,   #A function to call when an SLA violation occurs.
    # 'trigger_rule': 'all_success'                #Defines the conditions under which a task should be triggered.
}


def helloWorldLoop():
    for word in ['hello','world']:
        print(word)

with DAG(dag_id='01_basic_dag',
    default_args=default_args,
    schedule_interval='@once') as dag: 
    start=DummyOperator(task_id='start')
    
    test_python = PythonOperator(
        task_id='test_python',
        python_callable=helloWorldLoop
    )
    
    test_bash=BashOperator(
        task_id='test_bash',
        bash_command='echo test_bash'
    )

start >> test_python >> test_bash