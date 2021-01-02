from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(dag_id='first_test', default_args=default_args, schedule_interval=None)

def run_this_func(**context):
    random_value = random.random(),
    received_value = context['ti'].xcom_pull(key'random_value')
    print('hi, I received the following {str(received_value)})

def push_to_xcom(**context):
    random_value = random.random()
    context['ti'].xcom_push(key='random_value', value=random_value)
    print('I am okay')

#def randomly_fail(**context):
#    if random.random() > 0.7:
#        raise Exception('Exception')
#    print('I am okay')

with dag:
    run_this_task = PythonOperator(
        task_id='123',
        python_callable=push_to_xcom,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=10)
    )

    run_this_task2 = PythonOperator(
        task_id='456',
        python_callable=run_this_func,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=10)
    )
    run_this_task >> run_this_task2
