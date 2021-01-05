from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
import random
import requests

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

dag = DAG('billomat', default_args=default_args, schedule_interval=None)

def billomat_run():
    url = 'https://helpcheck.billomat.net/api/invoices?api_key=1c06f5590c6c626b45a26ce30ebff7cd&format=json'
    r = requests.get(url)
    if r.status_code == 200:
        print('Success!')
    elif r.status_code == 404:
        print('Not Found.')
    else :
        print('No Success')

def billomat_print():
    print(r)

#
run_billomat_run = PythonOperator(
    task_id='1',
    python_callable=billomat_run
#   provide_context=True,
#   retries=10,
#   retry_delay=timedelta(seconds=10)
)

run_billomat_print = PythonOperator(
    task_id='2',
    python_callable=billomat_print
)

run_billomat_run >> run_billomat_print
