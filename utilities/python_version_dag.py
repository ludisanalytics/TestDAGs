import time
from datetime import timedelta, datetime
from random import randint
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def test(**context):
    time.sleep(5)
    ls = randint(1,1000)
    return ls

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email': ['mikemac@ludisanalytics.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(dag_id='python_version_dag', default_args=default_args, start_date=datetime(2021,1,1), schedule_interval="@daily", catchup=False)

with dag:
    task_start = BashOperator(
        task_id='start',
        bash_command='python3 --version'
    )