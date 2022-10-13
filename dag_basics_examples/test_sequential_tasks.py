import json
import time
import requests
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def get(url: str) -> None:
    endpoint = url.split('/')[-1]
    now = datetime.now()
    now = f"{now.year}-{now.month}-{now.day}T{now.hour}-{now.minute}-{now.second}"
    res = requests.get(url)
    res = json.loads(res.text)

    with open(f"/Users/dradecic/airflow/data/{endpoint}-{now}.json", 'w') as f:
        json.dump(res, f)
    time.sleep(2)

with DAG(
    dag_id='sequential_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 3, 1),
    catchup=False
) as dag:

    task_get_users = PythonOperator(
        task_id='get_users',
        python_callable=get,
        op_kwargs={'url': 'https://gorest.co.in/public/v2/users'}
    )

    task_get_posts = PythonOperator(
        task_id='get_posts',
        python_callable=get,
        op_kwargs={'url': 'https://gorest.co.in/public/v2/posts'}
    )

    task_get_comments = PythonOperator(
        task_id='get_comments',
        python_callable=get,
        op_kwargs={'url': 'https://gorest.co.in/public/v2/comments'}
    )

    task_get_todos = PythonOperator(
        task_id='get_todos',
        python_callable=get,
        op_kwargs={'url': 'https://gorest.co.in/public/v2/todos'}
    )

    task_get_users >> task_get_posts >> task_get_comments >> task_get_todos