######################################################
#
#              TEST PROJECT V2
#
#              Author: mikemac@ludisanalytics.com
#
#              Created on Ludis Analytics
#
#              Date: 2023-6-28
#
######################################################


# For more information
# https://ludisanalytics.github.io/documentation/#/deploy_workflows

# Imports

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    dag_id='docker_operator_example',
    default_args=default_args,
    description='An example R Docker DAG',
    schedule_interval='00 12 * * *'
)


# Python helper function
def my_task():
    print("Hello World")
    # Add your function helpe

with dag:

    # R_task = BashOperator(
    #     task_id='R_task',
    #     bash_command='Rscript /home/airflow/dags/test.R',
    # )

    # Example of using a Python Operator to wrap a function as a task
    python_task = PythonOperator(
       task_id='python_task',
       python_callable=my_task,
       dag=dag
    )

    start_dag = DummyOperator(
        task_id='start_dag'
        )

    end_dag = DummyOperator(
        task_id='end_dag'
        )

    t1 = BashOperator(
        task_id='print_current_date',
        bash_command='date'
        )

    t2 = DockerOperator(
        task_id='docker_R',
        image='rocker/r-ver:latest',
        container_name='run_r_script',
        api_version='auto',
        auto_remove=True,
        # volumes=['./:/home/airflow/dags/'],
        # command="Rscript /home/airflow/dags/hello_world.R",
        command="/bin/sleep 40 && echo 'Hello World'",
        # docker_url="unix://var/run/docker.sock",
        docker_url="//var/run/docker.sock",
        network_mode="bridge"
        )

    t3 = DockerOperator(
        task_id='docker_command_hello',
        image='ubuntu:latest',
        container_name='task_command_hello',
        api_version='auto',
        auto_remove=True,
        command="/bin/sleep 40 && echo 'Hello World'",
        #docker_url="unix://var/run/docker.sock",
        docker_url="//var/run/docker.sock",
        network_mode="bridge"
        )

    # Simple Bash Operator to run 'Hello World'
    t4 = BashOperator(
        task_id='print_hello',
        bash_command='echo "hello world"'
        )

    ### Set the Task Structure
    start_dag >> t1

    t1 >> t2 >> t4 >> python_task
    t1 >> t3 >> t4 >> python_task

    python_task >> end_dag