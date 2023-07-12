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
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
# Import your python def
# from 91ae0cea-be83-4fcc-9fae-08948312b733.[file] import [def]

home = os.environ['HOME']

# Using variables
# airflow_variables = Variable.get("91ae0cea-be83-4fcc-9fae-08948312b733", deserialize_json=True)
# variable_name = airflow_variables["variable_name"]

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    dag_id='r_helloworld_dag',
    default_args=default_args,
    description='An example R DAG',
    schedule_interval='00 12 * * *'
)


# Python helper function
def my_task():
    print("Hello World")
    # Add your function helpe

with dag:

    # Bash Operators can be used to run Bash Scripts
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Executing Bash Task"',
    )

    R_task = BashOperator(
        task_id='R_task',
        bash_command='Rscript ' + home + '/dags/TestDAGs/language_framwork_examples/R/hello_world.R',
    )

    # Example of using a Python Operator to wrap a function as a task
    python_task = PythonOperator(
       task_id='python_task',
       python_callable=my_task,
       dag=dag
    )


    # Set task dependecies
    bash_task >> python_task >> R_task

    # Add documentation
    bash_task.doc_md = "Bash task documentation"
    python_task.doc_md = "Python task documentation"
    R_task.doc_md = "R task documentation"