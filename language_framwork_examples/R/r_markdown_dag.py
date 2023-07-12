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
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
home = os.environ['HOME']
# Import your python def
# from 91ae0cea-be83-4fcc-9fae-08948312b733.[file] import [def]

# Using variables
# airflow_variables = Variable.get("91ae0cea-be83-4fcc-9fae-08948312b733", deserialize_json=True)
# variable_name = airflow_variables["variable_name"]

default_args = {
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    dag_id='r_markdown_dag',
    default_args=default_args,
    description='An example R Markdown DAG',
    schedule_interval='00 12 * * *'
)


# Python helper function
def my_task():
    print("Hello World")
    # Add your function helpe

with dag:

    # Bash Operators can be used to run Bash Scripts
    html_task = BashOperator(
        task_id='html_output_task',
        # bash_command='Rscript /home/airflow/dags/TestDAGs/language_framwork_examples/R/r_markdown_example_scripts/HTML_MWE.Rmd',
        bash_command="Rscript -e \"rmarkdown::render('" + home + "/dags/TestDAGs/language_framwork_examples/R/r_markdown_example_scripts/HTML_MWE.Rmd', output_file='" + home + "/dags/TestDAGs/language_framwork_examples/R/r_markdown_example_scripts/HTML_MWE.html')\""
    )

    pdf_task = BashOperator(
        task_id='pdf_output_task',
        # bash_command='Rscript /home/airflow/dags/TestDAGs/language_framwork_examples/R/r_markdown_example_scripts/PDF_MWE.Rmd',
        bash_command="Rscript -e \"rmarkdown::render('" + home + "/dags/TestDAGs/language_framwork_examples/R/r_markdown_example_scripts/PDF_MWE.Rmd', output_file='" + home + "/dags/TestDAGs/language_framwork_examples/R/r_markdown_example_scripts/PDF_MWE.pdf')\""

    )

    # Example of using a Python Operator to wrap a function as a task
    python_task = PythonOperator(
       task_id='python_task',
       python_callable=my_task,
       dag=dag
    )

    end_task = DummyOperator(
       task_id='end_task',
       dag=dag
    )


    # Set task dependecies
    python_task >> pdf_task >> end_task
    python_task >> html_task >> end_task

    # Add documentation
    html_task.doc_md = "HTML RMd task documentation"
    python_task.doc_md = "Python task documentation"
    pdf_task.doc_md = "PDF Rmd task documentation"