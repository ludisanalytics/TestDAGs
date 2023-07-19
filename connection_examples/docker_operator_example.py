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
from docker.types import Mount
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime
import requests
import json

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
def my_task(type='html'):

    my_api_token = 'EQfQne0KQkKpshK'
    url = 'https://staging-backend.ludisanalytics.com/v1/api/ludisurl/d0035f59-a601-4735-a086-fe3ab5021b80'
    headers = {'x-api-key': my_api_token}
    body={"path": f"MikemacTest{type}.{type}"}
    res = requests.put(url, headers=headers, data=body)

    # Read the contents of the local HTML file
    output_file_path = f"/home/airflow/dags/TestDAGs/language_framwork_examples/R/r_markdown_example_scripts/Long_RMd_example.{type}"
    # Encode the HTML content as UTF-8
    if type=='html':
        with open(output_file_path, 'r') as file:
            output_content = file.read()
        encoded_content = output_content.encode('utf-8')
    else:
        with open(output_file_path, 'rb') as file:
            output_content = file.read()
        encoded_content = output_content
    # Send the encoded content to the server using a POST request
    res2 = requests.put(res.url, data=encoded_content)


with dag:

    # Example of using a Python Operator to wrap a function as a task
    html_upload_task = PythonOperator(
       task_id='html_upload_task',
       python_callable=my_task,
       op_kwargs={'type': 'html'},
       dag=dag
    )

    pdf_upload_task = PythonOperator(
       task_id='pdf_upload_task',
       python_callable=my_task,
       op_kwargs={'type': 'pdf'},
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
        task_id='docker_R_HTML',
        image='rocker/r-ver:latest',
        container_name='run_rmd_html',
        api_version='auto',
        auto_remove=True,
        mounts=[ Mount(
            source='/home/ubuntu/local-airflow-cluster/dags/TestDAGs/language_framwork_examples/R/r_markdown_example_scripts/',
            target='/home/airflow/dags/',
            type='bind'
        )],
        command=[
            "/bin/bash",
            "-c",
            "apt update; "
            "apt install pandoc -y; "
            "R -e \"install.packages('knitr')\"; "
            "R -e \"install.packages('ggplot2', repos='http://cran.rstudio.com/')\"; "
            "R -e \"install.packages('rmarkdown')\"; "
            "R -e \"rmarkdown::render('/home/airflow/dags/Long_RMd_example.rmd',output_file='/home/airflow/dags/Long_RMd_example.html')\" "
            "pandoc -s /home/airflow/dags/Long_RMd_example.html -o /home/airflow/dags/Long_RMd_HTML_example.pdf"
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
        )

    t3 = DockerOperator(
        task_id='docker_R_PDF',
        image='rocker/r-ver:latest',
        container_name='run_rmd_pdf',
        api_version='auto',
        auto_remove=True,
        mounts=[ Mount(
            source='/home/ubuntu/local-airflow-cluster/dags/TestDAGs/language_framwork_examples/R/r_markdown_example_scripts/',
            target='/home/airflow/dags/',
            type='bind'
        )],
        command=[
            "/bin/bash",
            "-c",
            "apt update; "
            "apt install pandoc -y; "
            "R -e \"install.packages('knitr')\"; "
            "R -e \"install.packages('tinytex')\"; "
            "R -e \"tinytex::install_tinytex()\"; "
            "R -e \"install.packages('ggplot2', repos='http://cran.rstudio.com/')\"; "
            "R -e \"install.packages('rmarkdown')\"; "
            "R -e \"rmarkdown::render('/home/airflow/dags/Long_RMd_example.rmd',output_file='/home/airflow/dags/Long_RMd_example.pdf')\" "
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge"
        )

    # Simple Docker Operator to run 'Hello World'
    # t3 = DockerOperator(
    #     task_id='docker_command_hello',
    #     image='ubuntu:latest',
    #     container_name='task_command_hello',
    #     api_version='auto',
    #     auto_remove=True,
    #     command="echo 'Hello World'",
    #     docker_url="unix://var/run/docker.sock",
    #     network_mode="bridge"
    #     )

    # Simple Bash Operator to run 'Hello World'
    t4 = BashOperator(
        task_id='print_hello',
        bash_command='echo "hello world"'
        )

    ### Set the Task Structure
    start_dag >> t1
    t1 >> t2 >> html_upload_task >> t4
    t1 >> t3 >> pdf_upload_task >> t4
    t4 >> end_dag