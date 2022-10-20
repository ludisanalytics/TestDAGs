
# Import python packages for OS and Logging
import logging, os

# Import the required DAG classes and operators
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Import the default arguments for the DAG from default_args.py
import TestDAGs.connection_examples.default_args as default_args
# Make changes to the default args
default_args.default_args['email'] = 'mikemac@ludisanalytics.com'

# Check prod vs dev file paths
import TestDAGs.connection_examples.gcp_development
gcp_connection_id = 'google_cloud_dev'
# If running on the production DB, then this section will import the additional contract addresses
# and the correct GCP connection.
# prod_file_path = os.path.join(os.getcwd(), 'gcs', 'allium_ingest', 'gcp_production.py')
prod_file_path = '${HOME}/dags/TestDAGs/connection_examples/'
if os.path.isfile(os.path.join(prod_file_path, 'gcp_production.py')):
    import TestDAGs.connection_examples.gcp_production
    gcp_connection_id = 'google_cloud_prod'

### PYTHON FUNCTIONS
def log_context(**kwargs):
    for key, value in kwargs.items():
        print(f"Context key {key} = {value}")
        logging.info(f"Context key {key} = {value}")

def compute_product(a=None, b=None):
    logging.info(f"Inputs: a={a}, b={b}")
    if a == None or b == None:
        return None
    return a * b

def create_logging_task(log_string):
    task = PythonOperator(
        task_id="task1",
        python_callable=log_context,
        op_kwargs={'gcp_conn': log_string},
        dag=dag
    )
    return task

### DAG Definition
with DAG(
    'prod_path_test',
    default_args=default_args.default_args,
    schedule_interval=default_args.schedule_interval,
    start_date=default_args.start_date,
    max_active_runs=default_args.max_active_runs,
    max_active_tasks=default_args.max_active_tasks,
    catchup=default_args.catchup
) as dag: 

    ### TASKS (OPERATORS)
    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    t2 = PythonOperator(
        task_id="task2",
        python_callable=compute_product,
        op_kwargs={'a': 3, 'b': 5},
        dag=dag
    )

    task_list = []
    # for param in default_args.default_args:
    #     logging.info('The current key value pair is: ', param)
    #     task_list.append(create_logging_task(param))
    task_list.append(create_logging_task(gcp_connection_id))

start >> task_list >> t2 >> end