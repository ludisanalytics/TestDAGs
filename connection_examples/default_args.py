from datetime import datetime, timedelta

# DEFAULT ARGS PARAMETER
default_args = {
    'owner': 'airflow',
    'email': ['ayushg@ludisanalytics.com'],
    'email_on_failure': False,
    'email_on_retry': False, 
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# ADDITIONAL DAG PARAMETERS
schedule_interval = '@daily'
start_date=datetime(2022,9,1)
max_active_runs=1
max_active_tasks=3
catchup=False