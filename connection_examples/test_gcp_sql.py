import os
from airflow.contrib.operators.gcp_sql_operator import CloudSqlQueryOperator
from airflow import models
from airflow.utils.dates import days_ago

postgres_kwargs = dict(
    project_id='ludis-airflow-v2',
    location='us-west1',
    instance='ludis-crypto-v2',
    database='postgres',
    user='postgres',
    password='123456',
    public_ip='35.247.70.12',
    public_port=5432
)

default_args = {
    'start_date': days_ago(1)
}

#os.environ['AIRFLOW_CONN_PUBLIC_POSTGRES_TCP'] = \
os.environ['AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT'] = \
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?" \
    "database_type=postgres&" \
    "project_id={project_id}&" \
    "location={location}&" \
    "instance={instance}&" \
    "use_proxy=False&" \
    "use_ssl=False".format(**postgres_kwargs)

SQL = [
    'CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)',
    'CREATE TABLE IF NOT EXISTS TABLE_TEST3 (I INTEGER)',  # shows warnings logged
    'INSERT INTO TABLE_TEST3 VALUES (0)',
    'INSERT INTO TABLE_TEST3 VALUES (1)',
    'INSERT INTO TABLE_TEST3 VALUES (2)',
    'INSERT INTO TABLE_TEST3 VALUES (3)',
    'CREATE TABLE IF NOT EXISTS TABLE_TEST2 (I INTEGER)',
    'DROP TABLE TABLE_TEST',
    'DROP TABLE TABLE_TEST2',
    'SELECT * FROM TABLE_TEST3'
]

with models.DAG(
    dag_id = 'test_gcp_sql',
    default_args=default_args,
    schedule_interval=None
) as dag:
    task_test_connection = CloudSqlQueryOperator(
        #gcp_cloudsql_conn_id="public_postgres_tcp",
        gcp_cloudsql_conn_id="google_cloud_default",
        task_id = "gcp_sql_example",
        sql=SQL
    )

task_test_connection