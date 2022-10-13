from datetime import datetime, timedelta
from email.mime import image
from unicodedata import name

from airflow import DAG
from airflow.configuration import conf
from airflow.providers.google.cloud.operators import bigquery
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

import logging

# ===================================================================
# DAG defaults setup 
# ===================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

airflow_base_path = '/home/airflow'
airflow_dags_path = airflow_base_path + '/dags'
airflow_scripts_path = airflow_dags_path + '/scripts'


secret_env = Secret(
    # Expose the secret as environment variable.
    deploy_type='env',
    # The name of the environment variable, since deploy_type is `env` rather
    # than `volume`.
    deploy_target='SQL_CONN',
    # Name of the Kubernetes Secret
    secret='airflow-secrets',
    # Key of a secret stored in this Secret object
    key='sql_alchemy_conn')
secret_volume = Secret(
    deploy_type='volume',
    # Path where we mount the secret as volume
    deploy_target=airflow_base_path,
    # Name of Kubernetes Secret
    secret='service-account',
    # Key in the form of service account file name
    key='key.json')

# ===================================================================
# Setup Volume Mounts for K8s Pods
# ===================================================================

volume_mount = k8s.V1VolumeMount(
    name='scripts', mount_path=airflow_dags_path, sub_path=None, read_only=False
)
volume = k8s.V1Volume(
    name='scripts',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='scripts'),
)

# ===================================================================
# Get k8s namespaces
# ===================================================================

namespace = conf.get('kubernetes', 'NAMESPACE')
# This will detect the default namespace locally and read the
# environment namespace when deployed to Astronomer.
if namespace =='default':
    config_file = airflow_base_path + '/.kube/config'
    in_cluster = False
else:
    in_cluster = True
    config_file = None

# Print/Log the namespace for debug
logging.info('The namespace is: ' + namespace)
print('Namespace: {} !'.format(namespace))

dag = DAG('kubernetes_pod_example', schedule_interval='@once', default_args=default_args)

with dag:
    # TODO: Test passing key.json as a Secret Object to K8s cluster from DAG
    # kubernetes_secret_vars_ex = KubernetesPodOperator(
    #     task_id='ex-kube-secrets',
    #     name='ex-kube-secrets',
    #     namespace='default',
    #     image='ubuntu',
    #     startup_timeout_seconds=300,
    #     # The secrets to pass to Pod, the Pod will fail to create if the
    #     # secrets you specify in a Secret object do not exist in Kubernetes.
    #     secrets=[secret_env, secret_volume],
    #     # env_vars allows you to specify environment variables for your
    #     # container to use. env_vars is templated.
    #     env_vars={
    #         #'EXAMPLE_VAR': '/example/value',
    #         'GOOGLE_APPLICATION_CREDENTIALS': '/home/airflow/key.json'})

    # Initial task to test connection to Kubernetes cluster
    task_one = KubernetesPodOperator(
        namespace="default",
        image="us-west1-docker.pkg.dev/ludis-airflow-v2/ludis-r-airflow/ludis-r-airflow",
        labels={"task1": "ludis-r-airflow"},
        name="airflow-test-pod",
        task_id="k8s_sample_task_one",
        in_cluster=False,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context="docker-desktop",  # is ignored when in_cluster is set to True
        #config_file=config_file,
        is_delete_operator_pod=True,
        get_logs=True,
        volumes=[volume],
        volume_mounts=[volume_mount],
    )

    # Second parallel task to test connection to Kubernetes cluster
    task_two = KubernetesPodOperator(
        namespace=namespace,
        image="us-west1-docker.pkg.dev/ludis-airflow-v2/ludis-r-airflow/ludis-r-airflow",
        labels={"task2": "ludis-r-airflow"},
        name="airflow-test-pod",
        task_id="k8s_sample_task_two",
        in_cluster=in_cluster,  # if set to true, will look in the cluster, if false, looks for file
        cluster_context="docker-desktop",  # is ignored when in_cluster is set to True
        #config_file=config_file,
        is_delete_operator_pod=True,
        get_logs=True,
        volumes=[volume],
        volume_mounts=[volume_mount],
    )

[task_one, task_two]