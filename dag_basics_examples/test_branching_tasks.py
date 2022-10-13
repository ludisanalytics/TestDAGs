# Sample DAG from https://www.youtube.com/watch?v=IH1-0hwFZRQ&t=196s and https://marclamberti.com/blog/airflow-dag-creating-your-first-dag-in-5-minutes/

# Airflow imports
from audioop import avg
from airflow import DAG # Always need to import the DAG class for a DAG script
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

# Other imports
from datetime import datetime # Need to import to start a datetime
from random import randint

# Python Functions
def _training_model():
    ls = []
    for i in range(1,1000000):
        ls.append(randint(1,10))
    return ls

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C',
        'training_model_D',
        'training_model_E',
        'training_model_F',
        'training_model_G',
        'training_model_H',
        'training_model_I'
    ])
    avg_acc = []
    for i in range(1,len(accuracies)):
      avg_acc.append(avg(accuracies[i]))
    best_accuracy = max(avg_acc)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'

# Airflow DAG definition
with DAG("my_dag", start_date=datetime(2021,1,1), schedule_interval="@daily", catchup=False) as dag: # Created a DAG object, and all DAGs must have unique identifiers

    training_model_A = PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model
    )

    training_model_B = PythonOperator(
        task_id="training_model_B",
        python_callable=_training_model
    )
 
    training_model_C = PythonOperator(
        task_id="training_model_C",
        python_callable=_training_model
    )
    training_model_D = PythonOperator(
        task_id="training_model_D",
        python_callable=_training_model
    )

    training_model_E = PythonOperator(
        task_id="training_model_E",
        python_callable=_training_model
    )
 
    training_model_F = PythonOperator(
        task_id="training_model_F",
        python_callable=_training_model
    )
    training_model_G = PythonOperator(
        task_id="training_model_G",
        python_callable=_training_model
    )

    training_model_H = PythonOperator(
        task_id="training_model_H",
        python_callable=_training_model
    )
 
    training_model_I = PythonOperator(
        task_id="training_model_I",
        python_callable=_training_model
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=_choose_best_model
    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

    [training_model_A, training_model_B, training_model_C, training_model_D, training_model_E, training_model_F, training_model_G, training_model_H, training_model_I] >> choose_best_model >> [accurate, inaccurate]