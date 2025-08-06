from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_airflow():
    print("Hello Airflow world! This is a minimal test task.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id="test_minimal_dag",
    schedule=None,
    default_args=default_args,
    catchup=False,
    tags=["test"]
) as dag:

    test_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello_airflow,
    )
