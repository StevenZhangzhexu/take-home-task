from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import yaml
import os

# DAG configuration
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bd_transformer_pipeline',
    default_args=default_args,
    description='BD Transformer Pipeline using Spark',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['spark', 'transformer', 'data_processing'],
)


def load_config(**context):
    """Load configuration from YAML file."""
    config_path = context['dag_run'].conf.get('config_path', 'config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Store config in XCom for downstream tasks
    context['task_instance'].xcom_push(key='config', value=config)
    return config


def fit_column_transformer(column_name: str, **context):
    """Fit transformer for a specific column."""
    config = context['task_instance'].xcom_pull(task_ids='load_config', key='config')
    column_config = config[column_name]
    
    # This would be implemented as a Spark job
    print(f"Fitting transformer for column: {column_name}")
    print(f"Column config: {column_config}")
    
    # In a real implementation, this would submit a Spark job
    # For now, we'll just simulate the process
    return f"fitted_{column_name}"


def transform_column_data(column_name: str, **context):
    """Transform data for a specific column."""
    config = context['task_instance'].xcom_pull(task_ids='load_config', key='config')
    column_config = config[column_name]
    
    print(f"Transforming data for column: {column_name}")
    print(f"Column config: {column_config}")
    
    # In a real implementation, this would submit a Spark job
    return f"transformed_{column_name}"


def inverse_transform_column_data(column_name: str, **context):
    """Inverse transform data for a specific column."""
    config = context['task_instance'].xcom_pull(task_ids='load_config', key='config')
    column_config = config[column_name]
    
    print(f"Inverse transforming data for column: {column_name}")
    print(f"Column config: {column_config}")
    
    # In a real implementation, this would submit a Spark job
    return f"inverse_transformed_{column_name}"


def validate_results(**context):
    """Validate the results of the pipeline."""
    print("Validating pipeline results...")
    
    # In a real implementation, this would compare results
    # and check for any errors or anomalies
    
    return "validation_passed"


# Task 1: Load configuration
load_config_task = PythonOperator(
    task_id='load_config',
    python_callable=load_config,
    dag=dag,
)

# Task 2: Fit transformers for each column (parallel)
fit_tasks = []
transform_tasks = []
inverse_transform_tasks = []

# We'll create tasks for each column in the config
# For demonstration, we'll use the columns from the config.yaml
columns = ['day_of_month', 'height', 'account_balance', 'net_profit', 'customer_ratings', 'leaderboard_rank']

for column in columns:
    # Fit task
    fit_task = PythonOperator(
        task_id=f'fit_{column}',
        python_callable=fit_column_transformer,
        op_kwargs={'column_name': column},
        dag=dag,
    )
    fit_tasks.append(fit_task)
    
    # Transform task
    transform_task = PythonOperator(
        task_id=f'transform_{column}',
        python_callable=transform_column_data,
        op_kwargs={'column_name': column},
        dag=dag,
    )
    transform_tasks.append(transform_task)
    
    # Inverse transform task
    inverse_task = PythonOperator(
        task_id=f'inverse_transform_{column}',
        python_callable=inverse_transform_column_data,
        op_kwargs={'column_name': column},
        dag=dag,
    )
    inverse_transform_tasks.append(inverse_task)

# Task 3: Validate results
validate_task = PythonOperator(
    task_id='validate_results',
    python_callable=validate_results,
    dag=dag,
)

# Set up task dependencies
load_config_task >> fit_tasks
fit_tasks >> transform_tasks
transform_tasks >> inverse_transform_tasks
inverse_transform_tasks >> validate_task 