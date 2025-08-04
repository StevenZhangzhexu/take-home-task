from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'spark_transformer_pipeline',
    default_args=default_args,
    description='Spark-based BD Transformer Pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['spark', 'transformer', 'data_processing'],
)


def prepare_spark_job(**context):
    """Prepare Spark job parameters and configuration."""
    # Get configuration from DAG run
    config_path = context['dag_run'].conf.get('config_path', 'config.yaml')
    data_path = context['dag_run'].conf.get('data_path', 'data/large/')
    output_path = context['dag_run'].conf.get('output_path', 'results/')
    
    # Store parameters in XCom
    context['task_instance'].xcom_push(key='config_path', value=config_path)
    context['task_instance'].xcom_push(key='data_path', value=data_path)
    context['task_instance'].xcom_push(key='output_path', value=output_path)
    
    print(f"Prepared Spark job with:")
    print(f"  Config path: {config_path}")
    print(f"  Data path: {data_path}")
    print(f"  Output path: {output_path}")
    
    return "prepared"


def fit_spark_transformer(**context):
    """Fit the Spark transformer on the data."""
    config_path = context['task_instance'].xcom_pull(task_ids='prepare_spark_job', key='config_path')
    data_path = context['task_instance'].xcom_pull(task_ids='prepare_spark_job', key='data_path')
    
    print(f"Fitting Spark transformer with config: {config_path}")
    print(f"Data path: {data_path}")
    
    # In a real implementation, this would be a Spark job
    # For now, we'll simulate the process
    return "fitted"


def transform_spark_data(**context):
    """Transform data using the fitted Spark transformer."""
    config_path = context['task_instance'].xcom_pull(task_ids='prepare_spark_job', key='config_path')
    data_path = context['task_instance'].xcom_pull(task_ids='prepare_spark_job', key='data_path')
    output_path = context['task_instance'].xcom_pull(task_ids='prepare_spark_job', key='output_path')
    
    print(f"Transforming data with config: {config_path}")
    print(f"Data path: {data_path}")
    print(f"Output path: {output_path}")
    
    # In a real implementation, this would be a Spark job
    return "transformed"


def inverse_transform_spark_data(**context):
    """Inverse transform data using the fitted Spark transformer."""
    config_path = context['task_instance'].xcom_pull(task_ids='prepare_spark_job', key='config_path')
    output_path = context['task_instance'].xcom_pull(task_ids='prepare_spark_job', key='output_path')
    
    print(f"Inverse transforming data with config: {config_path}")
    print(f"Output path: {output_path}")
    
    # In a real implementation, this would be a Spark job
    return "inverse_transformed"


def validate_spark_results(**context):
    """Validate the results of the Spark pipeline."""
    output_path = context['task_instance'].xcom_pull(task_ids='prepare_spark_job', key='output_path')
    
    print(f"Validating results in: {output_path}")
    
    # In a real implementation, this would check the results
    # and verify data quality
    
    return "validated"


# Task 1: Prepare Spark job
prepare_task = PythonOperator(
    task_id='prepare_spark_job',
    python_callable=prepare_spark_job,
    dag=dag,
)

# Task 2: Fit Spark transformer
fit_task = PythonOperator(
    task_id='fit_spark_transformer',
    python_callable=fit_spark_transformer,
    dag=dag,
)

# Task 3: Transform data with Spark
transform_task = PythonOperator(
    task_id='transform_spark_data',
    python_callable=transform_spark_data,
    dag=dag,
)

# Task 4: Inverse transform data with Spark
inverse_transform_task = PythonOperator(
    task_id='inverse_transform_spark_data',
    python_callable=inverse_transform_spark_data,
    dag=dag,
)

# Task 5: Validate results
validate_task = PythonOperator(
    task_id='validate_spark_results',
    python_callable=validate_spark_results,
    dag=dag,
)

# Set up task dependencies
prepare_task >> fit_task >> transform_task >> inverse_transform_task >> validate_task 