from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import yaml
import os
import sys
import time
import json
import requests
from typing import Dict, Optional

# Add the project root to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from performance_monitor import (
    RealTimeMonitor, 
    SparkMetricsCollector, 
    monitor_pipeline_with_spark,
    create_spark_session_with_monitoring,
    analyze_performance_metrics,
    save_monitoring_results
)

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
    'enhanced_spark_transformer_pipeline',
    default_args=default_args,
    description='Enhanced Spark-based BD Transformer Pipeline with Real-Time Monitoring',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['spark', 'transformer', 'data_processing', 'monitoring'],
)


def prepare_enhanced_spark_job(**context):
    """Prepare enhanced Spark job with monitoring configuration."""
    # Get configuration from DAG run
    config_path = context['dag_run'].conf.get('config_path', 'config.yaml')
    data_path = context['dag_run'].conf.get('data_path', 'data/large/')
    output_path = context['dag_run'].conf.get('output_path', 'results/')
    monitoring_enabled = context['dag_run'].conf.get('monitoring_enabled', True)
    
    # Load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Store parameters in XCom
    context['task_instance'].xcom_push(key='config_path', value=config_path)
    context['task_instance'].xcom_push(key='data_path', value=data_path)
    context['task_instance'].xcom_push(key='output_path', value=output_path)
    context['task_instance'].xcom_push(key='monitoring_enabled', value=monitoring_enabled)
    context['task_instance'].xcom_push(key='config', value=config)
    
    print(f"Prepared enhanced Spark job with:")
    print(f"  Config path: {config_path}")
    print(f"  Data path: {data_path}")
    print(f"  Output path: {output_path}")
    print(f"  Monitoring enabled: {monitoring_enabled}")
    
    return "prepared"


def run_enhanced_spark_pipeline(**context):
    """Run the complete Spark pipeline with enhanced monitoring."""
    config_path = context['task_instance'].xcom_pull(task_ids='prepare_enhanced_spark_job', key='config_path')
    data_path = context['task_instance'].xcom_pull(task_ids='prepare_enhanced_spark_job', key='data_path')
    output_path = context['task_instance'].xcom_pull(task_ids='prepare_enhanced_spark_job', key='output_path')
    monitoring_enabled = context['task_instance'].xcom_pull(task_ids='prepare_enhanced_spark_job', key='monitoring_enabled')
    config = context['task_instance'].xcom_pull(task_ids='prepare_enhanced_spark_job', key='config')
    
    print(f"Running enhanced Spark pipeline...")
    print(f"  Config: {config_path}")
    print(f"  Data: {data_path}")
    print(f"  Output: {output_path}")
    print(f"  Monitoring: {monitoring_enabled}")
    
    # Create Spark session with monitoring-friendly configuration
    spark = create_spark_session_with_monitoring(
        app_name="EnhancedBDTransformer",
        master="local[*]",  # For local testing
        spark_driver_memory="4g",
        spark_executor_memory="4g"
    )
    
    def run_pipeline():
        """Run the complete pipeline."""
        import pandas as pd
        from bd_transformer.spark_transformer import SparkTransformer
        from performance_monitor import pandas_to_spark_df
        
        # Load data
        print("Loading data...")
        data = pd.read_parquet(data_path)
        spark_data = pandas_to_spark_df(data, spark)
        print(f"Data shape: {data.shape}")
        
        # Run pipeline
        print("Running Spark transformer...")
        transformer = SparkTransformer(config, spark)
        
        print("Fitting transformer...")
        transformer.fit(spark_data)
        
        print("Transforming data...")
        transformed = transformer.transform(spark_data)
        
        print("Inverse transforming data...")
        inversed = transformer.inverse_transform(transformed)
        
        # Save results
        os.makedirs(output_path, exist_ok=True)
        transformed_pdf = transformed.toPandas()
        inversed_pdf = inversed.toPandas()
        
        transformed_pdf.to_parquet(f"{output_path}/transformed_data.parquet")
        inversed_pdf.to_parquet(f"{output_path}/inversed_data.parquet")
        
        return {
            'data_shape': data.shape,
            'transformed_count': transformed.count(),
            'inversed_count': inversed.count(),
            'output_files': [
                f"{output_path}/transformed_data.parquet",
                f"{output_path}/inversed_data.parquet"
            ]
        }
    
    try:
        if monitoring_enabled:
            # Run with enhanced monitoring
            print("Running with enhanced monitoring...")
            monitoring_result = monitor_pipeline_with_spark(spark, run_pipeline)
            
            # Save monitoring results
            monitoring_output_dir = f"{output_path}/monitoring"
            monitoring_file = save_monitoring_results(monitoring_result, monitoring_output_dir)
            
            # Store monitoring info in XCom
            context['task_instance'].xcom_push(key='monitoring_result', value=monitoring_result)
            context['task_instance'].xcom_push(key='monitoring_file', value=monitoring_file)
            context['task_instance'].xcom_push(key='spark_ui_url', value=monitoring_result.get('spark_ui_url'))
            
            # Analyze performance
            analysis = analyze_performance_metrics(monitoring_result)
            context['task_instance'].xcom_push(key='performance_analysis', value=analysis)
            
            print(f"Pipeline completed successfully with monitoring")
            print(f"  Execution time: {monitoring_result['execution_time']:.2f} seconds")
            print(f"  Spark UI: {monitoring_result.get('spark_ui_url', 'N/A')}")
            print(f"  Monitoring results: {monitoring_file}")
            
            return monitoring_result.get('result', {})
        else:
            # Run without monitoring
            print("Running without monitoring...")
            result = run_pipeline()
            context['task_instance'].xcom_push(key='pipeline_result', value=result)
            return result
            
    finally:
        # Clean up Spark session
        spark.stop()


def monitor_spark_ui(**context):
    """Monitor Spark UI during pipeline execution."""
    spark_ui_url = context['task_instance'].xcom_pull(task_ids='run_enhanced_spark_pipeline', key='spark_ui_url')
    
    if not spark_ui_url:
        print("No Spark UI URL available for monitoring")
        return "no_spark_ui"
    
    print(f"Monitoring Spark UI: {spark_ui_url}")
    
    # In a real implementation, this would continuously monitor the Spark UI
    # For now, we'll just log the URL
    context['task_instance'].xcom_push(key='spark_ui_monitored', value=True)
    
    return "monitored"


def validate_enhanced_results(**context):
    """Validate the results of the enhanced Spark pipeline."""
    output_path = context['task_instance'].xcom_pull(task_ids='prepare_enhanced_spark_job', key='output_path')
    monitoring_enabled = context['task_instance'].xcom_pull(task_ids='prepare_enhanced_spark_job', key='monitoring_enabled')
    
    print(f"Validating enhanced results in: {output_path}")
    
    # Check if output files exist
    expected_files = [
        f"{output_path}/transformed_data.parquet",
        f"{output_path}/inversed_data.parquet"
    ]
    
    if monitoring_enabled:
        expected_files.append(f"{output_path}/monitoring/")
    
    validation_results = {}
    for file_path in expected_files:
        if os.path.exists(file_path):
            validation_results[file_path] = "exists"
            print(f"  ✓ {file_path}")
        else:
            validation_results[file_path] = "missing"
            print(f"  ✗ {file_path}")
    
    # Get performance analysis if monitoring was enabled
    if monitoring_enabled:
        analysis = context['task_instance'].xcom_pull(task_ids='run_enhanced_spark_pipeline', key='performance_analysis')
        if analysis:
            print("\nPerformance Analysis:")
            print(f"  Execution time: {analysis['execution_summary']['total_time']:.2f} seconds")
            print(f"  Success: {analysis['execution_summary']['success']}")
            
            if analysis.get('recommendations'):
                print("  Recommendations:")
                for rec in analysis['recommendations']:
                    print(f"    - {rec}")
    
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    
    return "validated"


def log_performance_metrics(**context):
    """Log performance metrics to Airflow logs."""
    monitoring_enabled = context['task_instance'].xcom_pull(task_ids='prepare_enhanced_spark_job', key='monitoring_enabled')
    
    if not monitoring_enabled:
        print("Monitoring was not enabled for this run")
        return "no_monitoring"
    
    monitoring_result = context['task_instance'].xcom_pull(task_ids='run_enhanced_spark_pipeline', key='monitoring_result')
    analysis = context['task_instance'].xcom_pull(task_ids='run_enhanced_spark_pipeline', key='performance_analysis')
    
    if monitoring_result and analysis:
        print("\n=== PERFORMANCE METRICS LOG ===")
        print(f"Execution Time: {monitoring_result['execution_time']:.2f} seconds")
        print(f"Success: {monitoring_result['success']}")
        
        if analysis.get('system_analysis'):
            sys_analysis = analysis['system_analysis']
            if 'cpu' in sys_analysis:
                cpu = sys_analysis['cpu']
                print(f"CPU Usage - Avg: {cpu['avg_usage']:.1f}%, Max: {cpu['max_usage']:.1f}%, Level: {cpu['utilization_level']}")
            
            if 'memory' in sys_analysis:
                mem = sys_analysis['memory']
                print(f"Memory Usage - Avg: {mem['avg_usage']:.1f}%, Max: {mem['max_usage']:.1f}%, Level: {mem['utilization_level']}")
        
        if analysis.get('spark_analysis'):
            spark_analysis = analysis['spark_analysis']
            if 'stages' in spark_analysis:
                stages = spark_analysis['stages']
                print(f"Spark Stages - Completed: {stages['completed']}, Failed: {stages['failed']}, Success Rate: {stages['success_rate']:.2%}")
            
            if 'resources' in spark_analysis:
                resources = spark_analysis['resources']
                print(f"Resource Utilization - Memory: {resources['memory_utilization']:.2%}, Cores: {resources['core_utilization']:.2%}")
        
        if analysis.get('recommendations'):
            print("Recommendations:")
            for rec in analysis['recommendations']:
                print(f"  - {rec}")
        
        print("=== END PERFORMANCE METRICS ===")
    
    return "logged"


# Task 1: Prepare enhanced Spark job
prepare_task = PythonOperator(
    task_id='prepare_enhanced_spark_job',
    python_callable=prepare_enhanced_spark_job,
    dag=dag,
)

# Task 2: Run enhanced Spark pipeline with monitoring
run_pipeline_task = PythonOperator(
    task_id='run_enhanced_spark_pipeline',
    python_callable=run_enhanced_spark_pipeline,
    dag=dag,
)

# Task 3: Monitor Spark UI (parallel with pipeline execution)
monitor_ui_task = PythonOperator(
    task_id='monitor_spark_ui',
    python_callable=monitor_spark_ui,
    dag=dag,
)

# Task 4: Validate enhanced results
validate_task = PythonOperator(
    task_id='validate_enhanced_results',
    python_callable=validate_enhanced_results,
    dag=dag,
)

# Task 5: Log performance metrics
log_metrics_task = PythonOperator(
    task_id='log_performance_metrics',
    python_callable=log_performance_metrics,
    dag=dag,
)

# Set up task dependencies
# Prepare -> Run Pipeline (with monitoring)
prepare_task >> run_pipeline_task

# Run Pipeline and Monitor UI can run in parallel
run_pipeline_task >> [validate_task, log_metrics_task]
monitor_ui_task >> validate_task

# Validate and Log Metrics are final tasks
validate_task >> log_metrics_task 