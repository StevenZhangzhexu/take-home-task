import sys
sys.path.append('/Users/zhangzhexu/Downloads/take-home-task')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yaml
import pandas as pd
from pyspark.sql import SparkSession
from bd_transformer.spark_transformer import SparkTransformer
from performance_monitor import RealTimeMonitor
import time
import os
import matplotlib.pyplot as plt


def load_config(config_path: str) -> dict:
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def pandas_to_spark_df(pandas_df: pd.DataFrame, spark: SparkSession):
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
    schema = StructType()
    for col_name, dtype in pandas_df.dtypes.items():
        if dtype == 'object':
            schema.add(StructField(col_name, StringType(), True))
        elif dtype in ['int64', 'int32']:
            schema.add(StructField(col_name, IntegerType(), True))
        else:
            schema.add(StructField(col_name, DoubleType(), True))
    return spark.createDataFrame(pandas_df, schema)

def plot_metrics(metrics: dict, output_path: str):
    timestamps = metrics['timestamps']
    plt.figure(figsize=(12, 8))
    plt.plot(timestamps, metrics['cpu_usage'], label='CPU Usage (%)')
    plt.plot(timestamps, metrics['memory_usage'], label='Memory Usage (%)')
    plt.plot(timestamps, metrics['disk_io'], label='Disk IO (MB/s)')
    plt.plot(timestamps, metrics['network_io'], label='Network IO (MB/s)')
    plt.xlabel('Time (s)')
    plt.ylabel('Usage')
    plt.title('System Performance Over Time')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    print(f"Performance plot saved to: {output_path}")

def run_pipeline(**kwargs):
    data_path = kwargs['data_path']
    config_path = kwargs['config_path']
    output_dir = kwargs.get('output_dir', 'results')
    os.makedirs(output_dir, exist_ok=True)

    config = load_config(config_path)
    spark = (
        SparkSession.builder
        .appName("AirflowSparkPipeline")
        .config("spark.default.parallelism", 150)
        .config("spark.sql.shuffle.partitions", 120)
        .config("spark.driver.memory", "8g")
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .getOrCreate()
    )

    monitor = RealTimeMonitor(output_file=os.path.join(output_dir, "perf_metrics.json"))
    monitor.start_monitoring()

    data = pd.read_parquet(data_path)
    spark_df = pandas_to_spark_df(data, spark)
    transformer = SparkTransformer(config, spark)

    print("Fitting transformer...")
    fit_start = time.time()
    transformer.fit(spark_df)
    fit_time = time.time() - fit_start

    print("Transforming data...")
    transform_start = time.time()
    transformed = transformer.transform(spark_df)
    transformed_count = transformed.count()
    transform_time = time.time() - transform_start

    print("Inverse transforming data...")
    inverse_start = time.time()
    inversed = transformer.inverse_transform(transformed)
    inversed_count = inversed.count()
    inverse_time = time.time() - inverse_start

    monitor.stop_monitoring()

    metrics_plot = os.path.join(output_dir, "system_performance_plot.png")
    plot_metrics(monitor.metrics, metrics_plot)

    summary = {
        'fit_time': fit_time,
        'transform_time': transform_time,
        'inverse_time': inverse_time,
        'transformed_count': transformed_count,
        'inversed_count': inversed_count
    }

    with open(os.path.join(output_dir, 'summary.json'), 'w') as f:
        import json
        json.dump(summary, f, indent=2)

    print("Pipeline completed. Summary:", summary)

def get_default_args():
    return {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2025, 1, 1)
    }

def build_dag():
    return DAG(
        dag_id='spark_pipeline_with_monitoring',
        default_args=get_default_args(),
        description='Run Spark pipeline with system and Spark monitoring',
        schedule=None,
        catchup=False,
        tags=['spark', 'monitoring']
    )

dag = build_dag()

with dag:
    pipeline_task = PythonOperator(
        task_id='run_spark_pipeline',
        python_callable=run_pipeline,
        op_kwargs={
            'data_path': '/path/to/data.parquet',
            'config_path': '/path/to/config.yaml',
            'output_dir': '/path/to/results'
        }
    )

    pipeline_task
