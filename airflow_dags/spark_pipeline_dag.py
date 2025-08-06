# dags/spark_pipeline_dag.py
import sys
sys.path.append('/Users/zhangzhexu/Downloads/take-home-task')

import os
python_bin = os.popen("which python").read().strip()
print(python_bin)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yaml
import pandas as pd
from pyspark.sql import SparkSession
from bd_transformer.spark_transformer import SparkTransformer
from performance_monitor import RealTimeMonitor
import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import psutil
import json
import logging
import traceback

logger = logging.getLogger(__name__)

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
    plt.close()
    logger.info(f"Performance plot saved to: {output_path}")

def print_memory():
    vm = psutil.virtual_memory()
    logger.info(f"[Memory] Total: {vm.total/1e9:.2f} GB | Available: {vm.available/1e9:.2f} GB | Used: {vm.percent}%")

def run_pipeline(**kwargs):
    data_path = kwargs['data_path']
    config_path = kwargs['config_path']
    output_dir = kwargs.get('output_dir', 'results')
    os.makedirs(output_dir, exist_ok=True)

    logger.info("Loading config...")
    config = load_config(config_path)

    logger.info("Creating Spark session...")
    spark = (
        SparkSession.builder
        .appName("AirflowSparkPipeline")
        .config("spark.pyspark.python", python_bin)
        .config("spark.pyspark.driver.python", python_bin)
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

    logger.info("Starting system monitor...")
    monitor = RealTimeMonitor(output_file=os.path.join(output_dir, "perf_metrics.json"))
    monitor.start_monitoring()

    try:
        print_memory()
        logger.info(f"Reading parquet data directly with Spark: {data_path}")
        spark_df = spark.read.parquet(data_path)

        logger.info(f"DataFrame row count: {spark_df.count()}")
        logger.info(f"DataFrame column count: {len(spark_df.columns)}")

        print_memory()

        logger.info("Initializing transformer...")
        transformer = SparkTransformer(config, spark)

        logger.info("Fitting transformer...")
        fit_start = time.time()
        transformer.fit(spark_df)
        fit_time = time.time() - fit_start

        logger.info("Transforming data...")
        transform_start = time.time()
        transformed = transformer.transform(spark_df)
        logger.info("Counting transformed...")
        transformed_count = transformed.count()
        transform_time = time.time() - transform_start

        logger.info("Inverse transforming data...")
        inverse_start = time.time()
        inversed = transformer.inverse_transform(transformed)
        logger.info("Counting inversed...")
        inversed_count = inversed.count()
        inverse_time = time.time() - inverse_start

    except Exception as e:
        logger.error("ERROR during pipeline execution:\n%s", traceback.format_exc())
        monitor.stop_monitoring()
        raise e

    monitor.stop_monitoring()

    try:
        logger.info("Plotting performance metrics...")
        metrics_plot = os.path.join(output_dir, "system_performance_plot.png")
        plot_metrics(monitor.metrics, metrics_plot)
    except Exception as e:
        logger.error("Plotting failed:\n%s", traceback.format_exc())

    summary = {
        'fit_time': fit_time,
        'transform_time': transform_time,
        'inverse_time': inverse_time,
        'transformed_count': transformed_count,
        'inversed_count': inversed_count
    }

    with open(os.path.join(output_dir, 'summary.json'), 'w') as f:
        json.dump(summary, f, indent=2)

    logger.info("Pipeline completed. Summary: %s", summary)

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
            'data_path': '/Users/zhangzhexu/Downloads/take-home-task/data/large/',
            'config_path': '/Users/zhangzhexu/Downloads/take-home-task/config.yaml',
            'output_dir': '/Users/zhangzhexu/Downloads/take-home-task/results'
        },
    )

    pipeline_task
