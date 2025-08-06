# dags/spark_pipeline_dag.py
import sys
sys.path.append('/Users/zhangzhexu/Downloads/take-home-task')

import os
python_bin = os.popen("which python").read().strip()

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import yaml
import pandas as pd
from pyspark.sql import SparkSession
from bd_transformer.spark_transformer_airflow import SparkTransformer
from bd_transformer.spark_components.converter import SparkConverter
from bd_transformer.spark_components.normalizer import SparkNormalizer
import time
import json
import logging
import traceback
import pickle
import gc
from pathlib import Path

logger = logging.getLogger(__name__)



# Global paths
DATA_PATH = '/Users/zhangzhexu/Downloads/take-home-task/data/large/'
CONFIG_PATH = '/Users/zhangzhexu/Downloads/take-home-task/config.yaml'
OUTPUT_DIR = '/Users/zhangzhexu/Downloads/take-home-task/results'
TEMP_DIR = '/tmp/airflow_spark_pipeline'

def ensure_dirs():
    """Ensure all required directories exist"""
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

def get_spark_session(app_name: str = "AirflowSparkTask"):
    """Create optimized Spark session for small tasks"""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.pyspark.python", python_bin)
        .config("spark.pyspark.driver.python", python_bin)
        .config("spark.default.parallelism", 50)
        .config("spark.sql.shuffle.partitions", 50)
        .config("spark.driver.memory", "3g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

def load_config(**kwargs):
    """Load configuration and store column list for downstream tasks"""
    ensure_dirs()

    import os, sys
    print("Current user:", os.getlogin())
    print("Current working directory:", os.getcwd())
    print("Python executable:", sys.executable)
    print("PYTHONPATH:", os.environ.get("PYTHONPATH"))
    
    # Also check if your path exists
    print("Path exists?", os.path.exists('/Users/zhangzhexu/Downloads/take-home-task'))
    
    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)
    
    # Save config for other tasks
    with open(f"{TEMP_DIR}/config.json", 'w') as f:
        json.dump(config, f)
    
    # Store column names for dynamic task generation
    column_names = list(config.keys())
    Variable.set("pipeline_columns", json.dumps(column_names))
    
    logger.info(f"Loaded config for {len(column_names)} columns: {column_names}")
    return column_names

def load_and_cache_data(**kwargs):
    """Load data once and cache basic info"""
    spark = None
    try:
        spark = get_spark_session("DataLoader")
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Loading data from {DATA_PATH}")
        df = spark.read.parquet(DATA_PATH)
        
        # Cache the dataframe for reuse
        df = df.cache()
        
        # Get basic info
        row_count = df.count()
        col_count = len(df.columns)
        
        # Save data info
        data_info = {
            'row_count': row_count,
            'col_count': col_count,
            'columns': df.columns,
            'data_cached': True
        }
        
        with open(f"{TEMP_DIR}/data_info.json", 'w') as f:
            json.dump(data_info, f)
        
        logger.info(f"Data loaded: {row_count} rows, {col_count} columns")
        return data_info
        
    finally:
        if spark:
            spark.stop()
        gc.collect()

def fit_column_transformer(column_name: str, **kwargs):
    """Fit transformer for a single column - lightweight and parallelizable"""
    spark = None
    try:
        spark = get_spark_session(f"FitColumn_{column_name}")
        spark.sparkContext.setLogLevel("WARN")
        
        # Load config
        with open(f"{TEMP_DIR}/config.json", 'r') as f:
            config = json.load(f)
        
        if column_name not in config:
            raise ValueError(f"Column {column_name} not found in config")
        
        col_config = config[column_name]
        logger.info(f"Fitting transformers for column: {column_name}")
        
        # Load data (should be cached from previous task)
        df = spark.read.parquet(DATA_PATH).cache()
        
        # Fit converter
        converter = SparkConverter(
            **col_config.get("converter", {}),
            spark=spark
        )
        converter.fit(df.select(column_name))
        
        # Convert data for normalizer fitting
        converted_df = converter.convert(df.select(column_name))
        
        # Fit normalizer
        normalizer = SparkNormalizer(
            **col_config.get("normalizer", {}),
            spark=spark
        )
        normalizer.fit(converted_df)
        
        # Save fitted transformers
        transformer_data = {
            'converter': {
                'min_val': converter._min_val,
                'max_val': converter._max_val,
                'clip_oor': converter._clip_oor,
                'prefix': converter._prefix,
                'suffix': converter._suffix,
                'rounding': converter._rounding,
                'type': converter._type.__name__ if hasattr(converter._type, '__name__') else 'unknown',
                'column_name': converter._column_name
            },
            'normalizer': {
                'min': normalizer._min,
                'max': normalizer._max,
                'scale': normalizer._scale,
                'clip': normalizer._clip,
                'column_name': normalizer._column_name
            }
        }
        
        with open(f"{TEMP_DIR}/transformer_{column_name}.json", 'w') as f:
            json.dump(transformer_data, f)
        
        logger.info(f"Column {column_name} transformers fitted and saved")
        return f"transformer_{column_name}.json"
        
    except Exception as e:
        logger.error(f"Error fitting column {column_name}: {traceback.format_exc()}")
        raise
    finally:
        if spark:
            spark.stop()
        gc.collect()

def build_and_apply_transformations(**kwargs):
    """Build complete transformer and apply all transformations"""
    spark = None
    try:
        start_time = time.time()
        spark = get_spark_session("TransformData")
        spark.sparkContext.setLogLevel("WARN")
        
        # Load config
        with open(f"{TEMP_DIR}/config.json", 'r') as f:
            config = json.load(f)
        
        # Load all fitted transformers
        transformers = {}
        for column_name in config.keys():
            with open(f"{TEMP_DIR}/transformer_{column_name}.json", 'r') as f:
                transformers[column_name] = json.load(f)
        
        logger.info("Creating complete SparkTransformer from fitted components")
        
        # Create SparkTransformer and populate with fitted transformers
        transformer = SparkTransformer(config, spark)
        
        # Reconstruct fitted transformers
        for col_name, transformer_data in transformers.items():
            # Reconstruct converter
            conv = SparkConverter(
                **config[col_name].get("converter", {}),
                spark=spark
            )
            conv_data = transformer_data['converter']
            conv._min_val = conv_data['min_val']
            conv._max_val = conv_data['max_val']
            conv._clip_oor = conv_data['clip_oor']
            conv._prefix = conv_data['prefix']
            conv._suffix = conv_data['suffix']
            conv._rounding = conv_data['rounding']
            conv._column_name = conv_data['column_name']
            # Reconstruct type
            if conv_data['type'] == 'str':
                conv._type = str
            else:
                conv._type = type(conv_data['type'])
            
            transformer.converters[col_name] = conv
            
            # Reconstruct normalizer
            norm = SparkNormalizer(
                **config[col_name].get("normalizer", {}),
                spark=spark
            )
            norm_data = transformer_data['normalizer']
            norm._min = norm_data['min']
            norm._max = norm_data['max']
            norm._scale = norm_data['scale']
            norm._clip = norm_data['clip']
            norm._column_name = norm_data['column_name']
            
            transformer.normalizers[col_name] = norm
        
        # Load data
        df = spark.read.parquet(DATA_PATH).cache()
        
        logger.info("Applying transformations...")
        
        # Transform
        transform_start = time.time()
        transformed = transformer.transform(df)
        transformed_count = transformed.count()
        transform_time = time.time() - transform_start
        
        # Inverse transform
        inverse_start = time.time()
        inversed = transformer.inverse_transform(transformed)
        inversed_count = inversed.count()
        inverse_time = time.time() - inverse_start
        
        total_time = time.time() - start_time
        
        # Save results
        results = {
            'transform_time': transform_time,
            'inverse_time': inverse_time,
            'total_time': total_time,
            'transformed_count': transformed_count,
            'inversed_count': inversed_count,
            'success': True
        }
        
        with open(f"{OUTPUT_DIR}/pipeline_results.json", 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Pipeline completed successfully: {results}")
        return results
        
    except Exception as e:
        logger.error(f"Error in transformation pipeline: {traceback.format_exc()}")
        raise
    finally:
        if spark:
            spark.stop()
        gc.collect()

def cleanup_temp_files(**kwargs):
    """Clean up temporary files"""
    import shutil
    try:
        if os.path.exists(TEMP_DIR):
            shutil.rmtree(TEMP_DIR)
        logger.info("Temporary files cleaned up")
    except Exception as e:
        logger.warning(f"Failed to clean temp files: {e}")

def get_default_args():
    return {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
        'start_date': datetime(2025, 1, 1)
    }

def build_dag():
    dag = DAG(
        dag_id='parallel_spark_pipeline',
        default_args=get_default_args(),
        description='Parallel Spark pipeline with column-wise fitting',
        schedule=None,
        catchup=False,
        tags=['spark', 'parallel', 'optimized'],
        max_active_runs=1,
    )
    
    with dag:
        # Step 1: Load configuration
        load_config_task = PythonOperator(
            task_id='load_config',
            python_callable=load_config,
        )
        
        # Step 2: Load and cache data
        load_data_task = PythonOperator(
            task_id='load_data',
            python_callable=load_and_cache_data,
        )
        
        # Step 3: Create dynamic tasks for each column (runs in parallel)
        # We'll use Variable to get column names and create tasks dynamically
        
        # Step 4: Build and apply transformations
        transform_task = PythonOperator(
            task_id='transform_data',
            python_callable=build_and_apply_transformations,
        )
        
        # Step 5: Cleanup
        cleanup_task = PythonOperator(
            task_id='cleanup',
            python_callable=cleanup_temp_files,
            trigger_rule='all_done'  # Run regardless of upstream success/failure
        )
        
        # Set up basic dependencies
        load_config_task >> load_data_task
        load_data_task >> transform_task >> cleanup_task
    
    return dag

# Create the DAG
dag = build_dag()

# Dynamic task creation - this runs when the DAG is parsed
try:
    # Try to get columns from Variable (will be set by load_config task on first run)
    columns_json = Variable.get("pipeline_columns", default_var=None)
    if columns_json:
        columns = json.loads(columns_json)
        
        # Create parallel tasks for each column
        with dag:
            column_tasks = []
            for col_name in columns:
                fit_task = PythonOperator(
                    task_id=f'fit_column_{col_name}',
                    python_callable=fit_column_transformer,
                    op_kwargs={'column_name': col_name},
                    pool='column_fit_pool'  # Limit concurrent column tasks
                )
                column_tasks.append(fit_task)
            
            # Set up dependencies
            load_data_task = dag.get_task('load_data')
            transform_task = dag.get_task('transform_data')
            
            # All column fitting tasks depend on data loading
            load_data_task >> column_tasks
            # Transform task depends on all column fitting tasks
            column_tasks >> transform_task
            
except Exception as e:
    # On first run or if Variable doesn't exist, tasks will be created after config loading
    logger.info("Dynamic tasks will be created after first config load")
    pass


