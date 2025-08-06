import argparse
import os
import time
import psutil
import yaml
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from performance_monitor import RealTimeMonitor

from bd_transformer.transformer import Transformer
from bd_transformer.spark_transformer import SparkTransformer


def get_system_specs():
    """Get system specifications."""
    memory = psutil.virtual_memory()
    cpu_count = psutil.cpu_count()
    
    specs = {
        "total_memory_gb": memory.total / (1024**3),
        "available_memory_gb": memory.available / (1024**3),
        "cpu_count": cpu_count,
        "cpu_freq_mhz": psutil.cpu_freq().current if psutil.cpu_freq() else None
    }
    
    print("=== System Specifications ===")
    print(f"Total Memory: {specs['total_memory_gb']:.2f} GB")
    print(f"Available Memory: {specs['available_memory_gb']:.2f} GB")
    print(f"CPU Count: {specs['cpu_count']}")
    if specs['cpu_freq_mhz']:
        print(f"CPU Frequency: {specs['cpu_freq_mhz']:.2f} MHz")
    
    return specs


def pandas_to_spark_df(pandas_df: pd.DataFrame, spark: SparkSession) -> pd.DataFrame:
    """Convert pandas DataFrame to Spark DataFrame."""
    schema = StructType()
    for col_name, dtype in pandas_df.dtypes.items():
        if dtype == 'object':
            schema.add(StructField(col_name, StringType(), True))
        elif dtype in ['int64', 'int32']:
            schema.add(StructField(col_name, IntegerType(), True))
        else:
            schema.add(StructField(col_name, DoubleType(), True))
    
    return spark.createDataFrame(pandas_df, schema)


def test_spark_pipeline_enhanced(data_path: str, config: dict, output_dir: str = "results"):
    """Enhanced Spark pipeline test with real-time monitoring and Spark UI metrics."""
    
    # Initialize Spark with monitoring
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("EnhancedPipelineTest")
        .config("spark.default.parallelism", 200)
        .config("spark.sql.shuffle.partitions", 120)
        .config("spark.driver.memory", "4g")
        # Kryo serializer settings
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer", "64k")
        .config("spark.kryoserializer.buffer.max", "512m")
        # Adaptive execution settings
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .getOrCreate()
    )

    # spark = (
    #     SparkSession.builder
    #     .master("local[*]")  # Use all local cores (for testing on laptop)
    #     .appName("PipelineComparison")
    #     .config("spark.default.parallelism", 200)  # Default number of tasks for RDDs
    #     .config("spark.sql.shuffle.partitions", 120)  # Number of shuffle partitions (for joins/aggregations)
    #     .config("spark.driver.memory", "4g")  # Max memory for driver JVM
    #     .config("spark.kryoserializer.buffer.max", "512m")  # Max buffer for Kryo serialization
    #     .config("spark.sql.adaptive.enabled", "true")
    #     .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    #     .config("spark.sql.adaptive.skewJoin.enabled", "true")
    #     .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
    #     .getOrCreate()
    # )

    
    # Get Spark UI URL and app ID
    spark_ui_url = None
    app_id = None
    try:
        spark_context = spark.sparkContext
        app_id = spark_context.applicationId
        spark_ui_url = spark_context.uiWebUrl
        print(f"Spark UI available at: {spark_ui_url}")
    except Exception as e:
        print(f"Warning: Could not get Spark UI URL: {e}")
    
    # Load data
    data = pd.read_parquet(data_path)
    spark_data = pandas_to_spark_df(data, spark)
    print(f"Data shape: {data.shape}")
    
    # Create transformer
    transformer = SparkTransformer(config, spark)
    
    # Start continuous monitoring for the entire pipeline
    print("Starting continuous Spark monitoring...")
    continuous_monitor = RealTimeMonitor(output_file=f"{output_dir}/spark_continuous_metrics.json")
    continuous_monitor.start_monitoring()
    
    # Monitor fit operation
    print("Fitting transformer...")
    fit_start_time = time.time()
    
    transformer.fit(spark_data)
    
    fit_time = time.time() - fit_start_time
    print(f"Fit time: {fit_time:.2f} seconds")
    
    # Monitor transform operation
    print("Transforming data...")
    transform_start_time = time.time()
    
    transformed = transformer.transform(spark_data)
    
    # Force execution by calling count() to ensure the transformation actually happens
    print("Forcing transform execution...")
    transformed_count = transformed.count()
    
    transform_time = time.time() - transform_start_time
    print(f"Transform time (including execution): {transform_time:.2f} seconds")
    
    # Monitor inverse transform operation
    print("Inverse transforming data...")
    inverse_start_time = time.time()
    
    inversed = transformer.inverse_transform(transformed)
    
    # Force execution by calling count() to ensure the inverse transformation actually happens
    print("Forcing inverse transform execution...")
    inversed_count = inversed.count()
    
    inverse_time = time.time() - inverse_start_time
    print(f"Inverse transform time (including execution): {inverse_time:.2f} seconds")
    
    
    
    return {
        'fit_time': fit_time,
        'transform_time': transform_time,
        'inverse_time': inverse_time,

        'data_shape': data.shape,
        'transformed_count': transformed_count,
        'inversed_count': inversed_count,

        'spark_ui_url': spark_ui_url
    }



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, default="data/large/")
    parser.add_argument("--config_path", type=str, default="config.yaml")

    args = parser.parse_args()

    # Get system specs
    specs = get_system_specs()
    
    # Load config
    with open(args.config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    print("\n=== Performance Testing ===")

    # Test Spark pipeline
    print("\n--- Spark Pipeline ---")
    spark_results = test_spark_pipeline_enhanced(args.data_path, config)
    
    
    # Print results
    print("\n=== Results Summary ===")
    
    
    print("\nSpark Pipeline:")
    print(f"  Fit time: {spark_results['fit_time']:.2f} seconds")
    print(f"  Transform time: {spark_results['transform_time']:.2f} seconds")
    print(f"  Inverse time: {spark_results['inverse_time']:.2f} seconds")
    print(f"  Total time: {spark_results['fit_time'] + spark_results['transform_time'] + spark_results['inverse_time']:.2f} seconds")
    print(
    'data_shape:', spark_results['data_shape'],
    'transformed_count:', spark_results['transformed_count'],
    'inversed_count:', spark_results['inversed_count']
    )


    



if __name__ == "__main__":
    main() 