import argparse
import os
import time
import psutil
import yaml
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import matplotlib.pyplot as plt
import numpy as np

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


def monitor_resources(duration_seconds=60):
    """Monitor CPU, memory, and disk I/O usage."""
    cpu_usage = []
    memory_usage = []
    disk_io = []
    timestamps = []
    
    start_time = time.time()
    process = psutil.Process()
    
    while time.time() - start_time < duration_seconds:
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_usage.append(cpu_percent)
        
        # Memory usage
        memory_info = psutil.virtual_memory()
        memory_usage.append(memory_info.percent)
        
        # Disk I/O (simplified)
        disk_io.append(0)  # Placeholder - would need more complex monitoring
        
        timestamps.append(time.time() - start_time)
    
    return {
        'cpu_usage': cpu_usage,
        'memory_usage': memory_usage,
        'disk_io': disk_io,
        'timestamps': timestamps
    }


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


def test_pandas_pipeline(data_path: str, config: dict):
    """Test pandas pipeline performance."""
    print("Testing pandas pipeline...")
    
    # Load data
    data = pd.read_parquet(data_path)
    print(f"Data shape: {data.shape}")
    
    # Monitor resources during fit
    print("Fitting transformer...")
    start_time = time.time()
    fit_metrics = monitor_resources(30)  # Monitor for 30 seconds during fit
    
    transformer = Transformer(config)
    transformer.fit(data)
    
    fit_time = time.time() - start_time
    print(f"Fit time: {fit_time:.2f} seconds")
    
    # Monitor resources during transform
    print("Transforming data...")
    start_time = time.time()
    transform_metrics = monitor_resources(30)  # Monitor for 30 seconds during transform
    
    transformed = transformer.transform(data.copy())
    
    transform_time = time.time() - start_time
    print(f"Transform time: {transform_time:.2f} seconds")
    
    # Monitor resources during inverse transform
    print("Inverse transforming data...")
    start_time = time.time()
    inverse_metrics = monitor_resources(30)  # Monitor for 30 seconds during inverse transform
    
    inversed = transformer.inverse_transform(transformed.copy())
    
    inverse_time = time.time() - start_time
    print(f"Inverse transform time: {inverse_time:.2f} seconds")
    
    return {
        'fit_time': fit_time,
        'transform_time': transform_time,
        'inverse_time': inverse_time,
        'fit_metrics': fit_metrics,
        'transform_metrics': transform_metrics,
        'inverse_metrics': inverse_metrics
    }


def test_spark_pipeline(data_path: str, config: dict, extra_confs: dict = {}):
    """Test Spark pipeline performance."""
    print("Testing Spark pipeline...")
    
    # Initialize Spark
    #spark = SparkSession.builder.appName("PerformanceTest").getOrCreate()

    # spark = (
    # SparkSession.builder
    # .appName("PipelineComparison")
    # .config("spark.default.parallelism", 100)  # More default tasks
    # .config("spark.sql.shuffle.partitions", 100) 
    # .config("spark.driver.memory", "4g")  # Optional memory tweak
    # .config("spark.kryoserializer.buffer.max", "512m")  # Optional serialization
    # .getOrCreate()
    # )


    spark = (
        SparkSession.builder
        .master("local[*]")  # Use all local cores (for testing on laptop)
        .appName("PipelineComparison")
        .config("spark.default.parallelism", 150)  # Default number of tasks for RDDs
        .config("spark.sql.shuffle.partitions", 120)  # Number of shuffle partitions (for joins/aggregations)
        .config("spark.driver.memory", "4g")  # Max memory for driver JVM
        .config("spark.kryoserializer.buffer.max", "512m")  # Max buffer for Kryo serialization
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .getOrCreate()
    )


    # builder = SparkSession.builder.appName("PipelineComparison")
    # for k, v in extra_confs.items():
    #     builder = builder.config(k, v)
    # spark = builder.getOrCreate()


    
    # Load data
    data = pd.read_parquet(data_path)
    spark_data = pandas_to_spark_df(data, spark)
    print(f"Data shape: {data.shape}")
    
    # Monitor resources during fit
    print("Fitting transformer...")
    start_time = time.time()
    fit_metrics = monitor_resources(30)  # Monitor for 30 seconds during fit
    
    transformer = SparkTransformer(config, spark)
    transformer.fit(spark_data)
    
    fit_time = time.time() - start_time
    print(f"Fit time: {fit_time:.2f} seconds")
    
    # Monitor resources during transform
    print("Transforming data...")
    start_time = time.time()
    transform_metrics = monitor_resources(30)  # Monitor for 30 seconds during transform
    
    transformed = transformer.transform(spark_data)
    
    transform_time = time.time() - start_time
    print(f"Transform time: {transform_time:.2f} seconds")
    
    # Monitor resources during inverse transform
    print("Inverse transforming data...")
    start_time = time.time()
    inverse_metrics = monitor_resources(30)  # Monitor for 30 seconds during inverse transform
    
    inversed = transformer.inverse_transform(transformed)
    
    inverse_time = time.time() - start_time
    print(f"Inverse transform time: {inverse_time:.2f} seconds")
    
    # Clean up
    spark.stop()
    
    return {
        'fit_time': fit_time,
        'transform_time': transform_time,
        'inverse_time': inverse_time,
        'fit_metrics': fit_metrics,
        'transform_metrics': transform_metrics,
        'inverse_metrics': inverse_metrics
    }


def plot_metrics(pandas_results, spark_results, output_dir="results"):
    """Plot resource usage metrics."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Create subplots for CPU and Memory usage
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    
    # Pandas metrics
    for i, (operation, metrics) in enumerate([('fit', pandas_results['fit_metrics']), 
                                            ('transform', pandas_results['transform_metrics']), 
                                            ('inverse', pandas_results['inverse_metrics'])]):
        axes[0, i].plot(metrics['timestamps'], metrics['cpu_usage'], label='CPU %', color='blue')
        axes[0, i].set_title(f'Pandas {operation.capitalize()} - CPU Usage')
        axes[0, i].set_xlabel('Time (s)')
        axes[0, i].set_ylabel('CPU Usage (%)')
        axes[0, i].grid(True)
        
        axes[1, i].plot(metrics['timestamps'], metrics['memory_usage'], label='Memory %', color='red')
        axes[1, i].set_title(f'Pandas {operation.capitalize()} - Memory Usage')
        axes[1, i].set_xlabel('Time (s)')
        axes[1, i].set_ylabel('Memory Usage (%)')
        axes[1, i].grid(True)
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/pandas_metrics.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # Spark metrics
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    
    for i, (operation, metrics) in enumerate([('fit', spark_results['fit_metrics']), 
                                            ('transform', spark_results['transform_metrics']), 
                                            ('inverse', spark_results['inverse_metrics'])]):
        axes[0, i].plot(metrics['timestamps'], metrics['cpu_usage'], label='CPU %', color='blue')
        axes[0, i].set_title(f'Spark {operation.capitalize()} - CPU Usage')
        axes[0, i].set_xlabel('Time (s)')
        axes[0, i].set_ylabel('CPU Usage (%)')
        axes[0, i].grid(True)
        
        axes[1, i].plot(metrics['timestamps'], metrics['memory_usage'], label='Memory %', color='red')
        axes[1, i].set_title(f'Spark {operation.capitalize()} - Memory Usage')
        axes[1, i].set_xlabel('Time (s)')
        axes[1, i].set_ylabel('Memory Usage (%)')
        axes[1, i].grid(True)
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/spark_metrics.png", dpi=300, bbox_inches='tight')
    plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, required=True)
    parser.add_argument("--config_path", type=str, default="config.yaml")
    parser.add_argument("--output_dir", type=str, default="results")
    #parser.add_argument("--conf", action="append", help="Extra Spark config in key=value format") #grid
    args = parser.parse_args()
    
    #extra_confs = dict(item.split("=", 1) for item in (args.conf or [])) #grid

    # Get system specs
    specs = get_system_specs()
    
    # Load config
    with open(args.config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    print("\n=== Performance Testing ===")

    # Test Spark pipeline
    print("\n--- Spark Pipeline ---")
    spark_results = test_spark_pipeline(args.data_path, config, extra_confs={}) #grid
    
    
    
    # Print results
   
    print("\nSpark Pipeline:")
    print(f"  Fit time: {spark_results['fit_time']:.2f} seconds")
    print(f"  Transform time: {spark_results['transform_time']:.2f} seconds")
    print(f"  Inverse time: {spark_results['inverse_time']:.2f} seconds")
    print(f"  Total time: {spark_results['fit_time'] + spark_results['transform_time'] + spark_results['inverse_time']:.2f} seconds")
    
    


if __name__ == "__main__":
    main() 