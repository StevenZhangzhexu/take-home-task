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
from performance_monitor import RealTimeMonitor, SparkMetricsCollector

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


def test_pandas_pipeline(data_path: str, config: dict, output_dir: str = "results"):
    """Test pandas pipeline performance with parallel real-time monitoring."""
    print("Testing pandas pipeline...")
    
    # Load data
    data = pd.read_parquet(data_path)
    print(f"Data shape: {data.shape}")
    
    # Create transformer
    transformer = Transformer(config)
    
    # Start continuous monitoring for the entire pipeline
    print("Starting continuous pandas monitoring...")
    continuous_monitor = RealTimeMonitor(output_file=f"{output_dir}/pandas_continuous_metrics.json")
    continuous_monitor.start_monitoring()
    
    # Monitor fit operation
    print("Fitting transformer...")
    fit_start_time = time.time()
    
    transformer.fit(data)
    
    fit_time = time.time() - fit_start_time
    print(f"Fit time: {fit_time:.2f} seconds")
    
    # Monitor transform operation
    print("Transforming data...")
    transform_start_time = time.time()
    
    transformed = transformer.transform(data.copy())
    
    transform_time = time.time() - transform_start_time
    print(f"Transform time: {transform_time:.2f} seconds")
    
    # Monitor inverse transform operation
    print("Inverse transforming data...")
    inverse_start_time = time.time()
    
    inversed = transformer.inverse_transform(transformed.copy())
    
    inverse_time = time.time() - inverse_start_time
    print(f"Inverse transform time: {inverse_time:.2f} seconds")
    
    # Stop continuous monitoring
    continuous_monitor.stop_monitoring()
    
    # Split continuous metrics into phases based on timing
    continuous_metrics = continuous_monitor.metrics
    total_duration = continuous_metrics['timestamps'][-1] if continuous_metrics['timestamps'] else 0
    
    # Calculate phase boundaries
    fit_end_time = fit_time
    transform_end_time = fit_time + transform_time
    inverse_end_time = fit_time + transform_time + inverse_time
    
    # Split metrics into phases
    def split_metrics_by_phase(metrics, phase_start, phase_end):
        """Split continuous metrics into a specific phase."""
        phase_metrics = {
            'timestamps': [],
            'cpu_usage': [],
            'memory_usage': [],
            'disk_io': [],
            'network_io': [],
            'process_metrics': []
        }
        
        for i, timestamp in enumerate(metrics['timestamps']):
            if phase_start <= timestamp <= phase_end:
                for key in phase_metrics:
                    if key in metrics and i < len(metrics[key]):
                        phase_metrics[key].append(metrics[key][i])
        
        return phase_metrics
    
    # Split metrics into phases
    fit_metrics = split_metrics_by_phase(continuous_metrics, 0, fit_end_time)
    transform_metrics = split_metrics_by_phase(continuous_metrics, fit_end_time, transform_end_time)
    inverse_metrics = split_metrics_by_phase(continuous_metrics, transform_end_time, inverse_end_time)
    
    return {
        'fit_time': fit_time,
        'transform_time': transform_time,
        'inverse_time': inverse_time,
        'fit_metrics': fit_metrics,
        'transform_metrics': transform_metrics,
        'inverse_metrics': inverse_metrics,
        'data_shape': data.shape
    }


def test_spark_pipeline_enhanced(data_path: str, config: dict, output_dir: str = "results"):
    """Enhanced Spark pipeline test with real-time monitoring and Spark UI metrics."""
    
    # Initialize Spark with monitoring
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("EnhancedPipelineTest")
        .config("spark.default.parallelism", 150)
        .config("spark.sql.shuffle.partitions", 120)
        .config("spark.driver.memory", "4g")
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .getOrCreate()
    )
    
    # Initialize Spark metrics collector
    spark_collector = SparkMetricsCollector(spark)
    spark_ui_url = spark_collector.get_spark_ui_url()
    
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
    
    # Stop continuous monitoring
    continuous_monitor.stop_monitoring()
    
    # Collect final Spark metrics
    spark_metrics = spark_collector.collect_spark_metrics()
    
    # Clean up
    spark.stop()
    
    # Split continuous metrics into phases based on timing
    continuous_metrics = continuous_monitor.metrics
    total_duration = continuous_metrics['timestamps'][-1] if continuous_metrics['timestamps'] else 0
    
    # Calculate phase boundaries
    fit_end_time = fit_time
    transform_end_time = fit_time + transform_time
    inverse_end_time = fit_time + transform_time + inverse_time
    
    # Split metrics into phases
    def split_metrics_by_phase(metrics, phase_start, phase_end):
        """Split continuous metrics into a specific phase."""
        phase_metrics = {
            'timestamps': [],
            'cpu_usage': [],
            'memory_usage': [],
            'disk_io': [],
            'network_io': [],
            'process_metrics': []
        }
        
        for i, timestamp in enumerate(metrics['timestamps']):
            if phase_start <= timestamp <= phase_end:
                for key in phase_metrics:
                    if key in metrics and i < len(metrics[key]):
                        phase_metrics[key].append(metrics[key][i])
        
        return phase_metrics
    
    # Split metrics into phases
    fit_metrics = split_metrics_by_phase(continuous_metrics, 0, fit_end_time)
    transform_metrics = split_metrics_by_phase(continuous_metrics, fit_end_time, transform_end_time)
    inverse_metrics = split_metrics_by_phase(continuous_metrics, transform_end_time, inverse_end_time)
    
    return {
        'fit_time': fit_time,
        'transform_time': transform_time,
        'inverse_time': inverse_time,
        'fit_metrics': fit_metrics,
        'transform_metrics': transform_metrics,
        'inverse_metrics': inverse_metrics,
        'data_shape': data.shape,
        'transformed_count': transformed_count,
        'inversed_count': inversed_count,
        'spark_metrics': spark_metrics,
        'spark_ui_url': spark_ui_url
    }


def plot_metrics(pandas_results, spark_results, output_dir="results"):
    """Plot resource usage metrics with real processing times."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Create subplots for CPU and Memory usage
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    
    # Pandas metrics
    for i, (operation, metrics) in enumerate([('fit', pandas_results.get('fit_metrics', {})), 
                                            ('transform', pandas_results.get('transform_metrics', {})), 
                                            ('inverse', pandas_results.get('inverse_metrics', {}))]):
        if metrics and 'timestamps' in metrics and 'cpu_usage' in metrics and len(metrics['timestamps']) > 0:
            # Use actual processing time for x-axis
            actual_time = pandas_results.get(f'{operation}_time', 0)
            if actual_time > 0:
                # Normalize timestamps to actual processing time
                max_timestamp = max(metrics['timestamps'])
                if max_timestamp > 0:
                    normalized_timestamps = [t * actual_time / max_timestamp for t in metrics['timestamps']]
                    axes[0, i].plot(normalized_timestamps, metrics['cpu_usage'], label='CPU %', color='blue')
                    axes[0, i].set_title(f'Pandas {operation.capitalize()} - CPU Usage ({actual_time:.1f}s)')
                    axes[0, i].set_xlabel('Time (s)')
                    axes[0, i].set_ylabel('CPU Usage (%)')
                    axes[0, i].grid(True)
            
            if metrics and 'timestamps' in metrics and 'memory_usage' in metrics and len(metrics['timestamps']) > 0:
                max_timestamp = max(metrics['timestamps'])
                if max_timestamp > 0:
                    normalized_timestamps = [t * actual_time / max_timestamp for t in metrics['timestamps']]
                    axes[1, i].plot(normalized_timestamps, metrics['memory_usage'], label='Memory %', color='red')
                    axes[1, i].set_title(f'Pandas {operation.capitalize()} - Memory Usage ({actual_time:.1f}s)')
                    axes[1, i].set_xlabel('Time (s)')
                    axes[1, i].set_ylabel('Memory Usage (%)')
                    axes[1, i].grid(True)
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/pandas_metrics.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # Spark metrics
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    
    for i, (operation, metrics) in enumerate([('fit', spark_results.get('fit_metrics', {})), 
                                            ('transform', spark_results.get('transform_metrics', {})), 
                                            ('inverse', spark_results.get('inverse_metrics', {}))]):
        if metrics and 'timestamps' in metrics and 'cpu_usage' in metrics and len(metrics['timestamps']) > 0:
            # Use actual processing time for x-axis
            actual_time = spark_results.get(f'{operation}_time', 0)
            if actual_time > 0:
                # Normalize timestamps to actual processing time
                max_timestamp = max(metrics['timestamps'])
                if max_timestamp > 0:
                    normalized_timestamps = [t * actual_time / max_timestamp for t in metrics['timestamps']]
                    axes[0, i].plot(normalized_timestamps, metrics['cpu_usage'], label='CPU %', color='blue')
                    axes[0, i].set_title(f'Spark {operation.capitalize()} - CPU Usage ({actual_time:.1f}s)')
                    axes[0, i].set_xlabel('Time (s)')
                    axes[0, i].set_ylabel('CPU Usage (%)')
                    axes[0, i].grid(True)
            
            if metrics and 'timestamps' in metrics and 'memory_usage' in metrics and len(metrics['timestamps']) > 0:
                max_timestamp = max(metrics['timestamps'])
                if max_timestamp > 0:
                    normalized_timestamps = [t * actual_time / max_timestamp for t in metrics['timestamps']]
                    axes[1, i].plot(normalized_timestamps, metrics['memory_usage'], label='Memory %', color='red')
                    axes[1, i].set_title(f'Spark {operation.capitalize()} - Memory Usage ({actual_time:.1f}s)')
                    axes[1, i].set_xlabel('Time (s)')
                    axes[1, i].set_ylabel('Memory Usage (%)')
                    axes[1, i].grid(True)
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/spark_metrics.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot Spark UI metrics if available
    if spark_results.get('spark_metrics') and spark_results['spark_metrics'].get('application'):
        plot_spark_ui_metrics(spark_results, output_dir)
    
    # Add debug information
    print(f"\n=== Monitoring Debug Info ===")
    print(f"Pandas - Fit: {pandas_results.get('fit_time', 0):.2f}s, Transform: {pandas_results.get('transform_time', 0):.2f}s, Inverse: {pandas_results.get('inverse_time', 0):.2f}s")
    print(f"Spark - Fit: {spark_results.get('fit_time', 0):.2f}s, Transform: {spark_results.get('transform_time', 0):.2f}s, Inverse: {spark_results.get('inverse_time', 0):.2f}s")


def plot_spark_ui_metrics(spark_results, output_dir):
    """Plot Spark UI metrics over time."""
    spark_metrics = spark_results.get('spark_metrics', {})
    if not spark_metrics or 'application' not in spark_metrics:
        return
    
    app_metrics = spark_metrics['application']
    
    # Create Spark UI metrics plot
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    
    # Memory utilization
    if 'executor_memory_used' in app_metrics and 'executor_memory_max' in app_metrics:
        memory_used = app_metrics['executor_memory_used']
        memory_max = app_metrics['executor_memory_max']
        memory_utilization = (memory_used / memory_max * 100) if memory_max > 0 else 0
        
        axes[0, 0].bar(['Memory Used', 'Memory Max'], [memory_used, memory_max], color=['red', 'blue'])
        axes[0, 0].set_title(f'Spark Memory Usage ({memory_utilization:.1f}% utilized)')
        axes[0, 0].set_ylabel('Memory (MB)')
    
    # Core utilization
    if 'executor_cores_used' in app_metrics and 'executor_cores_max' in app_metrics:
        cores_used = app_metrics['executor_cores_used']
        cores_max = app_metrics['executor_cores_max']
        core_utilization = (cores_used / cores_max * 100) if cores_max > 0 else 0
        
        axes[0, 1].bar(['Cores Used', 'Cores Max'], [cores_used, cores_max], color=['green', 'orange'])
        axes[0, 1].set_title(f'Spark Core Usage ({core_utilization:.1f}% utilized)')
        axes[0, 1].set_ylabel('Cores')
    
    # Job/Stage completion
    if 'jobs_completed' in app_metrics and 'stages_completed' in app_metrics:
        jobs_completed = app_metrics['jobs_completed']
        stages_completed = app_metrics['stages_completed']
        
        axes[1, 0].bar(['Jobs', 'Stages'], [jobs_completed, stages_completed], color=['purple', 'brown'])
        axes[1, 0].set_title('Spark Jobs and Stages Completed')
        axes[1, 0].set_ylabel('Count')
    
    # Success rate - with proper error handling
    if 'jobs_completed' in app_metrics and 'jobs_failed' in app_metrics:
        jobs_completed = app_metrics['jobs_completed']
        jobs_failed = app_metrics['jobs_failed']
        total_jobs = jobs_completed + jobs_failed
        
        if total_jobs > 0:
            success_rate = (jobs_completed / total_jobs * 100)
            axes[1, 1].pie([jobs_completed, jobs_failed], labels=['Completed', 'Failed'], 
                          colors=['green', 'red'], autopct='%1.1f%%')
            axes[1, 1].set_title(f'Job Success Rate ({success_rate:.1f}%)')
        else:
            # Handle case where no jobs were executed
            axes[1, 1].text(0.5, 0.5, 'No jobs executed', ha='center', va='center', transform=axes[1, 1].transAxes)
            axes[1, 1].set_title('Job Success Rate (N/A)')
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/spark_ui_metrics.png", dpi=300, bbox_inches='tight')
    plt.close()


def save_detailed_results(pandas_results, spark_results, output_dir="results"):
    """Save detailed results including Spark UI metrics and timing data."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save comprehensive results
    detailed_results = {
        'timestamp': timestamp,
        'pandas_results': pandas_results,
        'spark_results': spark_results,
        'summary': {
            'pandas_total_time': pandas_results.get('fit_time', 0) + pandas_results.get('transform_time', 0) + pandas_results.get('inverse_time', 0),
            'spark_total_time': spark_results.get('fit_time', 0) + spark_results.get('transform_time', 0) + spark_results.get('inverse_time', 0),
            'speedup': (pandas_results.get('fit_time', 0) + pandas_results.get('transform_time', 0) + pandas_results.get('inverse_time', 0)) / 
                      max(spark_results.get('fit_time', 0) + spark_results.get('transform_time', 0) + spark_results.get('inverse_time', 0), 1)
        }
    }
    
    # Save detailed results
    try:
        with open(f"{output_dir}/detailed_results_{timestamp}.json", 'w') as f:
            json.dump(detailed_results, f, indent=2, default=str)
    except Exception as e:
        print(f"Warning: Could not save detailed results: {e}")
    
    # Save Spark UI metrics separately
    if spark_results.get('spark_metrics'):
        try:
            with open(f"{output_dir}/spark_ui_metrics_{timestamp}.json", 'w') as f:
                json.dump(spark_results['spark_metrics'], f, indent=2, default=str)
        except Exception as e:
            print(f"Warning: Could not save Spark UI metrics: {e}")
    
    # Save timing data as CSV for easy analysis
    try:
        timing_data = {
            'operation': ['fit', 'transform', 'inverse_transform', 'total'],
            'pandas_time': [
                pandas_results.get('fit_time', 0),
                pandas_results.get('transform_time', 0),
                pandas_results.get('inverse_time', 0),
                pandas_results.get('fit_time', 0) + pandas_results.get('transform_time', 0) + pandas_results.get('inverse_time', 0)
            ],
            'spark_time': [
                spark_results.get('fit_time', 0),
                spark_results.get('transform_time', 0),
                spark_results.get('inverse_time', 0),
                spark_results.get('fit_time', 0) + spark_results.get('transform_time', 0) + spark_results.get('inverse_time', 0)
            ]
        }
        
        timing_df = pd.DataFrame(timing_data)
        timing_df.to_csv(f"{output_dir}/timing_comparison_{timestamp}.csv", index=False)
    except Exception as e:
        print(f"Warning: Could not save timing comparison: {e}")
    
    print(f"Detailed results saved to {output_dir}/")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, required=True)
    parser.add_argument("--config_path", type=str, default="config.yaml")
    parser.add_argument("--output_dir", type=str, default="results")
    args = parser.parse_args()

    # Get system specs
    specs = get_system_specs()
    
    # Load config
    with open(args.config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    print("\n=== Performance Testing ===")

    # Test Spark pipeline
    print("\n--- Spark Pipeline ---")
    spark_results = test_spark_pipeline_enhanced(args.data_path, config, args.output_dir)
    
    # Test pandas pipeline
    print("\n--- Pandas Pipeline ---")
    pandas_results = test_pandas_pipeline(args.data_path, config, args.output_dir)
    
    # Print results
    print("\n=== Results Summary ===")
    print("Pandas Pipeline:")
    print(f"  Fit time: {pandas_results['fit_time']:.2f} seconds")
    print(f"  Transform time: {pandas_results['transform_time']:.2f} seconds")
    print(f"  Inverse time: {pandas_results['inverse_time']:.2f} seconds")
    print(f"  Total time: {pandas_results['fit_time'] + pandas_results['transform_time'] + pandas_results['inverse_time']:.2f} seconds")
    
    print("\nSpark Pipeline:")
    print(f"  Fit time: {spark_results['fit_time']:.2f} seconds")
    print(f"  Transform time: {spark_results['transform_time']:.2f} seconds")
    print(f"  Inverse time: {spark_results['inverse_time']:.2f} seconds")
    print(f"  Total time: {spark_results['fit_time'] + spark_results['transform_time'] + spark_results['inverse_time']:.2f} seconds")
    
    if spark_results.get('spark_ui_url'):
        print(f"  Spark UI: {spark_results['spark_ui_url']}")
    
    # Plot metrics
    plot_metrics(pandas_results, spark_results, args.output_dir)
    
    # Save detailed results
    save_detailed_results(pandas_results, spark_results, args.output_dir)
    
    # Save basic results
    results = {
        'system_specs': specs,
        'pandas_results': pandas_results,
        'spark_results': spark_results
    }
    
    with open(f"{args.output_dir}/performance_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nResults saved to {args.output_dir}/")


if __name__ == "__main__":
    main() 