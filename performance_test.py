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


def test_pandas_pipeline(data_path: str, config: dict, output_dir: str = "results"):
    """Test pandas pipeline performance with parallel real-time monitoring."""
    
    # Create output directory first
    os.makedirs(output_dir, exist_ok=True)
    
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
    
    # Create output directory first
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize Spark with monitoring
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("EnhancedPipelineTest")
        .config("spark.default.parallelism", 200)
        .config("spark.sql.shuffle.partitions", 120)
        .config("spark.driver.memory", "4g")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer", "64k")
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .getOrCreate()
    )
    
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
    
    # Stop continuous monitoring
    continuous_monitor.stop_monitoring()
    
    # Collect basic Spark metrics
    spark_metrics = {}
    try:
        # Get basic Spark metrics from the context
        spark_context = spark.sparkContext
        spark_metrics = {
            'application': {
                'id': app_id or 'unknown',
                'name': spark_context.appName,
                'ui_url': spark_ui_url,
                'executor_memory_used': 0,  # These would need more complex collection
                'executor_memory_max': 0,
                'executor_cores_used': 0,
                'executor_cores_max': 0,
                'stages_completed': 0,
                'stages_failed': 0,
                'jobs_completed': 0,
                'jobs_failed': 0
            },
            'basic_info': {
                'default_parallelism': spark_context.defaultParallelism,
                'master': spark_context.master,
                'app_name': spark_context.appName,
                'application_id': app_id
            }
        }
    except Exception as e:
        print(f"Warning: Could not collect Spark metrics: {e}")
        spark_metrics = {'error': str(e)}
    
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
    """Plot resource usage metrics including CPU, Memory, and Disk I/O."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Create subplots for CPU, Memory, and Disk I/O usage
    fig, axes = plt.subplots(3, 3, figsize=(15, 15))
    
    # --- Plot Pandas Metrics ---
    for i, (operation, metrics) in enumerate([('fit', pandas_results.get('fit_metrics', {})), 
                                            ('transform', pandas_results.get('transform_metrics', {})), 
                                            ('inverse', pandas_results.get('inverse_metrics', {}))]):
        
        actual_time = pandas_results.get(f'{operation}_time', 0)
        
        if not (metrics and 'timestamps' in metrics and len(metrics['timestamps']) > 0 and actual_time > 0):
            continue

        max_timestamp = max(metrics['timestamps'])
        if max_timestamp <= 0:
            continue
            
        normalized_timestamps = [(t / max_timestamp) * actual_time for t in metrics['timestamps']]

        # Plot CPU Usage
        if 'cpu_usage' in metrics and len(metrics['cpu_usage']) > 0:
            axes[0, i].plot(normalized_timestamps, metrics['cpu_usage'], label='CPU %', color='blue')
            axes[0, i].set_title(f'Pandas {operation.capitalize()} - CPU Usage ({actual_time:.1f}s)')
            axes[0, i].set_xlabel('Time (s)')
            axes[0, i].set_ylabel('CPU Usage (%)')
            axes[0, i].grid(True)
            axes[0, i].set_xlim(0, actual_time)
        
        # Plot Memory Usage
        if 'memory_usage' in metrics and len(metrics['memory_usage']) > 0:
            axes[1, i].plot(normalized_timestamps, metrics['memory_usage'], label='Memory %', color='red')
            axes[1, i].set_title(f'Pandas {operation.capitalize()} - Memory Usage ({actual_time:.1f}s)')
            axes[1, i].set_xlabel('Time (s)')
            axes[1, i].set_ylabel('Memory Usage (%)')
            axes[1, i].grid(True)
            axes[1, i].set_xlim(0, actual_time)
            
        # Plot Disk I/O
        if 'disk_io' in metrics and len(metrics['disk_io']) > 0:
            axes[2, i].plot(normalized_timestamps, metrics['disk_io'], label='Disk I/O (MB/s)', color='green')
            axes[2, i].set_title(f'Pandas {operation.capitalize()} - Disk I/O ({actual_time:.1f}s)')
            axes[2, i].set_xlabel('Time (s)')
            axes[2, i].set_ylabel('Disk I/O (MB/s)')
            axes[2, i].grid(True)
            axes[2, i].set_xlim(0, actual_time)

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.suptitle('Pandas Performance Metrics', fontsize=16)
    plt.savefig(f"{output_dir}/pandas_metrics.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # --- Plot Spark Metrics ---
    fig, axes = plt.subplots(3, 3, figsize=(15, 15))
    
    for i, (operation, metrics) in enumerate([('fit', spark_results.get('fit_metrics', {})), 
                                            ('transform', spark_results.get('transform_metrics', {})), 
                                            ('inverse', spark_results.get('inverse_metrics', {}))]):
        
        actual_time = spark_results.get(f'{operation}_time', 0)

        if not (metrics and 'timestamps' in metrics and len(metrics['timestamps']) > 0 and actual_time > 0):
            continue
        
        max_timestamp = max(metrics['timestamps'])
        if max_timestamp <= 0:
            continue
            
        normalized_timestamps = [(t / max_timestamp) * actual_time for t in metrics['timestamps']]

        # Plot CPU Usage
        if 'cpu_usage' in metrics and len(metrics['cpu_usage']) > 0:
            axes[0, i].plot(normalized_timestamps, metrics['cpu_usage'], label='CPU %', color='blue')
            axes[0, i].set_title(f'Spark {operation.capitalize()} - CPU Usage ({actual_time:.1f}s)')
            axes[0, i].set_xlabel('Time (s)')
            axes[0, i].set_ylabel('CPU Usage (%)')
            axes[0, i].grid(True)
            axes[0, i].set_xlim(0, actual_time)
        
        # Plot Memory Usage
        if 'memory_usage' in metrics and len(metrics['memory_usage']) > 0:
            axes[1, i].plot(normalized_timestamps, metrics['memory_usage'], label='Memory %', color='red')
            axes[1, i].set_title(f'Spark {operation.capitalize()} - Memory Usage ({actual_time:.1f}s)')
            axes[1, i].set_xlabel('Time (s)')
            axes[1, i].set_ylabel('Memory Usage (%)')
            axes[1, i].grid(True)
            axes[1, i].set_xlim(0, actual_time)

        # Plot Disk I/O
        if 'disk_io' in metrics and len(metrics['disk_io']) > 0:
            axes[2, i].plot(normalized_timestamps, metrics['disk_io'], label='Disk I/O (MB/s)', color='green')
            axes[2, i].set_title(f'Spark {operation.capitalize()} - Disk I/O ({actual_time:.1f}s)')
            axes[2, i].set_xlabel('Time (s)')
            axes[2, i].set_ylabel('Disk I/O (MB/s)')
            axes[2, i].grid(True)
            axes[2, i].set_xlim(0, actual_time)
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    fig.suptitle('Spark Performance Metrics', fontsize=16)
    plt.savefig(f"{output_dir}/spark_metrics.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # Plot Spark UI metrics if available
    if spark_results.get('spark_metrics'):
        try:
            plot_spark_ui_metrics(spark_results, output_dir)
        except Exception as e:
            print(f"Warning: Could not plot Spark UI metrics: {e}")
            # Create a simple error plot instead
            try:
                fig, ax = plt.subplots(figsize=(8, 6))
                ax.text(0.5, 0.5, f'Spark UI metrics unavailable\nError: {str(e)}', 
                       ha='center', va='center', transform=ax.transAxes)
                ax.set_title('Spark UI Metrics (Error)')
                plt.savefig(f"{output_dir}/spark_ui_metrics.png", dpi=300, bbox_inches='tight')
                plt.close()
            except:
                print("Could not create error plot for Spark UI metrics")
    
    # Add debug information
    print(f"\n=== Monitoring Debug Info ===")
    print(f"Pandas - Fit: {pandas_results.get('fit_time', 0):.2f}s, Transform: {pandas_results.get('transform_time', 0):.2f}s, Inverse: {pandas_results.get('inverse_time', 0):.2f}s")
    print(f"Spark - Fit: {spark_results.get('fit_time', 0):.2f}s, Transform: {spark_results.get('transform_time', 0):.2f}s, Inverse: {spark_results.get('inverse_time', 0):.2f}s")


def plot_spark_ui_metrics(spark_results, output_dir):
    """Plot Spark UI metrics over time."""
    spark_metrics = spark_results.get('spark_metrics', {})
    if not spark_metrics:
        return
    
    # Create Spark UI metrics plot
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    
    # Basic Spark info
    basic_info = spark_metrics.get('basic_info', {})
    app_info = spark_metrics.get('application', {})
    
    # Application info
    axes[0, 0].text(0.1, 0.8, f"App ID: {app_info.get('id', 'N/A')}", transform=axes[0, 0].transAxes, fontsize=10)
    axes[0, 0].text(0.1, 0.6, f"App Name: {app_info.get('name', 'N/A')}", transform=axes[0, 0].transAxes, fontsize=10)
    axes[0, 0].text(0.1, 0.4, f"Master: {basic_info.get('master', 'N/A')}", transform=axes[0, 0].transAxes, fontsize=10)
    axes[0, 0].text(0.1, 0.2, f"Parallelism: {basic_info.get('default_parallelism', 'N/A')}", transform=axes[0, 0].transAxes, fontsize=10)
    axes[0, 0].set_title('Spark Application Info')
    axes[0, 0].axis('off')
    
    # Performance summary
    fit_time = spark_results.get('fit_time', 0)
    transform_time = spark_results.get('transform_time', 0)
    inverse_time = spark_results.get('inverse_time', 0)
    total_time = fit_time + transform_time + inverse_time
    
    times = [fit_time, transform_time, inverse_time]
    labels = ['Fit', 'Transform', 'Inverse']
    colors = ['blue', 'green', 'red']
    
    axes[0, 1].bar(labels, times, color=colors)
    axes[0, 1].set_title(f'Pipeline Performance (Total: {total_time:.1f}s)')
    axes[0, 1].set_ylabel('Time (seconds)')
    
    # Data processing info
    data_shape = spark_results.get('data_shape', (0, 0))
    transformed_count = spark_results.get('transformed_count', 0)
    inversed_count = spark_results.get('inversed_count', 0)
    
    axes[1, 0].text(0.1, 0.8, f"Input Shape: {data_shape}", transform=axes[1, 0].transAxes, fontsize=10)
    axes[1, 0].text(0.1, 0.6, f"Transformed Rows: {transformed_count:,}", transform=axes[1, 0].transAxes, fontsize=10)
    axes[1, 0].text(0.1, 0.4, f"Inversed Rows: {inversed_count:,}", transform=axes[1, 0].transAxes, fontsize=10)
    axes[1, 0].set_title('Data Processing Summary')
    axes[1, 0].axis('off')
    
    # Spark UI URL
    ui_url = app_info.get('ui_url')
    if ui_url:
        axes[1, 1].text(0.1, 0.8, 'Spark UI Available:', transform=axes[1, 1].transAxes, fontsize=10, weight='bold')
        axes[1, 1].text(0.1, 0.6, ui_url, transform=axes[1, 1].transAxes, fontsize=8, wrap=True)
        axes[1, 1].text(0.1, 0.3, 'Visit the URL above for detailed', transform=axes[1, 1].transAxes, fontsize=8)
        axes[1, 1].text(0.1, 0.2, 'Spark metrics and monitoring', transform=axes[1, 1].transAxes, fontsize=8)
    else:
        axes[1, 1].text(0.5, 0.5, 'Spark UI not available', ha='center', va='center', transform=axes[1, 1].transAxes)
    
    axes[1, 1].set_title('Spark UI Access')
    axes[1, 1].axis('off')
    
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
    try:
        plot_metrics(pandas_results, spark_results, args.output_dir)
    except Exception as e:
        print(f"Warning: Could not create performance plots: {e}")
    
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