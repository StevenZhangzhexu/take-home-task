#!/usr/bin/env python3
"""
Test script for the Spark-optimized DAG.
This script demonstrates the usage of the refactored DAG and its parallel monitoring capabilities.
"""

import os
import sys
import yaml
import time
import json
from datetime import datetime

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from performance_monitor import SparkParallelMonitor, create_spark_session_with_monitoring
from bd_transformer.spark_transformer import SparkTransformer


def test_spark_parallel_monitoring():
    """Test the Spark parallel monitoring functionality."""
    print("=== Testing Spark Parallel Monitoring ===")
    
    # Initialize monitor
    monitor = SparkParallelMonitor(output_dir="test_monitoring_results")
    
    # Start monitoring
    monitor.start_monitoring()
    
    # Simulate pipeline phases
    print("Simulating pipeline phases...")
    
    # Phase 1: Fit
    monitor.mark_phase_start("fit")
    time.sleep(2)  # Simulate fit operation
    monitor.mark_phase_end("fit")
    
    # Phase 2: Transform
    monitor.mark_phase_start("transform")
    time.sleep(3)  # Simulate transform operation
    monitor.mark_phase_end("transform")
    
    # Phase 3: Inverse Transform
    monitor.mark_phase_start("inverse_transform")
    time.sleep(2)  # Simulate inverse transform operation
    monitor.mark_phase_end("inverse_transform")
    
    # Stop monitoring
    monitor.stop_monitoring()
    
    print("Monitoring test completed. Check 'test_monitoring_results' directory for output files.")
    
    # Display results
    if os.path.exists("test_monitoring_results/monitoring_summary.json"):
        with open("test_monitoring_results/monitoring_summary.json", 'r') as f:
            summary = json.load(f)
        print(f"Monitoring Summary: {summary}")


def test_spark_pipeline_with_monitoring():
    """Test the complete Spark pipeline with parallel monitoring."""
    print("\n=== Testing Spark Pipeline with Parallel Monitoring ===")
    
    # Load configuration
    with open("config.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    # Create Spark session
    spark = create_spark_session_with_monitoring(
        app_name="TestSparkPipeline",
        master="local[*]",
        spark_driver_memory="2g",
        spark_executor_memory="2g"
    )
    
    try:
        # Initialize monitor
        monitor = SparkParallelMonitor(output_dir="test_pipeline_results")
        monitor.start_monitoring()
        
        # Load test data
        print("Loading test data...")
        import pandas as pd
        from performance_monitor import pandas_to_spark_df
        
        # Create small test dataset
        test_data = pd.DataFrame({
            'col1': [1, 2, 3, 4, 5],
            'col2': ['a', 'b', 'c', 'd', 'e'],
            'col3': [1.1, 2.2, 3.3, 4.4, 5.5]
        })
        
        spark_data = pandas_to_spark_df(test_data, spark)
        spark_data.cache()
        
        print(f"Test data shape: {test_data.shape}")
        
        # Create transformer
        transformer = SparkTransformer(config, spark)
        
        # Phase 1: Fit
        print("Starting fit phase...")
        monitor.mark_phase_start("fit")
        transformer.fit(spark_data)
        monitor.mark_phase_end("fit")
        
        # Phase 2: Transform
        print("Starting transform phase...")
        monitor.mark_phase_start("transform")
        transformed = transformer.transform(spark_data)
        transformed_count = transformed.count()
        transformed.cache()
        monitor.mark_phase_end("transform")
        
        # Phase 3: Inverse Transform
        print("Starting inverse transform phase...")
        monitor.mark_phase_start("inverse_transform")
        inversed = transformer.inverse_transform(transformed)
        inversed_count = inversed.count()
        monitor.mark_phase_end("inverse_transform")
        
        # Stop monitoring
        monitor.stop_monitoring()
        
        # Cleanup
        spark_data.unpersist()
        transformed.unpersist()
        
        print(f"Pipeline completed successfully!")
        print(f"  Original rows: {test_data.shape[0]}")
        print(f"  Transformed rows: {transformed_count}")
        print(f"  Inversed rows: {inversed_count}")
        
        # Display monitoring results
        if os.path.exists("test_pipeline_results/monitoring_summary.json"):
            with open("test_pipeline_results/monitoring_summary.json", 'r') as f:
                summary = json.load(f)
            print(f"Pipeline Monitoring Summary: {summary}")
        
    except Exception as e:
        print(f"Pipeline test failed: {str(e)}")
        raise
    finally:
        spark.stop()


def test_airflow_dag_configuration():
    """Test the Airflow DAG configuration loading."""
    print("\n=== Testing Airflow DAG Configuration ===")
    
    # Load Spark-optimized configuration
    config_path = "airflow_dags/spark_optimized_config.yaml"
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            dag_config = yaml.safe_load(f)
        
        print("DAG Configuration loaded successfully:")
        print(f"  Spark Driver Memory: {dag_config['spark_config']['driver_memory']}")
        print(f"  Spark Executor Memory: {dag_config['spark_config']['executor_memory']}")
        print(f"  Default Parallelism: {dag_config['spark_config']['default_parallelism']}")
        print(f"  Monitoring Enabled: {dag_config['monitoring']['enabled']}")
        print(f"  Sample Interval: {dag_config['monitoring']['sample_interval']}s")
    else:
        print(f"Configuration file not found: {config_path}")


def demonstrate_parallelization_concept():
    """Demonstrate why Spark operations can't be parallelized column-by-column."""
    print("\n=== Parallelization Concept Demonstration ===")
    
    print("Why Spark operations can't be parallelized column-by-column in Airflow:")
    print()
    print("1. SPARK IS VECTORIZED:")
    print("   - Spark operations are inherently vectorized")
    print("   - Each operation processes entire partitions, not individual columns")
    print("   - Internal parallelism is handled by Spark's execution engine")
    print()
    print("2. LAZY EVALUATION:")
    print("   - Spark operations are lazy and only execute when actions are triggered")
    print("   - Multiple Spark sessions would compete for resources")
    print("   - Catalyst optimizer automatically optimizes the execution plan")
    print()
    print("3. PERFORMANCE MONITOR CAN BE PARALLELIZED:")
    print("   - Monitoring runs independently of Spark operations")
    print("   - Uses background threads that don't interfere with Spark execution")
    print("   - Collects complementary metrics (system-level, not Spark-internal)")
    print()
    print("4. OPTIMAL APPROACH:")
    print("   - Let Spark handle its own parallelism")
    print("   - Use Airflow for orchestration and monitoring")
    print("   - Run monitoring in parallel with Spark operations")


def main():
    """Main test function."""
    print("Spark-Optimized DAG Test Suite")
    print("=" * 50)
    
    try:
        # Test 1: Parallel monitoring
        test_spark_parallel_monitoring()
        
        # Test 2: Complete pipeline with monitoring
        test_spark_pipeline_with_monitoring()
        
        # Test 3: Configuration loading
        test_airflow_dag_configuration()
        
        # Test 4: Demonstrate parallelization concept
        demonstrate_parallelization_concept()
        
        print("\n" + "=" * 50)
        print("All tests completed successfully!")
        print("\nNext steps:")
        print("1. Deploy the DAG to your Airflow instance")
        print("2. Trigger the DAG with: airflow dags trigger spark_optimized_transformer_pipeline")
        print("3. Monitor the execution in Airflow UI")
        print("4. Check the monitoring results in the output directory")
        
    except Exception as e:
        print(f"Test suite failed: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 