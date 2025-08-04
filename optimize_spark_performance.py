#!/usr/bin/env python3
"""
Script to optimize Spark performance and identify bottlenecks.
"""

import pandas as pd
import yaml
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from bd_transformer.spark_transformer import SparkTransformer


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


def optimize_spark_performance():
    """Test different Spark configurations for performance."""
    print("Testing Spark performance optimizations...")
    
    # Load data
    data = pd.read_parquet("data/small/001.parquet")
    with open("config.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    print(f"Data shape: {data.shape}")
    
    # Test different Spark configurations
    configurations = [
        {
            'name': 'Default',
            'config': {}
        },
        {
            'name': 'Optimized',
            'config': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.adaptive.skewJoin.enabled': 'true',
                'spark.sql.adaptive.localShuffleReader.enabled': 'true',
                'spark.sql.adaptive.advisoryPartitionSizeInBytes': '128m',
                'spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold': '0',
                'spark.sql.adaptive.forceApply': 'true'
            }
        },
        {
            'name': 'High Memory',
            'config': {
                'spark.driver.memory': '4g',
                'spark.executor.memory': '4g',
                'spark.sql.adaptive.enabled': 'true'
            }
        }
    ]
    
    for config_info in configurations:
        print(f"\n=== Testing {config_info['name']} Configuration ===")
        
        # Create Spark session with configuration
        spark_builder = SparkSession.builder.appName(f"PerformanceTest_{config_info['name']}")
        for key, value in config_info['config'].items():
            spark_builder = spark_builder.config(key, value)
        
        spark = spark_builder.getOrCreate()
        
        # Convert data
        spark_data = pandas_to_spark_df(data, spark)
        
        # Test performance
        start_time = time.time()
        
        transformer = SparkTransformer(config, spark)
        transformer.fit(spark_data)
        transformed = transformer.transform(spark_data)
        inversed = transformer.inverse_transform(transformed)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"Execution time: {execution_time:.2f} seconds")
        
        # Show some results
        sample_result = inversed.limit(3).toPandas()
        print(f"Sample result:\n{sample_result}")
        
        spark.stop()
    
    print("\n=== Performance Summary ===")
    print("The optimized configuration should show better performance.")
    print("Consider using the configuration with the best performance for your use case.")


def profile_spark_operations():
    """Profile individual Spark operations to identify bottlenecks."""
    print("\n=== Profiling Spark Operations ===")
    
    # Load data
    data = pd.read_parquet("data/small/001.parquet")
    with open("config.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    spark = SparkSession.builder.appName("ProfilingTest").getOrCreate()
    spark_data = pandas_to_spark_df(data, spark)
    
    transformer = SparkTransformer(config, spark)
    
    # Profile fit operation
    print("Profiling fit operation...")
    start_time = time.time()
    transformer.fit(spark_data)
    fit_time = time.time() - start_time
    print(f"Fit time: {fit_time:.2f} seconds")
    
    # Profile transform operation
    print("Profiling transform operation...")
    start_time = time.time()
    transformed = transformer.transform(spark_data)
    transform_time = time.time() - start_time
    print(f"Transform time: {transform_time:.2f} seconds")
    
    # Profile inverse transform operation
    print("Profiling inverse transform operation...")
    start_time = time.time()
    inversed = transformer.inverse_transform(transformed)
    inverse_time = time.time() - start_time
    print(f"Inverse transform time: {inverse_time:.2f} seconds")
    
    total_time = fit_time + transform_time + inverse_time
    print(f"\nTotal time: {total_time:.2f} seconds")
    print(f"Breakdown:")
    print(f"  Fit: {fit_time/total_time*100:.1f}%")
    print(f"  Transform: {transform_time/total_time*100:.1f}%")
    print(f"  Inverse Transform: {inverse_time/total_time*100:.1f}%")
    
    spark.stop()


if __name__ == "__main__":
    optimize_spark_performance()
    profile_spark_operations() 