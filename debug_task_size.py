#!/usr/bin/env python3
"""
Debug script to identify large task size causes and test partitioning strategies
"""

import pandas as pd
from pyspark.sql import SparkSession
import yaml

from bd_transformer.spark_transformer import SparkTransformer

def debug_task_size():
    """Debug what's causing large task size"""
    print("=== Debugging Large Task Size ===")
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Create sample data
    sample_data = pd.DataFrame({
        'day_of_month': [24, 15, 3, 3, 2],
        'height': ['75.03cm', '63.75cm', '86.64cm', '57.54cm', '82.92cm'],
        'account_balance': ['$4742.83', '$1574.47', '$9367.73', '$4652.01', '$8195.05'],
        'net_profit': ['$1349.11', '$-74.84', '$2088.75', '$3210.02', '$3673.54'],
        'customer_ratings': ['3.95stars', '1.04stars', '2.12stars', '4.04stars', '2.07stars'],
        'leaderboard_rank': [4010, 29818, 13769, 82262, 65536]
    })
    
    # Create larger dataset for testing
    large_data = pd.concat([sample_data] * 1000, ignore_index=True)
    print(f"Large dataset shape: {large_data.shape}")
    
    # Create optimized Spark session
    spark = SparkSession.builder \
        .appName("DebugTaskSize") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()
    
    # Convert to Spark DataFrame
    spark_data = spark.createDataFrame(large_data)
    print(f"Data shape: {spark_data.count()} rows, {len(spark_data.columns)} columns")
    
    # Check partition info
    print(f"Number of partitions: {spark_data.rdd.getNumPartitions()}")
    partition_sizes = spark_data.rdd.mapPartitions(lambda x: [len(list(x))]).collect()
    print(f"Partition sizes: {partition_sizes}")
    print(f"Min partition size: {min(partition_sizes)}")
    print(f"Max partition size: {max(partition_sizes)}")
    print(f"Avg partition size: {sum(partition_sizes) / len(partition_sizes):.1f}")
    
    # Check data distribution
    print("\n--- Data Distribution Analysis ---")
    for col in spark_data.columns:
        distinct_count = spark_data.select(col).distinct().count()
        print(f"{col}: {distinct_count} distinct values")
    
    # Test different partitioning strategies
    print("\n--- Testing Partitioning Strategies ---")
    
    # 1. Default partitioning
    print("1. Default partitioning:")
    default_partitions = spark_data.rdd.getNumPartitions()
    print(f"   Partitions: {default_partitions}")
    
    # 2. Increase partitions
    print("2. Increased partitions:")
    increased_data = spark_data.repartition(100)
    print(f"   Partitions: {increased_data.rdd.getNumPartitions()}")
    
    # 3. Partition by key (day_of_month)
    print("3. Partition by day_of_month:")
    partitioned_data = spark_data.repartition(50, "day_of_month")
    print(f"   Partitions: {partitioned_data.rdd.getNumPartitions()}")
    
    # 4. Partition by multiple keys
    print("4. Partition by multiple keys:")
    multi_partitioned = spark_data.repartition(50, "day_of_month", "leaderboard_rank")
    print(f"   Partitions: {multi_partitioned.rdd.getNumPartitions()}")
    
    # Test transformer with different partitioning
    spark_transformer = SparkTransformer(config, spark)
    
    print("\n--- Fitting transformer ---")
    spark_transformer.fit(spark_data)
    
    print("\n--- Testing transform with different partitioning ---")
    
    # Test with increased partitions
    print("Testing with increased partitions...")
    try:
        transformed_increased = spark_transformer.transform(increased_data)
        print("✓ Increased partitions successful!")
    except Exception as e:
        print(f"✗ Increased partitions failed: {e}")
    
    # Test with partitioned by key
    print("Testing with partitioned by key...")
    try:
        transformed_partitioned = spark_transformer.transform(partitioned_data)
        print("✓ Partitioned by key successful!")
    except Exception as e:
        print(f"✗ Partitioned by key failed: {e}")
    
    # Test with multi-partitioned
    print("Testing with multi-partitioned...")
    try:
        transformed_multi = spark_transformer.transform(multi_partitioned)
        print("✓ Multi-partitioned successful!")
    except Exception as e:
        print(f"✗ Multi-partitioned failed: {e}")
    
    spark.stop()
    print("=== Debug Complete ===")

if __name__ == "__main__":
    debug_task_size() 