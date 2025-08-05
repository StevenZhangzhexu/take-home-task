#!/usr/bin/env python3
"""
Test optimized Spark transformer on large dataset
"""

import time
import yaml
from pyspark.sql import SparkSession

from bd_transformer.spark_transformer import SparkTransformer

def test_large_dataset():
    """Test optimized Spark transformer on large dataset"""
    print("=== Testing Optimized Spark on Large Dataset ===")
    
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Create optimized Spark session
    spark = SparkSession.builder \
        .appName("LargeDatasetTest") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m") \
        .getOrCreate()
    
    # Load large dataset
    print("Loading large dataset...")
    large_data = spark.read.parquet("data/large/")
    print(f"Dataset shape: {large_data.count()} rows, {len(large_data.columns)} columns")
    
    # Test optimized transformer
    transformer = SparkTransformer(config, spark)
    
    print("\n--- Fitting transformer ---")
    start_time = time.time()
    transformer.fit(large_data)
    fit_time = time.time() - start_time
    print(f"Fit time: {fit_time:.2f} seconds")
    
    print("\n--- Transforming data ---")
    start_time = time.time()
    transformed = transformer.transform(large_data)
    transform_time = time.time() - start_time
    print(f"Transform time: {transform_time:.2f} seconds")
    
    print("\n--- Inverse transforming ---")
    start_time = time.time()
    inversed = transformer.inverse_transform(transformed)
    inverse_time = time.time() - start_time
    print(f"Inverse transform time: {inverse_time:.2f} seconds")
    
    print(f"\nTotal time: {fit_time + transform_time + inverse_time:.2f} seconds")
    
    spark.stop()
    print("=== Test Complete ===")

if __name__ == "__main__":
    test_large_dataset() 