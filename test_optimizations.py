#!/usr/bin/env python3
"""
Test optimized Spark transformer
"""

import time
import pandas as pd
from pyspark.sql import SparkSession
import yaml

from bd_transformer.transformer import Transformer as PandasTransformer
from bd_transformer.spark_transformer import SparkTransformer

def test_optimizations():
    """Test that optimizations work correctly"""
    print("=== Testing Optimized Spark Transformer ===")
    
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
    
    # Test pandas pipeline
    print("\n--- Testing Pandas Pipeline ---")
    pandas_transformer = PandasTransformer(config)
    
    start_time = time.time()
    pandas_transformer.fit(sample_data)
    pandas_fit_time = time.time() - start_time
    print(f"Pandas fit time: {pandas_fit_time:.2f} seconds")
    
    start_time = time.time()
    pandas_transformed = pandas_transformer.transform(sample_data)
    pandas_transform_time = time.time() - start_time
    print(f"Pandas transform time: {pandas_transform_time:.2f} seconds")
    
    start_time = time.time()
    pandas_inversed = pandas_transformer.inverse_transform(pandas_transformed)
    pandas_inverse_time = time.time() - start_time
    print(f"Pandas inverse transform time: {pandas_inverse_time:.2f} seconds")
    
    # Test Spark pipeline
    print("\n--- Testing Optimized Spark Pipeline ---")
    spark = SparkSession.builder \
        .appName("OptimizationTest") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m") \
        .getOrCreate()
    
    # Convert to Spark DataFrame
    spark_data = spark.createDataFrame(sample_data)
    print(f"Data shape: {spark_data.count()} rows, {len(spark_data.columns)} columns")
    
    spark_transformer = SparkTransformer(config, spark)
    
    start_time = time.time()
    spark_transformer.fit(spark_data)
    spark_fit_time = time.time() - start_time
    print(f"Spark fit time: {spark_fit_time:.2f} seconds")
    
    start_time = time.time()
    spark_transformed = spark_transformer.transform(spark_data)
    spark_transform_time = time.time() - start_time
    print(f"Spark transform time: {spark_transform_time:.2f} seconds")
    
    start_time = time.time()
    spark_inversed = spark_transformer.inverse_transform(spark_transformed)
    spark_inverse_time = time.time() - start_time
    print(f"Spark inverse transform time: {spark_inverse_time:.2f} seconds")
    
    # Compare results
    print("\n--- Comparing Results ---")
    
    # Convert Spark results to pandas for comparison
    spark_transformed_pd = spark_transformed.toPandas()
    spark_inversed_pd = spark_inversed.toPandas()
    
    # Compare transformed results (round to 6 decimal places)
    pandas_transformed_rounded = pandas_transformed.round(6)
    spark_transformed_rounded = spark_transformed_pd.round(6)
    
    transform_match = pandas_transformed_rounded.equals(spark_transformed_rounded)
    print(f"Transform results match: {transform_match}")
    
    if not transform_match:
        print("Pandas transformed sample:")
        print(pandas_transformed_rounded.head())
        print("\nSpark transformed sample:")
        print(spark_transformed_rounded.head())
    
    # Compare inverse results
    inverse_match = pandas_inversed.equals(spark_inversed_pd)
    print(f"Inverse transform results match: {inverse_match}")
    
    if not inverse_match:
        print("Pandas inversed sample:")
        print(pandas_inversed.head())
        print("\nSpark inversed sample:")
        print(spark_inversed_pd.head())
    
    # Performance comparison
    print("\n--- Performance Comparison ---")
    print(f"Pandas total time: {pandas_fit_time + pandas_transform_time + pandas_inverse_time:.2f}s")
    print(f"Spark total time: {spark_fit_time + spark_transform_time + spark_inverse_time:.2f}s")
    
    # Test with larger dataset
    print("\n--- Testing with Larger Dataset ---")
    large_data = pd.concat([sample_data] * 1000, ignore_index=True)
    large_spark_data = spark.createDataFrame(large_data)
    print(f"Large dataset shape: {large_spark_data.count()} rows, {len(large_spark_data.columns)} columns")
    
    start_time = time.time()
    large_spark_transformer = SparkTransformer(config, spark)
    large_spark_transformer.fit(large_spark_data)
    large_fit_time = time.time() - start_time
    print(f"Large dataset fit time: {large_fit_time:.2f} seconds")
    
    start_time = time.time()
    large_transformed = large_spark_transformer.transform(large_spark_data)
    large_transform_time = time.time() - start_time
    print(f"Large dataset transform time: {large_transform_time:.2f} seconds")
    
    spark.stop()
    print("\n=== Optimization Test Complete ===")

if __name__ == "__main__":
    test_optimizations() 