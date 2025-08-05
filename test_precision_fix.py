#!/usr/bin/env python3
"""
Test precision fix for Spark transformer
"""

import pandas as pd
from pyspark.sql import SparkSession
import yaml

from bd_transformer.transformer import Transformer as PandasTransformer
from bd_transformer.spark_transformer import SparkTransformer

def test_precision():
    """Test that Spark and pandas produce identical results"""
    print("=== Testing Precision Fix ===")
    
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
    print("Testing pandas pipeline...")
    pandas_transformer = PandasTransformer(config)
    pandas_transformer.fit(sample_data)
    pandas_transformed = pandas_transformer.transform(sample_data)
    pandas_inversed = pandas_transformer.inverse_transform(pandas_transformed)
    
    # Test Spark pipeline
    print("Testing Spark pipeline...")
    spark = SparkSession.builder.appName("PrecisionTest").getOrCreate()
    spark_data = spark.createDataFrame(sample_data)
    
    spark_transformer = SparkTransformer(config, spark)
    spark_transformer.fit(spark_data)
    spark_transformed = spark_transformer.transform(spark_data)
    spark_inversed = spark_transformer.inverse_transform(spark_transformed)
    
    # Convert to pandas for comparison
    spark_transformed_pd = spark_transformed.toPandas()
    spark_inversed_pd = spark_inversed.toPandas()
    
    # Compare results
    print("\n--- Comparing Results ---")
    
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
    
    spark.stop()
    print("=== Test Complete ===")

if __name__ == "__main__":
    test_precision() 