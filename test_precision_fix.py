#!/usr/bin/env python3
"""
Test script to verify that the fixed converter resolves precision issues.
"""

import pandas as pd
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from bd_transformer.transformer import Transformer
from bd_transformer.spark_components.converter import SparkConverter
from bd_transformer.spark_components.converter_fixed import SparkConverterFixed


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


def test_precision_fix():
    """Test that the fixed converter resolves precision issues."""
    print("Testing precision fix...")
    
    # Load data
    data = pd.read_parquet("data/small/001.parquet")
    with open("config.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    
    print(f"Data shape: {data.shape}")
    print(f"Sample data:\n{data.head()}")
    
    # Test pandas pipeline
    print("\n=== Pandas Pipeline ===")
    pandas_transformer = Transformer(config)
    pandas_transformer.fit(data)
    pandas_transformed = pandas_transformer.transform(data.copy())
    pandas_inversed = pandas_transformer.inverse_transform(pandas_transformed.copy())
    
    print(f"Pandas inversed sample:\n{pandas_inversed.head()}")
    
    # Test original Spark converter
    print("\n=== Original Spark Converter ===")
    spark = SparkSession.builder.appName("OriginalConverter").getOrCreate()
    spark_data = pandas_to_spark_df(data, spark)
    
    # Test with account_balance column
    account_config = config['account_balance']['converter']
    original_converter = SparkConverter(**account_config, spark=spark)
    original_converter.fit(spark_data.select('account_balance'))
    
    # Test conversion and inverse conversion
    converted = original_converter.convert(spark_data.select('account_balance'))
    inverse_converted = original_converter.inverse_convert(converted)
    
    print(f"Original Spark converter result:\n{inverse_converted.show()}")
    
    spark.stop()
    
    # Test fixed Spark converter
    print("\n=== Fixed Spark Converter ===")
    spark_fixed = SparkSession.builder.appName("FixedConverter").getOrCreate()
    spark_data_fixed = pandas_to_spark_df(data, spark_fixed)
    
    fixed_converter = SparkConverterFixed(**account_config, spark=spark_fixed)
    fixed_converter.fit(spark_data_fixed.select('account_balance'))
    
    # Test conversion and inverse conversion
    converted_fixed = fixed_converter.convert(spark_data_fixed.select('account_balance'))
    inverse_converted_fixed = fixed_converter.inverse_convert(converted_fixed)
    
    print(f"Fixed Spark converter result:\n{inverse_converted_fixed.show()}")
    
    spark_fixed.stop()
    
    # Compare specific values
    print("\n=== Comparing Specific Values ===")
    pandas_account = pandas_inversed['account_balance'].head(5).tolist()
    print(f"Pandas account_balance (first 5): {pandas_account}")
    
    # Get Spark results
    spark_fixed = SparkSession.builder.appName("Comparison").getOrCreate()
    spark_data_fixed = pandas_to_spark_df(data, spark_fixed)
    
    fixed_converter = SparkConverterFixed(**account_config, spark=spark_fixed)
    fixed_converter.fit(spark_data_fixed.select('account_balance'))
    converted_fixed = fixed_converter.convert(spark_data_fixed.select('account_balance'))
    inverse_converted_fixed = fixed_converter.inverse_convert(converted_fixed)
    
    spark_account = inverse_converted_fixed.select('account_balance').limit(5).collect()
    spark_account_values = [row[0] for row in spark_account]
    print(f"Fixed Spark account_balance (first 5): {spark_account_values}")
    
    # Check if they match
    matches = [pandas_val == spark_val for pandas_val, spark_val in zip(pandas_account, spark_account_values)]
    print(f"Values match: {matches}")
    print(f"All values match: {all(matches)}")
    
    spark_fixed.stop()
    
    print("\n=== Test Summary ===")
    if all(matches):
        print("✅ Precision fix successful! All values match between pandas and Spark.")
    else:
        print("❌ Some precision issues remain.")
        for i, (pandas_val, spark_val, match) in enumerate(zip(pandas_account, spark_account_values, matches)):
            if not match:
                print(f"  Row {i}: Pandas='{pandas_val}' vs Spark='{spark_val}'")


if __name__ == "__main__":
    test_precision_fix() 