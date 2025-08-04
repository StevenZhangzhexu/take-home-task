#!/usr/bin/env python3
"""
Test script to verify that the formatting issue (thousands separators) is fixed.
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from bd_transformer.spark_components.converter_fixed import SparkConverterFixed


def test_formatting():
    """Test that the formatting doesn't add thousands separators."""
    print("Testing formatting fix...")
    
    # Create test data
    test_data = pd.DataFrame({
        'account_balance': ['$4742.83', '$1574.47', '$9367.73', '$4652.01', '$8195.05']
    })
    
    print(f"Test data:\n{test_data}")
    
    # Initialize Spark
    spark = SparkSession.builder.appName("FormattingTest").getOrCreate()
    
    # Convert to Spark DataFrame
    schema = StructType([StructField('account_balance', StringType(), True)])
    spark_data = spark.createDataFrame(test_data, schema)
    
    print(f"Spark data:\n{spark_data.show()}")
    
    # Test converter configuration
    converter_config = {
        'min_val': 0,
        'max_val': False,
        'clip_oor': True,
        'prefix': '$',
        'suffix': '',
        'rounding': 2
    }
    
    print(f"Converter config: {converter_config}")
    
    # Create and fit converter
    converter = SparkConverterFixed(**converter_config, spark=spark)
    converter.fit(spark_data.select('account_balance'))
    
    print("Converter fitted successfully")
    
    # Test conversion
    converted = converter.convert(spark_data.select('account_balance'))
    print(f"Converted data:\n{converted.show()}")
    
    # Test inverse conversion
    inverse_converted = converter.inverse_convert(converted)
    print(f"Inverse converted data:\n{inverse_converted.show()}")
    
    # Check if the formatting is correct
    inverse_values = inverse_converted.select('account_balance').collect()
    formatted_values = [row[0] for row in inverse_values]
    
    print(f"Formatted values: {formatted_values}")
    
    # Check for thousands separators
    has_thousands_separators = any(',' in val for val in formatted_values if val is not None)
    
    if has_thousands_separators:
        print("❌ Still has thousands separators!")
        for val in formatted_values:
            if val and ',' in val:
                print(f"  Found thousands separator in: {val}")
    else:
        print("✅ No thousands separators found!")
    
    # Clean up
    spark.stop()
    
    print("Test completed!")


if __name__ == "__main__":
    test_formatting() 