#!/usr/bin/env python3
"""
Test script to verify the Spark converter fix.
"""

import pandas as pd
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from bd_transformer.spark_components.converter import SparkConverter


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


def test_spark_converter():
    """Test the Spark converter with string data."""
    print("Testing Spark converter...")
    
    # Create test data
    test_data = pd.DataFrame({
        'height': ['50.85cm', '75.20cm', '60.10cm', '80.50cm']
    })
    
    print(f"Test data:\n{test_data}")
    
    # Initialize Spark
    spark = SparkSession.builder.appName("TestConverter").getOrCreate()
    spark_data = pandas_to_spark_df(test_data, spark)
    
    print(f"Spark data schema: {spark_data.schema}")
    print(f"Spark data:\n{spark_data.show()}")
    
    # Test converter configuration
    converter_config = {
        'min_val': 1,
        'max_val': False,
        'clip_oor': True,
        'prefix': '',
        'suffix': 'cm',
        'rounding': 2
    }
    
    print(f"Converter config: {converter_config}")
    
    # Create and fit converter
    converter = SparkConverter(**converter_config, spark=spark)
    converter.fit(spark_data.select('height'))
    
    print("Converter fitted successfully")
    
    # Test conversion
    converted = converter.convert(spark_data.select('height'))
    print(f"Converted data:\n{converted.show()}")
    
    # Test inverse conversion
    inverse_converted = converter.inverse_convert(converted)
    print(f"Inverse converted data:\n{inverse_converted.show()}")
    
    # Clean up
    spark.stop()
    
    print("Test completed successfully!")


if __name__ == "__main__":
    test_spark_converter() 