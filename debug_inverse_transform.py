#!/usr/bin/env python3
"""
Debug script to investigate inverse transform issues.
"""

import pandas as pd
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from bd_transformer.transformer import Transformer
from bd_transformer.spark_transformer import SparkTransformer
#from bd_transformer.spark_transformer_simple import SparkTransformerSimple


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


def debug_inverse_transform():
    """Debug the inverse transform issue."""
    print("Debugging inverse transform...")
    
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
    
    print(f"Pandas transformed sample:\n{pandas_transformed.head()}")
    print(f"Pandas inversed sample:\n{pandas_inversed.head()}")
    
    # Test Spark pipeline
    print("\n=== Spark Pipeline ===")
    spark = SparkSession.builder.appName("DebugInverse").getOrCreate()
    spark_data = pandas_to_spark_df(data, spark)
    
    spark_transformer = SparkTransformer(config, spark)
    spark_transformer.fit(spark_data)
    spark_transformed = spark_transformer.transform(spark_data)
    spark_inversed = spark_transformer.inverse_transform(spark_transformed)
    
    # Convert Spark results to pandas for comparison
    spark_transformed_pandas = spark_transformed.toPandas()
    spark_inversed_pandas = spark_inversed.toPandas()
    
    print(f"Spark transformed sample:\n{spark_transformed_pandas.head()}")
    print(f"Spark inversed sample:\n{spark_inversed_pandas.head()}")

    # # Test Spark_simple pipeline
    # print("\n=== Spark Pipeline ===")
    # spark = SparkSession.builder.appName("DebugInverse").getOrCreate()
    # spark_data = pandas_to_spark_df(data, spark)
    
    # spark_transformer_s = SparkTransformerSimple(config, spark)
    # spark_transformer_s.fit(spark_data)
    # spark_transformed_s = spark_transformer_s.transform(spark_data)
    # spark_inversed_s = spark_transformer_s.inverse_transform(spark_transformed)
    
    # Convert Spark results to pandas for comparison
    # spark_transformed_pandas_s = spark_transformed_s.toPandas()
    # spark_inversed_pandas_s = spark_inversed_s.toPandas()
    
    # print(f"Spark sim transformed sample:\n{spark_transformed_pandas_s.head()}")
    # print(f"Spark sim inversed sample:\n{spark_inversed_pandas_s.head()}")
    
    # Compare specific columns
    for col in ['day_of_month','height', 'account_balance', 'net_profit', 'customer_ratings', 'leaderboard_rank']:
        print(f"\n=== Comparing {col} ===")
        pandas_col = pandas_inversed[col]
        spark_col = spark_inversed_pandas[col]
        
        print(f"Pandas {col} (view first 5): {pandas_col.head().tolist()}")
        print(f"Spark {col} (view first 5): {spark_col.head().tolist()}")
        
        # Check if they're equal
        is_equal = pandas_col.equals(spark_col) # all
        print(f"Equal: {is_equal}")
        
        if not is_equal:
            for i in range(min(5, len(pandas_col))):
                if pandas_col.iloc[i] != spark_col.iloc[i]:
                    print(f"  Row {i}: Pandas='{pandas_col.iloc[i]}' vs Spark='{spark_col.iloc[i]}'")
    
    spark.stop()


if __name__ == "__main__":
    debug_inverse_transform() 