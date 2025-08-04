import pandas as pd
import yaml
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from bd_transformer.transformer import Transformer
from bd_transformer.spark_transformer import SparkTransformer


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    with open(config_path, "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def pandas_to_spark_df(pandas_df: pd.DataFrame, spark: SparkSession) -> pd.DataFrame:
    """Convert pandas DataFrame to Spark DataFrame."""
    # Infer schema from pandas DataFrame
    schema = StructType()
    for col_name, dtype in pandas_df.dtypes.items():
        if dtype == 'object':
            schema.add(StructField(col_name, StringType(), True))
        elif dtype in ['int64', 'int32']:
            schema.add(StructField(col_name, IntegerType(), True))
        else:
            schema.add(StructField(col_name, DoubleType(), True))
    
    return spark.createDataFrame(pandas_df, schema)


def spark_to_pandas_df(spark_df: pd.DataFrame) -> pd.DataFrame:
    """Convert Spark DataFrame to pandas DataFrame."""
    return spark_df.toPandas()


def compare_pipelines(config_path: str, data_path: str):
    """Compare pandas and Spark pipeline outputs."""
    print("Loading data and configuration...")
    
    # Load data
    pandas_data = pd.read_parquet(data_path)
    config = load_config(config_path)
    
    # Initialize Spark
    spark = SparkSession.builder.appName("PipelineComparison").getOrCreate()
    # spark = SparkSession.builder \
    # .appName("PipelineComparison") \
    # .config("spark.driver.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED") \
    # .getOrCreate()
    spark_data = pandas_to_spark_df(pandas_data, spark)
    
    print(f"Data shape: {pandas_data.shape}")
    print(f"Columns: {list(pandas_data.columns)}")
    
    # Test pandas pipeline
    print("\n=== Testing Pandas Pipeline ===")
    start_time = time.time()
    
    pandas_transformer = Transformer(config)
    pandas_transformer.fit(pandas_data)
    pandas_transformed = pandas_transformer.transform(pandas_data.copy())
    pandas_inversed = pandas_transformer.inverse_transform(pandas_transformed.copy())
    
    pandas_time = time.time() - start_time
    print(f"Pandas pipeline time: {pandas_time:.2f} seconds")
    
    # Test Spark pipeline
    print("\n=== Testing Spark Pipeline ===")
    start_time = time.time()
    
    spark_transformer = SparkTransformer(config, spark)
    spark_transformer.fit(spark_data)
    spark_transformed = spark_transformer.transform(spark_data)
    spark_inversed = spark_transformer.inverse_transform(spark_transformed)
    
    spark_time = time.time() - start_time
    print(f"Spark pipeline time: {spark_time:.2f} seconds")
    
    # Convert Spark results to pandas for comparison
    spark_transformed_pandas = spark_to_pandas_df(spark_transformed)
    spark_inversed_pandas = spark_to_pandas_df(spark_inversed)
    
    # Compare results
    print("\n=== Comparing Results ===")
    
    # Compare transformed data
    print("Comparing transformed data...")
    for col in pandas_data.columns:
        if col in config:
            pandas_col = pandas_transformed[col]
            spark_col = spark_transformed_pandas[col]
            
            # Check if columns are similar (allowing for small numerical differences)
            if pandas_col.dtype in ['float64', 'float32']:
                diff = abs(pandas_col - spark_col).max()
                print(f"  {col}: max difference = {diff:.6f}")
            else:
                # For non-numeric columns, check exact equality
                is_equal = pandas_col.equals(spark_col)
                print(f"  {col}: equal = {is_equal}")
    
    # Compare inverse transformed data
    print("\nComparing inverse transformed data...")
    for col in pandas_data.columns:
        if col in config:
            pandas_col = pandas_inversed[col]
            spark_col = spark_inversed_pandas[col]
            
            # Check if columns are similar (allowing for small numerical differences)
            if pandas_col.dtype in ['float64', 'float32']:
                diff = abs(pandas_col - spark_col).max()
                print(f"  {col}: max difference = {diff:.6f}")
            else:
                # For non-numeric columns, check exact equality
                is_equal = pandas_col.equals(spark_col)
                print(f"  {col}: equal = {is_equal}")
    
    print(f"\n=== Summary ===")
    print(f"Pandas pipeline time: {pandas_time:.2f} seconds")
    print(f"Spark pipeline time: {spark_time:.2f} seconds")
    print(f"Speedup: {pandas_time / spark_time:.2f}x")
    
    # Clean up
    spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_path", type=str, default="config.yaml")
    parser.add_argument("--data_path", type=str, default="data/small/001.parquet")
    args = parser.parse_args()
    
    compare_pipelines(args.config_path, args.data_path) 