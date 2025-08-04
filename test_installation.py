#!/usr/bin/env python3
"""
Test script to verify that all components are working correctly.
"""

import sys
import os
import yaml
import pandas as pd

def test_imports():
    """Test that all required packages can be imported."""
    print("Testing imports...")
    
    try:
        import numpy as np
        print("✓ numpy imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import numpy: {e}")
        return False
    
    try:
        import pandas as pd
        print("✓ pandas imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import pandas: {e}")
        return False
    
    try:
        import yaml
        print("✓ yaml imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import yaml: {e}")
        return False
    
    try:
        import psutil
        print("✓ psutil imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import psutil: {e}")
        return False
    
    try:
        import matplotlib.pyplot as plt
        print("✓ matplotlib imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import matplotlib: {e}")
        return False
    
    try:
        from pyspark.sql import SparkSession
        print("✓ pyspark imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import pyspark: {e}")
        return False
    
    return True


def test_bd_transformer_imports():
    """Test that bd_transformer components can be imported."""
    print("\nTesting bd_transformer imports...")
    
    try:
        from bd_transformer.transformer import Transformer
        print("✓ Original Transformer imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import original Transformer: {e}")
        return False
    
    try:
        from bd_transformer.spark_transformer import SparkTransformer
        print("✓ SparkTransformer imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import SparkTransformer: {e}")
        return False
    
    try:
        from bd_transformer.components.converter import Converter
        print("✓ Original Converter imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import original Converter: {e}")
        return False
    
    try:
        from bd_transformer.components.normalizer import Normalizer
        print("✓ Original Normalizer imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import original Normalizer: {e}")
        return False
    
    try:
        from bd_transformer.spark_components.converter import SparkConverter
        print("✓ SparkConverter imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import SparkConverter: {e}")
        return False
    
    try:
        from bd_transformer.spark_components.normalizer import SparkNormalizer
        print("✓ SparkNormalizer imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import SparkNormalizer: {e}")
        return False
    
    return True


def test_config_loading():
    """Test that configuration can be loaded."""
    print("\nTesting configuration loading...")
    
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
        print("✓ Configuration loaded successfully")
        print(f"  Found {len(config)} columns in configuration")
        return True
    except FileNotFoundError:
        print("✗ config.yaml not found")
        return False
    except yaml.YAMLError as e:
        print(f"✗ Failed to parse config.yaml: {e}")
        return False


def test_data_loading():
    """Test that data can be loaded."""
    print("\nTesting data loading...")
    
    try:
        data = pd.read_parquet("data/small/001.parquet")
        print("✓ Data loaded successfully")
        print(f"  Data shape: {data.shape}")
        print(f"  Columns: {list(data.columns)}")
        return True
    except FileNotFoundError:
        print("✗ data/small/001.parquet not found")
        return False
    except Exception as e:
        print(f"✗ Failed to load data: {e}")
        return False


def test_spark_session():
    """Test that Spark session can be created."""
    print("\nTesting Spark session...")
    
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("TestSession").getOrCreate()
        print("✓ Spark session created successfully")
        print(f"  Spark version: {spark.version}")
        spark.stop()
        return True
    except Exception as e:
        print(f"✗ Failed to create Spark session: {e}")
        return False


def main():
    """Run all tests."""
    print("BD Transformer Installation Test")
    print("=" * 40)
    
    tests = [
        test_imports,
        test_bd_transformer_imports,
        test_config_loading,
        test_data_loading,
        test_spark_session,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 40)
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! Installation is successful.")
        return 0
    else:
        print("❌ Some tests failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 