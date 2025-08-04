import argparse
import os
import psutil
import numpy as np
import pandas as pd
from data_gen import generate_data


def get_memory_info():
    """Get system memory information."""
    memory = psutil.virtual_memory()
    total_gb = memory.total / (1024**3)
    available_gb = memory.available / (1024**3)
    print(f"Total memory: {total_gb:.2f} GB")
    print(f"Available memory: {available_gb:.2f} GB")
    return total_gb, available_gb


def estimate_dataset_size(num_rows, num_columns=6):
    """Estimate dataset size in GB."""
    # Rough estimate: each row is about 100 bytes
    estimated_size_gb = (num_rows * num_columns * 100) / (1024**3)
    return estimated_size_gb


def generate_large_dataset(output_path: str, memory_multiplier: float = 20.0):
    """Generate a dataset that is approximately memory_multiplier times the system memory."""
    total_gb, available_gb = get_memory_info()
    
    # Calculate target dataset size
    target_size_gb = available_gb * memory_multiplier
    print(f"Target dataset size: {target_size_gb:.2f} GB")
    
    # Estimate number of rows needed
    estimated_rows = int((target_size_gb * (1024**3)) / (6 * 100))  # 6 columns, ~100 bytes per value
    print(f"Estimated rows needed: {estimated_rows:,}")
    
    # Generate the dataset
    print(f"Generating dataset at: {output_path}")
    generate_data(output_path, num_rows=estimated_rows, chunk_size=10000)
    
    # Verify the actual size
    actual_size_gb = sum(
        os.path.getsize(os.path.join(output_path, f)) 
        for f in os.listdir(output_path) 
        if f.endswith('.parquet')
    ) / (1024**3)
    
    print(f"Actual dataset size: {actual_size_gb:.2f} GB")
    print(f"Memory multiplier achieved: {actual_size_gb / available_gb:.2f}x")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_path", type=str, default="data/large/")
    parser.add_argument("--memory_multiplier", type=float, default=20.0)
    args = parser.parse_args()
    
    generate_large_dataset(args.output_path, args.memory_multiplier) 