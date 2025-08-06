# debug.py
import sys
sys.path.append('/Users/zhangzhexu/Downloads/take-home-task')

from airflow_dags import spark_pipeline_dag as pipeline

def main():
    print("=== Step 1: Load config ===")
    cols = pipeline.load_config()
    print(f"Loaded {len(cols)} columns: {cols}")

    print("\n=== Step 2: Load and cache data ===")
    data_info = pipeline.load_and_cache_data()
    print(f"Data info: {data_info}")

    print("\n=== Step 3: Fit each column transformer ===")
    for col in cols:
        print(f"Fitting column: {col}")
        pipeline.fit_column_transformer(col)

    print("\n=== Step 4: Build and apply transformations ===")
    results = pipeline.build_and_apply_transformations()
    print(f"Pipeline results: {results}")

    print("\n=== Step 5: Cleanup temp files ===")
    pipeline.cleanup_temp_files()
    print("Done.")

if __name__ == "__main__":
    main()
