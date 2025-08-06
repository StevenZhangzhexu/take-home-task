# debug.py
import sys
sys.path.append('/Users/zhangzhexu/Downloads/take-home-task')

from airflow_dags import spark_pipeline_dag as pipeline  # Updated import to match your DAG location

def main():
    print("\n=== Step 1: Load config ===")
    config = pipeline.load_config()
    cols = list(config.keys())
    print(f"Loaded {len(cols)} columns: {cols}")

    print("\n=== Step 2: Load data info ===")
    data_cols = pipeline.load_data()
    print(f"Data contains {len(data_cols)} columns: {data_cols}")

    print("\n=== Step 3: Fit & transform entire dataset ===")
    results = pipeline.build_and_apply_transformations()

    print("\n=== Pipeline Timings ===")
    print(f"Fit time:         {results['fit_time']:.4f} sec")
    print(f"Transform time:   {results['transform_time']:.4f} sec")
    print(f"Inverse time:     {results['inverse_time']:.4f} sec")
    print(f"Total time:       {results['total_time']:.4f} sec")
    print(f"Rows transformed: {results['transformed_count']}")
    print(f"Rows inversed:    {results['inversed_count']}")

if __name__ == "__main__":
    main()
