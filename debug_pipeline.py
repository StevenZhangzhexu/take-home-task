import sys
sys.path.append('/Users/zhangzhexu/airflow/dags') 

from spark_pipeline_dag import run_pipeline

if __name__ == "__main__":
    run_pipeline(
        data_path="/Users/zhangzhexu/Downloads/take-home-task/data/large/",
        config_path="/Users/zhangzhexu/Downloads/take-home-task/config.yaml",
        output_dir="/Users/zhangzhexu/Downloads/take-home-task/results"
    )