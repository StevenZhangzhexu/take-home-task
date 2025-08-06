# BD Transformer Pipeline - Spark Implementation

This project implements a distributed data transformation pipeline using Apache Spark, converting the original pandas-based `bd_transformer` package to handle large-scale datasets efficiently.

## Project Structure

```
├── bd_transformer/
│   ├── components/           # Original pandas components
│   ├── spark_components/     # Spark-based components
│   ├── transformer.py        # Original pandas transformer
│   └── spark_transformer.py  # Spark-based transformer
├── airflow_dags/            # Airflow pipeline definitions
├── data/                    # Data directories
├── results/                 # Performance results and plots
├── config.yaml              # Configuration file
├── run.py                   # Original pandas pipeline runner
├── compare_pipelines.py     # Comparison script
├── generate_large_dataset.py # Large dataset generator
├── performance_test.py      # Performance testing script
└── PROJECT_README.md        # Detailed implementation notes
└── requirements.txt.        # for conda env
```

## Setup

**Install the package in a new environment:**
   ```bash
   conda create spark
   conda activate spark
   pip install -r requirements.txt
   export PYSPARK_PYTHON=$(which python)
   export PYSPARK_DRIVER_PYTHON=$(which python)
   ```


## Quick Start

### Step 1: Test the Original Pipeline

```bash
# Run the original pandas pipeline
python run.py --config_path config.yaml --data_path data/small/001.parquet
```

### Step 2: Compare Pandas vs Spark (on small dataset)

```bash
# Compare pandas and Spark implementations
python compare_pipelines.py --config_path config.yaml --data_path data/small/001.parquet
```

### Step 3: Generate Large Dataset

```bash
# Generate a dataset 20x larger than your system memory
python generate_large_dataset.py --output_path data/large/ --memory_multiplier 20.0
```
In my case:
* Available Memory: 0.30 GB
* large data original size = 0.3 x 20 GB = 6144 MB
* large data in parquet format = 392MB

### Step 4: Performance Testing (for large data)

```bash
# Test performance on large dataset
python performance_test.py --data_path data/large/ --config_path config.yaml --output_dir results/
```


##  Airflow Pipeline Usage


   ```bash
   export AIRFLOW_HOME=~/airflow
mkdir -p $AIRFLOW_HOME/dags
cp airflow_dags/* $AIRFLOW_HOME/dags/

airflow standalone
   ```
then trigger task from Airflow UI




## Performance Testing & Comparison


1. **System Specifications**: CPU, memory, and disk information
   ```
   Total Memory: 8.00 GB
   Available Memory: 0.30 GB
   CPU Count: 8
   CPU Frequency: 2400.00 MHz
   ```
2. **Performance Comparison**: Pandas vs Spark execution times
   ```
   === Results Summary ===
   Pandas Pipeline:
   Fit time: 54.05 seconds
   Transform time: 31.22 seconds
   Inverse time: 123.77 seconds
   Total time: 209.05 seconds

   Spark Pipeline:
   Fit time: 26.07 seconds
   Transform time: 1.28 seconds
   Inverse time: 1.20 seconds
   Total time: 28.55 seconds
   ```
4. **Resource Monitoring & Visualization**: CPU, memory, and disk I/O usage over time and Performance plots saved to `results/` directory

### Running Performance Tests

```bash
# Test on small dataset
python performance_test.py --data_path data/small/001.parquet --output_dir results/

# Test on large dataset
python performance_test.py --data_path data/large/ --output_dir results/
```

## Results and Outputs

### 1. Performance Results

- **JSON Reports**: Detailed performance metrics in `results/performance_results.json`
- **Visualizations**: CPU and memory usage plots in `results/`
- **System Specs**: Machine specifications and resource utilization

### 2. Comparison Results

The comparison script outputs:
- Execution times for both pipelines
- Accuracy verification (ensuring Spark produces same results as pandas)
- Performance speedup metrics

### 3. Airflow Pipeline Results
WIP


## Troubleshooting
### Airflow 
Error Log
   ```
   Log message source details: sources=["/Users/zhangzhexu/airflow/logs/dag_id=spark_pipeline_with_monitoring/run_id=manual__2025-08-06T10:37:32.085486+00:00/task_id=run_spark_pipeline/attempt=1.log"]
::group::Log message source details: sources=["/Users/zhangzhexu/airflow/logs/dag_id=spark_pipeline_with_monitoring/run_id=manual__2025-08-06T10:37:32.085486+00:00/task_id=run_spark_pipeline/attempt=1.log"]
[2025-08-06, 18:37:41] ERROR - Process terminated by signal: signal=-10: signal_name="SIGBUS": source="task"
   ```

* Issue:
The Airflow task crashes immediately with an OS signal SIGBUS (signal -10), abruptly terminating the Python subprocess. A Bus Error indicates an invalid memory access. Notably, this error occurs before any Python code in the task actually starts executing.

* Potential Fix
Since it's an environmental or hardware-level problems, not application bugs. A clean, isolated Python environment or containerized Airflow deployment could fix it.

## Challenges and Solution
### 1. Spark Performance Issue
- **Problem:**  
  Initial implementation translated the `fit()` method column-by-column from pandas to Spark directly. Each `fit()` invocation spawned multiple Spark subtasks, causing significant overhead and inefficiency. 
- **Solution:**  
  Adopt vectorized processing by combining multiple columns and passing them to a single Spark job. This approach leverages Spark’s distributed data processing efficiently, reducing overhead and improving performance. (fit time dropped from 120+ sec to 30+ sec)

### 2. Accuracy Issues Post-Optimization
- **Problem:**  
  After applying vectorized processing, some accuracy problems emerged due to complex input formats.
- **Solution:**  
  Use regular expressions (regex) to extract numeric values before applying the MinMax transform, which improved accuracy in handling complex data formats. (value difference dropped to <0.01)

### 3. Remaining Minor Differences
- **Observation:**  
  While the solution after step 2 became both efficient and more accurate, small differences (< 0.01) persisted compared to the original implementation.
- **Solution:**  
  Parallelize the `fit()` operation column-by-column using Airflow for orchestration. This enables local execution and achieves very high accuracy, effectively closing the accuracy gap.

### 4. Next Step : Addressing Airflow Deployment Issues
- Further work is ongoing to resolve challenges related to deploying this parallelized processing workflow on Airflow.




