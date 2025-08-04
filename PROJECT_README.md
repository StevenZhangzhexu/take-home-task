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
└── IMPLEMENTATION_NOTES.md  # Detailed implementation notes
```

## Installation

1. **Install the package in development mode:**
   ```bash
   pip install -e .
   ```

2. **Install additional dependencies:**
   ```bash
   pip install psutil matplotlib
   ```

## Quick Start

### Step 1: Test the Original Pipeline

```bash
# Run the original pandas pipeline
python run.py --config_path config.yaml --data_path data/small/001.parquet
```

### Step 2: Compare Pandas vs Spark

```bash
# Compare pandas and Spark implementations
python compare_pipelines.py --config_path config.yaml --data_path data/small/001.parquet
```

### Step 3: Generate Large Dataset

```bash
# Generate a dataset 20x larger than your system memory
python generate_large_dataset.py --output_path data/large/ --memory_multiplier 20.0
```

### Step 4: Performance Testing

```bash
# Test performance on large dataset
python performance_test.py --data_path data/large/ --config_path config.yaml --output_dir results/
```

## Detailed Usage

### 1. Spark Transformer Usage

```python
from pyspark.sql import SparkSession
from bd_transformer.spark_transformer import SparkTransformer
import yaml

# Initialize Spark
spark = SparkSession.builder.appName("BDTransformer").getOrCreate()

# Load configuration
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Load data
data = spark.read.parquet("data/large/")

# Create and fit transformer
transformer = SparkTransformer(config, spark)
transformer.fit(data)

# Transform data
transformed = transformer.transform(data)

# Inverse transform
inversed = transformer.inverse_transform(transformed)
```

### 2. Airflow Pipeline Usage

1. **Set up Airflow:**
   ```bash
   # Install Airflow
   pip install apache-airflow[spark]
   
   # Initialize Airflow database
   airflow db init
   
   # Start Airflow webserver
   airflow webserver --port 8080
   
   # Start Airflow scheduler
   airflow scheduler
   ```

2. **Copy DAG files:**
   ```bash
   cp airflow_dags/* $AIRFLOW_HOME/dags/
   ```

3. **Trigger the pipeline:**
   ```bash
   airflow dags trigger spark_transformer_pipeline \
     --conf '{"config_path": "config.yaml", "data_path": "data/large/", "output_path": "results/"}'
   ```

## Configuration

The `config.yaml` file defines the transformation parameters for each column:

```yaml
day_of_month:
  converter:
    min_val: 1
    max_val: 31
    clip_oor: False
    prefix: ""
    suffix: ""
    rounding: 0
  normalizer:
    clip: False
    reject: True
# ... more columns
```

## Performance Testing

The performance testing script provides comprehensive benchmarking:

1. **System Specifications**: CPU, memory, and disk information
2. **Resource Monitoring**: CPU, memory, and disk I/O usage over time
3. **Performance Comparison**: Pandas vs Spark execution times
4. **Visualization**: Performance plots saved to `results/` directory

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

- **Task Status**: Individual task success/failure status
- **Execution Logs**: Detailed logs for each pipeline step
- **XCom Data**: Parameter passing between tasks

## Troubleshooting

### Common Issues

1. **Memory Issues**: If you encounter memory errors:
   - Reduce the `memory_multiplier` in `generate_large_dataset.py`
   - Increase Spark executor memory settings
   - Use smaller chunk sizes in data generation

2. **Spark Connection Issues**: If Spark fails to start:
   - Check Java installation (Spark requires Java 8+)
   - Verify SPARK_HOME environment variable
   - Check available memory and CPU resources

3. **Airflow Issues**: If Airflow tasks fail:
   - Check Airflow logs in `$AIRFLOW_HOME/logs/`
   - Verify DAG file syntax
   - Check XCom data passing between tasks

4. **Spark Data Type Issues**: If you encounter data type mismatch errors:
   - The Spark converter now uses `concat()` function for string concatenation
   - Test with `test_spark_fix.py` to verify the fix works
   - Check that string columns are properly handled with prefix/suffix

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Development

### Adding New Columns

1. **Update config.yaml** with new column configuration
2. **Test with small dataset** using `compare_pipelines.py`
3. **Update Airflow DAG** if needed for new columns
4. **Run performance tests** to ensure scalability

### Extending the Pipeline

1. **New Transformers**: Add new transformer classes in `spark_components/`
2. **New Operations**: Extend the SparkTransformer class
3. **New Airflow Tasks**: Add tasks to the DAG files
4. **Testing**: Update comparison and performance scripts

## Contributing

1. **Code Style**: Follow PEP 8 guidelines
2. **Testing**: Add tests for new functionality
3. **Documentation**: Update documentation for new features
4. **Performance**: Ensure new code doesn't degrade performance

## License

This project is for educational and evaluation purposes.

## Support

For questions or issues:
1. Check the `IMPLEMENTATION_NOTES.md` for detailed explanations
2. Review the troubleshooting section above
3. Check the Airflow and Spark documentation for framework-specific issues 