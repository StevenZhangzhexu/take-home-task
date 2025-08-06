import threading
import time
import psutil
import json
import os
import requests
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


class RealTimeMonitor:
    """Non-blocking real-time performance monitor."""
    
    def __init__(self, output_file: str = "performance_metrics.json", sample_interval: float = 1.0):
        self.output_file = output_file
        self.sample_interval = sample_interval
        self.metrics = {
            'timestamps': [],
            'cpu_usage': [],
            'memory_usage': [],
            'disk_io': [],
            'network_io': [],
            'process_metrics': []
        }
        self.monitoring = False
        self.monitor_thread = None
        self.start_time = None
        
    def start_monitoring(self):
        """Start monitoring in background thread."""
        self.monitoring = True
        self.start_time = time.time()
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        print(f"Started real-time monitoring. Metrics will be saved to {self.output_file}")
        
    def stop_monitoring(self):
        """Stop monitoring and save metrics."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        
        # Save metrics to file
        with open(self.output_file, 'w') as f:
            json.dump(self.metrics, f, indent=2)
        print(f"Monitoring stopped. Metrics saved to {self.output_file}")
        
    def _monitor_loop(self):
        """Background monitoring loop."""
        # Initialize disk and network counters
        last_disk_io = psutil.disk_io_counters()
        last_net_io = psutil.net_io_counters()
        last_time = time.time()
        
        while self.monitoring:
            current_time = time.time()
            
            # System metrics
            cpu_percent = psutil.cpu_percent()
            memory_info = psutil.virtual_memory()
            
            # Disk I/O rate calculation
            try:
                current_disk_io = psutil.disk_io_counters()
                if last_disk_io:
                    time_diff = current_time - last_time
                    disk_read_rate = (current_disk_io.read_bytes - last_disk_io.read_bytes) / time_diff / 1024 / 1024  # MB/s
                    disk_write_rate = (current_disk_io.write_bytes - last_disk_io.write_bytes) / time_diff / 1024 / 1024  # MB/s
                    disk_io_total = disk_read_rate + disk_write_rate
                else:
                    disk_io_total = 0
                last_disk_io = current_disk_io
            except:
                disk_io_total = 0
            
            # Network I/O rate calculation
            try:
                current_net_io = psutil.net_io_counters()
                if last_net_io:
                    time_diff = current_time - last_time
                    net_sent_rate = (current_net_io.bytes_sent - last_net_io.bytes_sent) / time_diff / 1024 / 1024  # MB/s
                    net_recv_rate = (current_net_io.bytes_recv - last_net_io.bytes_recv) / time_diff / 1024 / 1024  # MB/s
                    net_io_total = net_sent_rate + net_recv_rate
                else:
                    net_io_total = 0
                last_net_io = current_net_io
            except:
                net_io_total = 0
            
            # Process-specific metrics
            try:
                process = psutil.Process()
                process_cpu = process.cpu_percent()
                process_memory = process.memory_info().rss / 1024 / 1024  # MB
                process_threads = process.num_threads()
            except:
                process_cpu = 0
                process_memory = 0
                process_threads = 0
            
            # Store metrics
            self.metrics['timestamps'].append(current_time - self.start_time)
            self.metrics['cpu_usage'].append(cpu_percent)
            self.metrics['memory_usage'].append(memory_info.percent)
            self.metrics['disk_io'].append(disk_io_total)
            self.metrics['network_io'].append(net_io_total)
            self.metrics['process_metrics'].append({
                'cpu_percent': process_cpu,
                'memory_mb': process_memory,
                'threads': process_threads
            })
            
            last_time = current_time
            time.sleep(self.sample_interval)


class SparkParallelMonitor:
    """
    Enhanced monitor designed to work in parallel with Spark operations.
    Handles the vectorized nature of Spark processing and provides
    Spark-specific optimizations.
    """
    
    def __init__(self, output_dir: str = "monitoring_results", sample_interval: float = 0.5):
        self.output_dir = output_dir
        self.sample_interval = sample_interval
        self.monitoring = False
        self.monitor_thread = None
        self.start_time = None
        
        # Separate metrics for different aspects
        self.system_metrics = {
            'timestamps': [],
            'cpu_usage': [],
            'memory_usage': [],
            'disk_io': [],
            'network_io': []
        }
        
        self.spark_metrics = {
            'timestamps': [],
            'executor_metrics': [],
            'stage_metrics': [],
            'memory_metrics': []
        }
        
        self.pipeline_phases = {
            'fit': {'start': None, 'end': None, 'duration': None},
            'transform': {'start': None, 'end': None, 'duration': None},
            'inverse_transform': {'start': None, 'end': None, 'duration': None}
        }
        
        os.makedirs(output_dir, exist_ok=True)
        
    def start_monitoring(self):
        """Start parallel monitoring."""
        self.monitoring = True
        self.start_time = time.time()
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        print(f"Started Spark parallel monitoring. Results will be saved to {self.output_dir}")
        
    def stop_monitoring(self):
        """Stop monitoring and save all metrics."""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        
        # Save all metrics
        self._save_metrics()
        print(f"Spark parallel monitoring stopped. Results saved to {self.output_dir}")
        
    def mark_phase_start(self, phase: str):
        """Mark the start of a pipeline phase."""
        if phase in self.pipeline_phases:
            self.pipeline_phases[phase]['start'] = time.time() - self.start_time
            print(f"Phase '{phase}' started at {self.pipeline_phases[phase]['start']:.2f}s")
            
    def mark_phase_end(self, phase: str):
        """Mark the end of a pipeline phase."""
        if phase in self.pipeline_phases:
            self.pipeline_phases[phase]['end'] = time.time() - self.start_time
            self.pipeline_phases[phase]['duration'] = (
                self.pipeline_phases[phase]['end'] - self.pipeline_phases[phase]['start']
            )
            print(f"Phase '{phase}' completed in {self.pipeline_phases[phase]['duration']:.2f}s")
    
    def _monitor_loop(self):
        """Enhanced monitoring loop optimized for Spark operations."""
        last_disk_io = psutil.disk_io_counters()
        last_net_io = psutil.net_io_counters()
        last_time = time.time()
        
        while self.monitoring:
            current_time = time.time()
            elapsed_time = current_time - self.start_time
            
            # System metrics (optimized for Spark workloads)
            cpu_percent = psutil.cpu_percent(interval=None)  # Non-blocking
            memory_info = psutil.virtual_memory()
            
            # Enhanced disk I/O monitoring for Spark shuffle operations
            try:
                current_disk_io = psutil.disk_io_counters()
                if last_disk_io:
                    time_diff = current_time - last_time
                    disk_read_rate = (current_disk_io.read_bytes - last_disk_io.read_bytes) / time_diff / 1024 / 1024
                    disk_write_rate = (current_disk_io.write_bytes - last_disk_io.write_bytes) / time_diff / 1024 / 1024
                    disk_io_total = disk_read_rate + disk_write_rate
                else:
                    disk_io_total = 0
                last_disk_io = current_disk_io
            except:
                disk_io_total = 0
            
            # Network I/O monitoring for Spark cluster communication
            try:
                current_net_io = psutil.net_io_counters()
                if last_net_io:
                    time_diff = current_time - last_time
                    net_sent_rate = (current_net_io.bytes_sent - last_net_io.bytes_sent) / time_diff / 1024 / 1024
                    net_recv_rate = (current_net_io.bytes_recv - last_net_io.bytes_recv) / time_diff / 1024 / 1024
                    net_io_total = net_sent_rate + net_recv_rate
                else:
                    net_io_total = 0
                last_net_io = current_net_io
            except:
                net_io_total = 0
            
            # Store system metrics
            self.system_metrics['timestamps'].append(elapsed_time)
            self.system_metrics['cpu_usage'].append(cpu_percent)
            self.system_metrics['memory_usage'].append(memory_info.percent)
            self.system_metrics['disk_io'].append(disk_io_total)
            self.system_metrics['network_io'].append(net_io_total)
            
            # Spark-specific metrics (if Spark context is available)
            self._collect_spark_metrics(elapsed_time)
            
            last_time = current_time
            time.sleep(self.sample_interval)
    
    def _collect_spark_metrics(self, elapsed_time: float):
        """Collect Spark-specific metrics if available."""
        try:
            # Try to get Spark context from global scope
            import pyspark
            spark_context = None
            
            # This is a simplified approach - in practice, you'd pass the Spark session
            # to the monitor or use a different mechanism to access it
            if hasattr(pyspark, '_sc') and pyspark._sc:
                spark_context = pyspark._sc
            elif 'spark' in globals():
                spark_context = globals()['spark'].sparkContext
                
            if spark_context:
                # Collect basic Spark metrics
                spark_metrics = {
                    'timestamp': elapsed_time,
                    'application_id': spark_context.applicationId,
                    'default_parallelism': spark_context.defaultParallelism,
                    'master': spark_context.master
                }
                
                self.spark_metrics['timestamps'].append(elapsed_time)
                self.spark_metrics['executor_metrics'].append(spark_metrics)
                
        except Exception as e:
            # Spark context not available, skip Spark metrics
            pass
    
    def _save_metrics(self):
        """Save all collected metrics to files."""
        # Save system metrics
        system_file = os.path.join(self.output_dir, "system_metrics.json")
        with open(system_file, 'w') as f:
            json.dump(self.system_metrics, f, indent=2)
        
        # Save Spark metrics
        spark_file = os.path.join(self.output_dir, "spark_metrics.json")
        with open(spark_file, 'w') as f:
            json.dump(self.spark_metrics, f, indent=2)
        
        # Save pipeline phases
        phases_file = os.path.join(self.output_dir, "pipeline_phases.json")
        with open(phases_file, 'w') as f:
            json.dump(self.pipeline_phases, f, indent=2)
        
        # Create summary report
        self._create_summary_report()
    
    def _create_summary_report(self):
        """Create a summary report of the monitoring session."""
        summary = {
            'monitoring_duration': time.time() - self.start_time,
            'total_samples': len(self.system_metrics['timestamps']),
            'pipeline_phases': self.pipeline_phases,
            'system_summary': self._calculate_system_summary(),
            'spark_summary': self._calculate_spark_summary()
        }
        
        summary_file = os.path.join(self.output_dir, "monitoring_summary.json")
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
    
    def _calculate_system_summary(self):
        """Calculate summary statistics for system metrics."""
        if not self.system_metrics['cpu_usage']:
            return {}
        
        return {
            'cpu': {
                'avg_usage': sum(self.system_metrics['cpu_usage']) / len(self.system_metrics['cpu_usage']),
                'max_usage': max(self.system_metrics['cpu_usage']),
                'min_usage': min(self.system_metrics['cpu_usage'])
            },
            'memory': {
                'avg_usage': sum(self.system_metrics['memory_usage']) / len(self.system_metrics['memory_usage']),
                'max_usage': max(self.system_metrics['memory_usage']),
                'min_usage': min(self.system_metrics['memory_usage'])
            },
            'disk_io': {
                'avg_rate': sum(self.system_metrics['disk_io']) / len(self.system_metrics['disk_io']),
                'max_rate': max(self.system_metrics['disk_io']),
                'total_io': sum(self.system_metrics['disk_io']) * self.sample_interval
            },
            'network_io': {
                'avg_rate': sum(self.system_metrics['network_io']) / len(self.system_metrics['network_io']),
                'max_rate': max(self.system_metrics['network_io']),
                'total_io': sum(self.system_metrics['network_io']) * self.sample_interval
            }
        }
    
    def _calculate_spark_summary(self):
        """Calculate summary statistics for Spark metrics."""
        if not self.spark_metrics['executor_metrics']:
            return {}
        
        return {
            'total_spark_metrics': len(self.spark_metrics['executor_metrics']),
            'application_id': self.spark_metrics['executor_metrics'][0].get('application_id', 'unknown') if self.spark_metrics['executor_metrics'] else 'unknown'
        }


class SparkMetricsCollector:
    """Collect metrics from Spark UI."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.app_id = None
        self.metrics_history = []
        
    def get_spark_ui_url(self) -> Optional[str]:
        """Get Spark UI URL."""
        try:
            spark_context = self.spark.sparkContext
            self.app_id = spark_context.applicationId
            ui_url = spark_context.uiWebUrl
            return ui_url
        except Exception as e:
            print(f"Error getting Spark UI URL: {e}")
            return None
            
    def collect_spark_metrics(self) -> Dict:
        """Collect metrics from Spark UI."""
        ui_url = self.get_spark_ui_url()
        if not ui_url:
            return {}
            
        try:
            # Get application metrics from Spark UI
            response = requests.get(f"{ui_url}/api/v1/applications/{self.app_id}", timeout=10)
            if response.status_code == 200:
                app_data = response.json()
                
                # Get stage metrics
                stages_response = requests.get(f"{ui_url}/api/v1/applications/{self.app_id}/stages", timeout=10)
                stages_data = stages_response.json() if stages_response.status_code == 200 else []
                
                # Get executor metrics
                executors_response = requests.get(f"{ui_url}/api/v1/applications/{self.app_id}/executors", timeout=10)
                executors_data = executors_response.json() if executors_response.status_code == 200 else []
                
                metrics = {
                    'timestamp': time.time(),
                    'application': {
                        'id': self.app_id,
                        'name': app_data.get('name', ''),
                        'state': app_data.get('state', ''),
                        'start_time': app_data.get('startTime', 0),
                        'end_time': app_data.get('endTime', 0),
                        'duration': app_data.get('duration', 0),
                        'executor_memory_used': app_data.get('executorMemoryUsed', 0),
                        'executor_memory_max': app_data.get('executorMemoryMax', 0),
                        'executor_cores_used': app_data.get('executorCoresUsed', 0),
                        'executor_cores_max': app_data.get('executorCoresMax', 0),
                        'stages_completed': app_data.get('stagesCompleted', 0),
                        'stages_failed': app_data.get('stagesFailed', 0),
                        'jobs_completed': app_data.get('jobsCompleted', 0),
                        'jobs_failed': app_data.get('jobsFailed', 0)
                    },
                    'stages': stages_data,
                    'executors': executors_data
                }
                
                self.metrics_history.append(metrics)
                return metrics
                
        except Exception as e:
            print(f"Error collecting Spark metrics: {e}")
            
        return {}


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


def monitor_pipeline_with_spark(spark: SparkSession, pipeline_func, *args, **kwargs):
    """Monitor pipeline execution with Spark metrics."""
    
    # Initialize monitors
    system_monitor = RealTimeMonitor()
    spark_collector = SparkMetricsCollector(spark)
    
    # Start monitoring
    system_monitor.start_monitoring()
    
    # Get Spark UI URL
    spark_ui_url = spark_collector.get_spark_ui_url()
    if spark_ui_url:
        print(f"Spark UI available at: {spark_ui_url}")
    
    # Execute pipeline
    start_time = time.time()
    try:
        result = pipeline_func(*args, **kwargs)
        success = True
    except Exception as e:
        result = None
        success = False
        print(f"Pipeline failed: {e}")
    
    execution_time = time.time() - start_time
    
    # Stop monitoring
    system_monitor.stop_monitoring()
    
    # Collect final Spark metrics
    spark_metrics = spark_collector.collect_spark_metrics()
    
    return {
        'success': success,
        'execution_time': execution_time,
        'system_metrics': system_monitor.metrics,
        'spark_metrics': spark_metrics,
        'spark_ui_url': spark_ui_url,
        'spark_metrics_history': spark_collector.metrics_history
    }


def test_spark_pipeline_enhanced(data_path: str, config: dict, spark: SparkSession):
    """Enhanced Spark pipeline test with real-time monitoring."""
    
    def run_spark_pipeline():
        """Run the complete Spark pipeline."""
        # Load data
        data = pd.read_parquet(data_path)
        spark_data = pandas_to_spark_df(data, spark)
        
        # Run pipeline
        from bd_transformer.spark_transformer import SparkTransformer
        transformer = SparkTransformer(config, spark)
        transformer.fit(spark_data)
        transformed = transformer.transform(spark_data)
        inversed = transformer.inverse_transform(transformed)
        
        return {
            'data_shape': data.shape,
            'transformed_count': transformed.count(),
            'inversed_count': inversed.count()
        }
    
    # Monitor the entire pipeline
    monitoring_result = monitor_pipeline_with_spark(spark, run_spark_pipeline)
    
    return monitoring_result


def create_spark_session_with_monitoring(app_name: str = "MonitoredPipeline", **configs):
    """Create Spark session with monitoring-friendly configuration."""
    
    builder = SparkSession.builder.appName(app_name)
    
    # Default monitoring-friendly configs
    default_configs = {
        "spark.default.parallelism": 150,
        "spark.sql.shuffle.partitions": 120,
        "spark.driver.memory": "4g",
        "spark.kryoserializer.buffer.max": "512m",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "/tmp/spark-events",
        "spark.history.fs.logDirectory": "/tmp/spark-events"
    }
    
    # Apply default configs
    for key, value in default_configs.items():
        builder = builder.config(key, value)
    
    # Apply custom configs
    for key, value in configs.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()


def analyze_performance_metrics(monitoring_result: Dict) -> Dict:
    """Analyze performance metrics and provide insights."""
    
    system_metrics = monitoring_result.get('system_metrics', {})
    spark_metrics = monitoring_result.get('spark_metrics', {})
    
    analysis = {
        'execution_summary': {
            'total_time': monitoring_result.get('execution_time', 0),
            'success': monitoring_result.get('success', False)
        },
        'system_analysis': {},
        'spark_analysis': {},
        'recommendations': []
    }
    
    # System metrics analysis
    if system_metrics:
        cpu_usage = system_metrics.get('cpu_usage', [])
        memory_usage = system_metrics.get('memory_usage', [])
        
        if cpu_usage:
            analysis['system_analysis']['cpu'] = {
                'avg_usage': sum(cpu_usage) / len(cpu_usage),
                'max_usage': max(cpu_usage),
                'min_usage': min(cpu_usage),
                'utilization_level': 'high' if sum(cpu_usage) / len(cpu_usage) > 80 else 'medium' if sum(cpu_usage) / len(cpu_usage) > 50 else 'low'
            }
        
        if memory_usage:
            analysis['system_analysis']['memory'] = {
                'avg_usage': sum(memory_usage) / len(memory_usage),
                'max_usage': max(memory_usage),
                'min_usage': min(memory_usage),
                'utilization_level': 'high' if sum(memory_usage) / len(memory_usage) > 80 else 'medium' if sum(memory_usage) / len(memory_usage) > 50 else 'low'
            }
    
    # Spark metrics analysis
    if spark_metrics and 'application' in spark_metrics:
        app_metrics = spark_metrics['application']
        
        analysis['spark_analysis'] = {
            'stages': {
                'completed': app_metrics.get('stages_completed', 0),
                'failed': app_metrics.get('stages_failed', 0),
                'success_rate': app_metrics.get('stages_completed', 0) / max(app_metrics.get('stages_completed', 0) + app_metrics.get('stages_failed', 0), 1)
            },
            'jobs': {
                'completed': app_metrics.get('jobs_completed', 0),
                'failed': app_metrics.get('jobs_failed', 0),
                'success_rate': app_metrics.get('jobs_completed', 0) / max(app_metrics.get('jobs_completed', 0) + app_metrics.get('jobs_failed', 0), 1)
            },
            'resources': {
                'memory_utilization': app_metrics.get('executor_memory_used', 0) / max(app_metrics.get('executor_memory_max', 1), 1),
                'core_utilization': app_metrics.get('executor_cores_used', 0) / max(app_metrics.get('executor_cores_max', 1), 1)
            }
        }
    
    # Generate recommendations
    if analysis['system_analysis'].get('cpu', {}).get('utilization_level') == 'low':
        analysis['recommendations'].append("Consider increasing Spark parallelism to better utilize CPU")
    
    if analysis['system_analysis'].get('memory', {}).get('utilization_level') == 'high':
        analysis['recommendations'].append("Consider increasing driver/executor memory or optimizing memory usage")
    
    if analysis['spark_analysis'].get('stages', {}).get('failed', 0) > 0:
        analysis['recommendations'].append("Some Spark stages failed - check logs for errors")
    
    return analysis


def save_monitoring_results(monitoring_result: Dict, output_dir: str = "monitoring_results"):
    """Save monitoring results to files."""
    
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save full monitoring result
    with open(f"{output_dir}/monitoring_result_{timestamp}.json", 'w') as f:
        json.dump(monitoring_result, f, indent=2, default=str)
    
    # Save performance analysis
    analysis = analyze_performance_metrics(monitoring_result)
    with open(f"{output_dir}/performance_analysis_{timestamp}.json", 'w') as f:
        json.dump(analysis, f, indent=2, default=str)
    
    # Save system metrics as CSV for easy plotting
    system_metrics = monitoring_result.get('system_metrics', {})
    if system_metrics:
        df = pd.DataFrame({
            'timestamp': system_metrics.get('timestamps', []),
            'cpu_usage': system_metrics.get('cpu_usage', []),
            'memory_usage': system_metrics.get('memory_usage', []),
            'disk_io': system_metrics.get('disk_io', []),
            'network_io': system_metrics.get('network_io', [])
        })
        df.to_csv(f"{output_dir}/system_metrics_{timestamp}.csv", index=False)
    
    print(f"Monitoring results saved to {output_dir}/")
    return f"{output_dir}/monitoring_result_{timestamp}.json"