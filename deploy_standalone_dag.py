#!/usr/bin/env python3
"""
Deploy the standalone Spark DAG to Airflow.
"""

import os
import shutil
from pathlib import Path

def deploy_standalone_dag():
    """Deploy the standalone DAG to Airflow."""
    print("Deploying standalone Spark DAG to Airflow...")
    
    # Get paths
    current_dir = Path(__file__).parent.absolute()
    source_dag = current_dir / "airflow_dags" / "spark_optimized_dag_standalone.py"
    
    # Get Airflow home
    airflow_home = os.environ.get('AIRFLOW_HOME', os.path.expanduser('~/airflow'))
    airflow_dags_dir = Path(airflow_home) / "dags"
    
    print(f"Source DAG: {source_dag}")
    print(f"Airflow DAGs directory: {airflow_dags_dir}")
    
    # Create Airflow DAGs directory if it doesn't exist
    airflow_dags_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy the standalone DAG
    target_dag = airflow_dags_dir / "spark_optimized_dag_standalone.py"
    shutil.copy2(source_dag, target_dag)
    
    print(f"✅ DAG deployed to: {target_dag}")
    
    # Test the DAG
    print("\nTesting DAG import...")
    try:
        import sys
        sys.path.insert(0, str(airflow_dags_dir))
        
        # Import the DAG
        import spark_optimized_dag_standalone
        
        print("✅ DAG imported successfully!")
        print(f"DAG ID: {spark_optimized_dag_standalone.dag.dag_id}")
        print(f"Number of tasks: {len(spark_optimized_dag_standalone.dag.tasks)}")
        
        # Print task information
        print("\nTasks:")
        for task in spark_optimized_dag_standalone.dag.tasks:
            print(f"  - {task.task_id} ({type(task).__name__})")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to import DAG: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function."""
    print("Standalone Spark DAG Deployment")
    print("=" * 40)
    
    if deploy_standalone_dag():
        print("\n" + "=" * 40)
        print("✅ Deployment successful!")
        print("\nNext steps:")
        print("1. Start Airflow webserver (if not running):")
        print("   airflow webserver --port 8080")
        print("2. Start Airflow scheduler (if not running):")
        print("   airflow scheduler")
        print("3. Trigger the DAG:")
        print("   airflow dags trigger spark_optimized_transformer_pipeline_standalone")
        print("4. View the DAG in Airflow UI: http://localhost:8080")
        print("\nThe standalone DAG includes:")
        print("- Embedded monitoring classes")
        print("- Mock transformer for testing")
        print("- Automatic test data generation")
        print("- No external dependencies")
    else:
        print("\n❌ Deployment failed. Please check the error messages above.")

if __name__ == "__main__":
    main() 