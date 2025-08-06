#!/usr/bin/env python3
"""
Test script for the working Spark DAG.
"""

import sys
import os
from pathlib import Path

# Add the airflow_dags directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "airflow_dags"))

def test_working_dag():
    """Test the working DAG."""
    try:
        print("Testing working DAG import...")
        import working_spark_dag
        
        print("✅ Working DAG imported successfully!")
        print(f"DAG ID: {working_spark_dag.dag.dag_id}")
        print(f"Number of tasks: {len(working_spark_dag.dag.tasks)}")
        
        # Print task information
        print("\nTasks:")
        for task in working_spark_dag.dag.tasks:
            print(f"  - {task.task_id} ({type(task).__name__})")
        
        # Print dependencies
        print("\nDependencies:")
        for task in working_spark_dag.dag.tasks:
            downstream_tasks = task.downstream_list
            if downstream_tasks:
                print(f"  {task.task_id} -> {[t.task_id for t in downstream_tasks]}")
            else:
                print(f"  {task.task_id} -> (end)")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to import working DAG: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function."""
    print("Working DAG Test")
    print("=" * 30)
    
    if test_working_dag():
        print("\n✅ Working DAG test passed!")
        print("This DAG should work with your Airflow installation.")
        print("\nTo use this DAG:")
        print("1. Run: python setup_airflow_dags.py")
        print("2. Set AIRFLOW_HOME environment variable")
        print("3. Trigger: airflow dags trigger working_spark_transformer_pipeline")
    else:
        print("\n❌ Working DAG test failed.")

if __name__ == "__main__":
    main() 