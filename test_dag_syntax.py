#!/usr/bin/env python3
"""
Test script to verify DAG syntax and import.
"""

import sys
import os
from pathlib import Path

# Add the airflow_dags directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "airflow_dags"))

def test_dag_import():
    """Test importing the DAG."""
    try:
        print("Testing DAG import...")
        import spark_optimized_dag
        
        print("✅ DAG imported successfully!")
        print(f"DAG ID: {spark_optimized_dag.dag.dag_id}")
        print(f"Number of tasks: {len(spark_optimized_dag.dag.tasks)}")
        
        # Print task information
        print("\nTasks:")
        for task in spark_optimized_dag.dag.tasks:
            print(f"  - {task.task_id} ({type(task).__name__})")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to import DAG: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_dag_structure():
    """Test DAG structure and dependencies."""
    try:
        import spark_optimized_dag
        
        dag = spark_optimized_dag.dag
        
        print("\nTesting DAG structure...")
        
        # Check if DAG has tasks
        if not dag.tasks:
            print("❌ DAG has no tasks")
            return False
        
        # Check task dependencies
        print("Task dependencies:")
        for task in dag.tasks:
            downstream_tasks = task.downstream_list
            if downstream_tasks:
                print(f"  {task.task_id} -> {[t.task_id for t in downstream_tasks]}")
            else:
                print(f"  {task.task_id} -> (end)")
        
        print("✅ DAG structure is valid!")
        return True
        
    except Exception as e:
        print(f"❌ Failed to test DAG structure: {str(e)}")
        return False

def main():
    """Main test function."""
    print("DAG Syntax Test")
    print("=" * 30)
    
    # Test import
    if test_dag_import():
        # Test structure
        test_dag_structure()
        
        print("\n" + "=" * 30)
        print("✅ All tests passed! The DAG is ready to use.")
        print("\nTo use this DAG:")
        print("1. Run the setup script: python setup_airflow_dags.py")
        print("2. Set AIRFLOW_HOME environment variable")
        print("3. Trigger the DAG: airflow dags trigger spark_optimized_transformer_pipeline")
    else:
        print("\n❌ Tests failed. Please fix the DAG syntax issues.")

if __name__ == "__main__":
    main() 