#!/usr/bin/env python3
"""
Test script for the simple Spark DAG.
"""

import sys
import os
from pathlib import Path

# Add the airflow_dags directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "airflow_dags"))

def test_simple_dag():
    """Test the simple DAG."""
    try:
        print("Testing simple DAG import...")
        import simple_spark_dag
        
        print("✅ Simple DAG imported successfully!")
        print(f"DAG ID: {simple_spark_dag.dag.dag_id}")
        print(f"Number of tasks: {len(simple_spark_dag.dag.tasks)}")
        
        # Print task information
        print("\nTasks:")
        for task in simple_spark_dag.dag.tasks:
            print(f"  - {task.task_id} ({type(task).__name__})")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to import simple DAG: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function."""
    print("Simple DAG Test")
    print("=" * 30)
    
    if test_simple_dag():
        print("\n✅ Simple DAG test passed!")
        print("The basic DAG syntax is working correctly.")
    else:
        print("\n❌ Simple DAG test failed.")

if __name__ == "__main__":
    main() 