#!/usr/bin/env python3
"""
Setup script for Airflow DAGs.
This script helps configure Airflow to find and load the DAG files.
"""

import os
import sys
import subprocess
from pathlib import Path

def get_airflow_home():
    """Get Airflow home directory."""
    airflow_home = os.environ.get('AIRFLOW_HOME')
    if not airflow_home:
        # Default Airflow home
        airflow_home = os.path.expanduser('~/airflow')
    return airflow_home

def setup_airflow_dags():
    """Set up Airflow to find the DAG files."""
    print("Setting up Airflow DAGs...")
    
    # Get current directory
    current_dir = Path(__file__).parent.absolute()
    airflow_dags_dir = current_dir / "airflow_dags"
    
    # Get Airflow home
    airflow_home = get_airflow_home()
    airflow_dags_folder = Path(airflow_home) / "dags"
    
    print(f"Current directory: {current_dir}")
    print(f"Airflow DAGs directory: {airflow_dags_dir}")
    print(f"Airflow home: {airflow_home}")
    print(f"Airflow DAGs folder: {airflow_dags_folder}")
    
    # Create Airflow home if it doesn't exist
    airflow_home_path = Path(airflow_home)
    if not airflow_home_path.exists():
        print(f"Creating Airflow home directory: {airflow_home}")
        airflow_home_path.mkdir(parents=True, exist_ok=True)
    
    # Create dags folder if it doesn't exist
    if not airflow_dags_folder.exists():
        print(f"Creating Airflow DAGs folder: {airflow_dags_folder}")
        airflow_dags_folder.mkdir(parents=True, exist_ok=True)
    
    # Copy DAG files to Airflow DAGs folder
    print("Copying DAG files to Airflow DAGs folder...")
    for dag_file in airflow_dags_dir.glob("*.py"):
        if dag_file.name.endswith("_dag.py"):
            target_file = airflow_dags_folder / dag_file.name
            print(f"Copying {dag_file.name} to {target_file}")
            
            # Read the source file
            with open(dag_file, 'r') as f:
                content = f.read()
            
            # Write to target file
            with open(target_file, 'w') as f:
                f.write(content)
    
    # Copy configuration files
    print("Copying configuration files...")
    for config_file in airflow_dags_dir.glob("*.yaml"):
        target_file = airflow_dags_folder / config_file.name
        print(f"Copying {config_file.name} to {target_file}")
        
        # Read the source file
        with open(config_file, 'r') as f:
            content = f.read()
        
        # Write to target file
        with open(target_file, 'w') as f:
            f.write(content)
    
    print("DAG files copied successfully!")
    
    # Set AIRFLOW_HOME environment variable
    print(f"\nTo use this setup, set the AIRFLOW_HOME environment variable:")
    print(f"export AIRFLOW_HOME={airflow_home}")
    print(f"Or add it to your shell profile (.bashrc, .zshrc, etc.)")
    
    return airflow_home

def test_dag_loading():
    """Test if the DAG can be loaded by Airflow."""
    print("\nTesting DAG loading...")
    
    try:
        # Import the DAG
        sys.path.insert(0, str(Path(__file__).parent / "airflow_dags"))
        
        # Try to import the DAG
        import spark_optimized_dag
        
        print("✅ DAG loaded successfully!")
        print(f"DAG ID: {spark_optimized_dag.dag.dag_id}")
        print(f"Tasks: {[task.task_id for task in spark_optimized_dag.dag.tasks]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to load DAG: {str(e)}")
        return False

def check_airflow_installation():
    """Check if Airflow is properly installed."""
    print("Checking Airflow installation...")
    
    try:
        result = subprocess.run(['airflow', 'version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print(f"✅ Airflow version: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ Airflow command failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Airflow not found or not accessible: {str(e)}")
        return False

def main():
    """Main setup function."""
    print("Airflow DAG Setup Script")
    print("=" * 50)
    
    # Check Airflow installation
    if not check_airflow_installation():
        print("Please install Airflow first:")
        print("pip install apache-airflow")
        return
    
    # Set up DAGs
    airflow_home = setup_airflow_dags()
    
    # Test DAG loading
    if test_dag_loading():
        print("\n" + "=" * 50)
        print("Setup completed successfully!")
        print("\nNext steps:")
        print("1. Set the AIRFLOW_HOME environment variable:")
        print(f"   export AIRFLOW_HOME={airflow_home}")
        print("2. Initialize Airflow database (if not done already):")
        print("   airflow db init")
        print("3. Start Airflow webserver:")
        print("   airflow webserver --port 8080")
        print("4. Start Airflow scheduler:")
        print("   airflow scheduler")
        print("5. Trigger the DAG:")
        print("   airflow dags trigger spark_optimized_transformer_pipeline")
        print("6. View the DAG in the Airflow UI: http://localhost:8080")
    else:
        print("\nSetup completed with errors. Please check the error messages above.")

if __name__ == "__main__":
    main() 