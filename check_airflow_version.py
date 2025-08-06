#!/usr/bin/env python3
"""
Check Airflow version and determine correct syntax.
"""

import subprocess
import sys

def check_airflow_version():
    """Check Airflow version."""
    try:
        result = subprocess.run(['airflow', 'version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            version = result.stdout.strip()
            print(f"Airflow version: {version}")
            
            # Parse version
            if version.startswith('2.'):
                print("✅ Using Airflow 2.x syntax")
                return "2.x"
            elif version.startswith('1.'):
                print("⚠️  Using Airflow 1.x syntax")
                return "1.x"
            else:
                print("❓ Unknown Airflow version")
                return "unknown"
        else:
            print(f"❌ Airflow command failed: {result.stderr}")
            return None
    except Exception as e:
        print(f"❌ Airflow not found or not accessible: {str(e)}")
        return None

def test_airflow_imports():
    """Test Airflow imports."""
    try:
        from airflow import DAG
        from airflow.operators.python import PythonOperator
        print("✅ Airflow imports successful")
        return True
    except Exception as e:
        print(f"❌ Airflow imports failed: {str(e)}")
        return False

def main():
    """Main function."""
    print("Airflow Version Check")
    print("=" * 30)
    
    # Check version
    version = check_airflow_version()
    
    # Test imports
    test_airflow_imports()
    
    if version == "2.x":
        print("\nFor Airflow 2.x, use:")
        print("- schedule instead of schedule_interval")
        print("- with dag: context manager for tasks")
        print("- No dag parameter in PythonOperator")
    elif version == "1.x":
        print("\nFor Airflow 1.x, use:")
        print("- schedule_interval parameter")
        print("- dag parameter in PythonOperator")
        print("- Direct task dependency setup")

if __name__ == "__main__":
    main() 