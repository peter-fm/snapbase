"""
Test configuration and fixtures for snapbase tests
"""

import pytest
import tempfile
import shutil
import os
from pathlib import Path
import csv
import json


@pytest.fixture
def temp_workspace():
    """Create a temporary workspace for testing"""
    temp_dir = tempfile.mkdtemp(prefix="snapbase_test_")
    temp_path = Path(temp_dir)
    
    # Create an isolated workspace configuration
    config_content = """[storage]
backend = "local"

[storage.local]
path = ".snapbase"

[snapshot]
default_name_pattern = "{source}_{format}_{seq}"

[databases]
"""
    config_file = temp_path / "snapbase.toml"
    with open(config_file, 'w') as f:
        f.write(config_content)
    
    # Store original working directory and change to temp workspace
    original_cwd = os.getcwd()
    os.chdir(temp_path)
    
    yield temp_path
    
    # Cleanup - change back to original directory first
    os.chdir(original_cwd)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_csv_file(temp_workspace):
    """Create a sample CSV file for testing"""
    csv_file = temp_workspace / "test_data.csv"
    
    # Create sample data
    data = [
        ["id", "name", "age", "city"],
        ["1", "Alice", "25", "New York"],
        ["2", "Bob", "30", "Los Angeles"],
        ["3", "Charlie", "35", "Chicago"]
    ]
    
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)
    
    return csv_file


@pytest.fixture
def sample_json_file(temp_workspace):
    """Create a sample JSON file for testing"""
    json_file = temp_workspace / "test_data.json"
    
    # Create sample data
    data = [
        {"id": 1, "name": "Alice", "age": 25, "city": "New York"},
        {"id": 2, "name": "Bob", "age": 30, "city": "Los Angeles"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"}
    ]
    
    with open(json_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    return json_file


@pytest.fixture
def updated_csv_file(temp_workspace):
    """Create an updated CSV file for testing change detection"""
    csv_file = temp_workspace / "test_data_updated.csv"
    
    # Create updated data (Alice's age changed, David added, Charlie removed)
    data = [
        ["id", "name", "age", "city"],
        ["1", "Alice", "26", "New York"],  # Age changed
        ["2", "Bob", "30", "Los Angeles"],  # Unchanged
        ["4", "David", "40", "Seattle"]     # New person
    ]
    
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)
    
    return csv_file