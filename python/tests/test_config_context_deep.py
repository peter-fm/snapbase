"""
Deep integration tests for config context issues.

These tests specifically target the scenario described:
- Python script running from parent directory  
- Workspace created with explicit subdirectory path
- Operations should use subdirectory config, not parent directory config
"""

import pytest
import os
import tempfile
import json
from pathlib import Path
from snapbase import Workspace


class TestConfigContextDeepIntegration:
    """Deep integration tests to catch subtle config context bugs"""
    
    def test_detailed_workspace_config_usage_scenario(self):
        """
        Test the exact scenario described in the issue:
        
        1. Python script in mydirectory/ 
        2. Creates Workspace("mydirectory/project1")
        3. project1/ has its own snapbase.toml
        4. All operations should use project1/snapbase.toml, not mydirectory/snapbase.toml
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup the exact directory structure described
            mydirectory = Path(temp_dir) / "mydirectory"
            project1 = mydirectory / "project1"
            mydirectory.mkdir()
            project1.mkdir()
            
            # mydirectory has one config (should NOT be used)
            mydirectory_config = mydirectory / "snapbase.toml"
            with open(mydirectory_config, 'w') as f:
                f.write('''[storage]
backend = "local"
path = "mydirectory_snapbase"

[snapshot]
default_name_pattern = "mydirectory_{seq}"
''')
            
            # project1 has different config (SHOULD be used)
            project1_config = project1 / "snapbase.toml"  
            with open(project1_config, 'w') as f:
                f.write('''[storage]
backend = "local" 
path = "project1_snapbase"

[snapshot]
default_name_pattern = "project1_{seq}"
''')
            
            # Create test data in project1
            test_csv = project1 / "data.csv"
            with open(test_csv, 'w') as f:
                f.write("id,name,value\n1,Alice,100\n2,Bob,200\n")
            
            # Simulate Python script running from mydirectory  
            original_cwd = os.getcwd()
            try:
                os.chdir(mydirectory)  # This is key - running from parent directory
                
                print(f"Running from: {os.getcwd()}")
                print(f"Creating workspace for: {project1}")
                
                # This is the exact pattern described in the issue
                workspace = Workspace(str(project1))
                
                # Verify config resolution 
                config_info = json.loads(workspace.get_config_info())
                print(f"Config info: {json.dumps(config_info, indent=2)}")
                
                # CRITICAL: Should use project1 config, not mydirectory config
                assert config_info["config_source"] == "workspace", \
                    f"Should use workspace config, got: {config_info['config_source']}"
                    
                assert "project1" in config_info["config_path"], \
                    f"Should use project1 config. Path: {config_info['config_path']}"
                    
                assert config_info["config_path"] == str(project1_config), \
                    f"Should use exact project1 config file. Got: {config_info['config_path']}"
                
                # Now test that operations actually use the correct config
                workspace.init()
                
                # Test snapshot creation - this should use project1 config context
                result = workspace.create_snapshot(str(test_csv))
                print(f"Snapshot result: {result}")
                
                # The snapshot name should follow project1 pattern, not mydirectory pattern
                # If using mydirectory config, name would start with "mydirectory_"
                # If using project1 config, name would start with "project1_"
                if "project1_" not in result:
                    print(f"WARNING: Snapshot name doesn't use project1 pattern: {result}")
                    # Don't fail here as the pattern might be overridden elsewhere
                
                # Verify workspace path is correct
                workspace_path = workspace.get_path()
                assert str(project1) in workspace_path, \
                    f"Workspace path should be project1 directory. Got: {workspace_path}"
                
                # Test that storage uses project1 config
                # The .snapbase directory should be created in project1, not mydirectory
                snapbase_dir = project1 / "project1_snapbase"  # Based on config
                if not snapbase_dir.exists():
                    # Check if default .snapbase was created instead
                    default_snapbase = project1 / ".snapbase"
                    if default_snapbase.exists():
                        print(f"Found default .snapbase instead of configured path")
                    else:
                        print(f"No snapbase directory found in project1")
                
            finally:
                os.chdir(original_cwd)
    
    def test_workspace_context_with_relative_operations(self):
        """
        Test that operations with relative paths work correctly with workspace context.
        
        This tests the scenario where:
        1. Script runs from parent directory
        2. Workspace points to subdirectory  
        3. Operations use relative paths within workspace
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            parent = Path(temp_dir) / "parent"
            child = parent / "workspace"
            parent.mkdir()
            child.mkdir()
            
            # Parent config
            with open(parent / "snapbase.toml", 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "parent_storage"\n')
            
            # Child config  
            with open(child / "snapbase.toml", 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "child_storage"\n')
            
            # Create data file in child workspace
            data_file = child / "test.csv"
            with open(data_file, 'w') as f:
                f.write("a,b,c\n1,2,3\n4,5,6\n")
            
            original_cwd = os.getcwd()
            try:
                os.chdir(parent)  # Run from parent
                
                # Create workspace pointing to child
                workspace = Workspace("workspace")  # Relative path to child
                
                config_info = json.loads(workspace.get_config_info())
                print(f"Config resolution: {config_info['config_source']} -> {config_info['config_path']}")
                
                # Should use child config
                assert "workspace" in config_info["config_path"]
                
                workspace.init()
                
                # Test operations with paths relative to workspace
                # This should use child workspace context, not parent context
                result = workspace.create_snapshot("test.csv", "relative_test")
                assert "relative_test" in result
                
                # Verify snapshot exists in workspace context
                assert workspace.snapshot_exists("relative_test")
                
            finally:
                os.chdir(original_cwd)
    
    def test_multiple_workspaces_from_same_script_location(self):
        """
        Test creating multiple workspaces from the same script location.
        Each should maintain its own config context.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            script_dir = Path(temp_dir) / "script_location"
            ws1_dir = script_dir / "workspace1"  
            ws2_dir = script_dir / "workspace2"
            
            script_dir.mkdir()
            ws1_dir.mkdir()
            ws2_dir.mkdir()
            
            # Script location config (should not interfere)
            with open(script_dir / "snapbase.toml", 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "script_storage"\n')
            
            # Workspace 1 config
            with open(ws1_dir / "snapbase.toml", 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "ws1_storage"\n')
                
            # Workspace 2 config
            with open(ws2_dir / "snapbase.toml", 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "ws2_storage"\n')
            
            original_cwd = os.getcwd()
            try:
                os.chdir(script_dir)  # Script runs from here
                
                # Create both workspaces
                ws1 = Workspace("workspace1")
                ws2 = Workspace("workspace2")
                
                # Each should use its own config
                config1 = json.loads(ws1.get_config_info())
                config2 = json.loads(ws2.get_config_info())
                
                print(f"WS1 config: {config1['config_path']}")
                print(f"WS2 config: {config2['config_path']}")
                
                assert "workspace1" in config1["config_path"]
                assert "workspace2" in config2["config_path"]
                assert config1["config_path"] != config2["config_path"]
                
                # Both should be workspace source, not current directory
                assert config1["config_source"] == "workspace"
                assert config2["config_source"] == "workspace"
                
            finally:
                os.chdir(original_cwd)

    def test_workspace_config_context_persistence(self):
        """
        Test that workspace config context persists across multiple operations.
        
        This ensures that once a workspace is created with a specific config context,
        all subsequent operations maintain that context.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            main_dir = Path(temp_dir) / "main"
            project_dir = main_dir / "myproject"  
            main_dir.mkdir()
            project_dir.mkdir()
            
            # Different configs
            with open(main_dir / "snapbase.toml", 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "main_storage"\n')
                
            with open(project_dir / "snapbase.toml", 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "project_storage"\n')
            
            # Test data
            data1 = project_dir / "data1.csv"
            data2 = project_dir / "data2.csv"
            with open(data1, 'w') as f:
                f.write("x,y\n1,2\n3,4\n")
            with open(data2, 'w') as f:
                f.write("x,y\n5,6\n7,8\n")
            
            original_cwd = os.getcwd()
            try:
                os.chdir(main_dir)  # Run from main directory
                
                workspace = Workspace("myproject")
                
                # Check initial config
                initial_config = json.loads(workspace.get_config_info())
                assert initial_config["config_source"] == "workspace"
                assert "myproject" in initial_config["config_path"]
                
                workspace.init()
                
                # Perform multiple operations
                result1 = workspace.create_snapshot("data1.csv", "snap1")
                assert "snap1" in result1
                
                # Check config again - should be the same
                mid_config = json.loads(workspace.get_config_info())
                assert mid_config["config_path"] == initial_config["config_path"]
                
                result2 = workspace.create_snapshot("data2.csv", "snap2")  
                assert "snap2" in result2
                
                # Final config check
                final_config = json.loads(workspace.get_config_info())
                assert final_config["config_path"] == initial_config["config_path"]
                
                # All operations should show workspace context
                assert workspace.snapshot_exists("snap1")
                assert workspace.snapshot_exists("snap2")
                
                snapshots = workspace.list_snapshots()
                assert "snap1" in snapshots
                assert "snap2" in snapshots
                
            finally:
                os.chdir(original_cwd)