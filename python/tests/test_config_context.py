"""
Tests to reproduce and verify the config context issue.

The issue: When creating a workspace with an explicit path like Workspace("mydirectory/project1"),
the workspace should use the config file from that directory, not the current directory or global config.
"""

import pytest
import os
import tempfile
import json
from pathlib import Path
from snapbase import Workspace


class TestConfigContext:
    """Test configuration context resolution across different scenarios"""
    
    def test_workspace_uses_own_config_not_current_directory(self):
        """
        CORE BUG TEST: Workspace should use its own config, not current directory config.
        
        This test reproduces the exact issue described:
        - Create workspace in subdirectory with specific config
        - Run Python script from parent directory  
        - Workspace should use subdirectory config, not parent directory config
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup: Create parent directory with one config
            parent_config = {
                "storage": {
                    "backend": "local",
                    "path": "parent_snapbase"
                }
            }
            parent_config_file = Path(temp_dir) / "snapbase.toml"
            with open(parent_config_file, 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "parent_snapbase"\n')
            
            # Setup: Create subdirectory with different config
            subdir = Path(temp_dir) / "project1" 
            subdir.mkdir()
            child_config_file = subdir / "snapbase.toml"
            with open(child_config_file, 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "child_snapbase"\n')
            
            # Change to parent directory (simulating running Python script from parent)
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                # BUG TEST: Create workspace pointing to subdirectory
                workspace = Workspace(str(subdir))
                config_info = json.loads(workspace.get_config_info())
                
                # CRITICAL ASSERTIONS: Should use child config, not parent config
                assert config_info["config_source"] == "workspace", \
                    f"Expected workspace config, got {config_info['config_source']}"
                assert str(subdir) in config_info["config_path"], \
                    f"Config path should contain subdirectory path. Got: {config_info['config_path']}"
                assert config_info["config_path"].endswith("project1/snapbase.toml"), \
                    f"Should use child config file. Got: {config_info['config_path']}"
                
                # Additional verification: workspace path should be the subdirectory
                assert config_info["workspace_path"] == str(subdir), \
                    f"Workspace path should be subdirectory. Got: {config_info['workspace_path']}"
                    
            finally:
                os.chdir(original_cwd)
    
    def test_workspace_config_isolation(self):
        """Test that multiple workspaces with different configs are isolated"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create two separate workspace directories with different configs
            ws1_dir = Path(temp_dir) / "workspace1"
            ws2_dir = Path(temp_dir) / "workspace2"
            ws1_dir.mkdir()
            ws2_dir.mkdir()
            
            # Workspace 1: Local storage with custom path
            with open(ws1_dir / "snapbase.toml", 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "custom1"\n')
            
            # Workspace 2: Local storage with different path  
            with open(ws2_dir / "snapbase.toml", 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "custom2"\n')
            
            # Create workspaces from a neutral directory
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                ws1 = Workspace(str(ws1_dir))
                ws2 = Workspace(str(ws2_dir))
                
                # Each workspace should use its own config
                config1 = json.loads(ws1.get_config_info())
                config2 = json.loads(ws2.get_config_info())
                
                assert config1["config_path"] != config2["config_path"], \
                    "Workspaces should use different config files"
                assert "workspace1" in config1["config_path"], \
                    f"WS1 should use workspace1 config. Got: {config1['config_path']}"
                assert "workspace2" in config2["config_path"], \
                    f"WS2 should use workspace2 config. Got: {config2['config_path']}"
                    
            finally:
                os.chdir(original_cwd)
    
    def test_workspace_config_vs_global_priority(self):
        """Test that workspace config takes priority over global config"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create workspace with specific config
            workspace_dir = Path(temp_dir) / "myworkspace"
            workspace_dir.mkdir()
            
            workspace_config = workspace_dir / "snapbase.toml"
            with open(workspace_config, 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "workspace_storage"\n')
            
            # Create workspace from different directory
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                workspace = Workspace(str(workspace_dir))
                config_info = json.loads(workspace.get_config_info())
                
                # Should use workspace config, not global
                assert config_info["config_source"] == "workspace", \
                    f"Should prioritize workspace config. Got: {config_info['config_source']}"
                assert config_info["config_path"] == str(workspace_config), \
                    f"Should use workspace config file. Got: {config_info['config_path']}"
                    
            finally:
                os.chdir(original_cwd)
    
    def test_relative_vs_absolute_workspace_paths(self):
        """Test config resolution with relative vs absolute workspace paths"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create workspace subdirectory
            subdir = Path(temp_dir) / "relative_test"
            subdir.mkdir()
            
            config_file = subdir / "snapbase.toml"
            with open(config_file, 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "relative_storage"\n')
            
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                # Test relative path
                workspace_rel = Workspace("relative_test")
                config_rel = json.loads(workspace_rel.get_config_info())
                
                # Test absolute path
                workspace_abs = Workspace(str(subdir))
                config_abs = json.loads(workspace_abs.get_config_info())
                
                # Both should resolve to the same config file
                assert config_rel["config_source"] == "workspace"
                assert config_abs["config_source"] == "workspace"
                
                # Both should point to the same config file (after normalization)
                assert Path(config_rel["config_path"]).resolve() == Path(config_abs["config_path"]).resolve()
                
            finally:
                os.chdir(original_cwd)

    def test_workspace_operations_use_workspace_config(self):
        """
        INTEGRATION TEST: Verify that workspace operations actually use workspace-specific config.
        
        This tests that not just config resolution works, but actual operations like 
        create_snapshot, status, etc. use the workspace config context.
        """  
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_dir = Path(temp_dir) / "operation_test"
            workspace_dir.mkdir()
            
            # Create workspace config with specific storage path
            config_file = workspace_dir / "snapbase.toml"
            with open(config_file, 'w') as f:
                f.write('[storage]\nbackend = "local"\npath = "operation_storage"\n')
            
            # Create test data file in workspace
            test_data = workspace_dir / "test.csv"
            with open(test_data, 'w') as f:
                f.write("id,name,value\n1,test,100\n2,demo,200\n")
            
            original_cwd = os.getcwd()
            try:
                # Run from different directory to test context
                os.chdir(temp_dir)
                
                workspace = Workspace(str(workspace_dir))
                
                # Verify config is correct
                config_info = json.loads(workspace.get_config_info())
                assert config_info["config_source"] == "workspace"
                assert str(workspace_dir) in config_info["config_path"]
                
                # Initialize workspace
                workspace.init()
                
                # Test that operations work with workspace context
                # This would fail if operations don't use workspace-specific config
                result = workspace.create_snapshot(str(test_data), "test_snapshot")
                assert "test_snapshot" in result
                
                # Verify snapshot was created using workspace config context
                assert workspace.snapshot_exists("test_snapshot")
                
                # Verify workspace reports correct path
                workspace_path = workspace.get_path()
                assert str(workspace_dir) in workspace_path
                
            finally:
                os.chdir(original_cwd)
