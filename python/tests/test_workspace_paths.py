"""
Tests for workspace path resolution - the fundamental functionality that was broken
"""

import pytest
import os
import tempfile
import shutil
from pathlib import Path
from snapbase import Workspace


def normalize_path(path):
    """Normalize paths for comparison (handles symlinks like /var -> /private/var)"""
    return os.path.realpath(path)


class TestWorkspacePaths:
    """Test that workspace paths work correctly in real-world scenarios"""
    
    def test_workspace_with_subdirectory_name(self):
        """Test creating workspace with a subdirectory name"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Change to temp directory
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                # Create workspace with subdirectory name
                workspace = Workspace("myproject")
                expected_path = os.path.realpath(os.path.join(temp_dir, "myproject"))
                actual_path = os.path.realpath(workspace.get_path())
                
                assert actual_path == expected_path
                
                # Initialize should create directory structure
                workspace.init()
                assert os.path.exists(workspace.get_path())
                assert os.path.exists(os.path.join(workspace.get_path(), ".snapbase"))
                
            finally:
                os.chdir(original_cwd)
    
    def test_workspace_default_current_directory(self):
        """Test creating workspace in current directory (no args)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                # Create workspace in current directory
                workspace = Workspace()
                assert normalize_path(workspace.get_path()) == normalize_path(temp_dir)
                
                # Initialize should create .snapbase in current dir
                workspace.init()
                assert os.path.exists(os.path.join(temp_dir, ".snapbase"))
                
            finally:
                os.chdir(original_cwd)
    
    def test_workspace_with_empty_string(self):
        """Test creating workspace with empty string defaults to current directory"""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                workspace = Workspace("")
                assert normalize_path(workspace.get_path()) == normalize_path(temp_dir)
                
            finally:
                os.chdir(original_cwd)
    
    def test_workspace_with_relative_path(self):
        """Test creating workspace with relative path like './subdir'"""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                workspace = Workspace("./subproject")
                expected_path = normalize_path(os.path.join(temp_dir, "subproject"))
                assert normalize_path(workspace.get_path()) == expected_path
                
            finally:
                os.chdir(original_cwd)
    
    def test_workspace_with_absolute_path(self):
        """Test creating workspace with absolute path"""
        with tempfile.TemporaryDirectory() as temp_dir:
            project_path = os.path.join(temp_dir, "absolute_project")
            
            workspace = Workspace(project_path)
            assert normalize_path(workspace.get_path()) == normalize_path(project_path)
    
    def test_workspace_ignores_existing_parent_workspace(self):
        """Test that workspace creation doesn't get confused by parent .snapbase dirs"""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                # Create a workspace in the parent directory
                parent_workspace = Workspace()
                parent_workspace.init()
                assert os.path.exists(os.path.join(temp_dir, ".snapbase"))
                
                # Now create a workspace in a subdirectory - should NOT use parent
                child_dir = "child_project"
                child_workspace = Workspace(child_dir)
                expected_child_path = normalize_path(os.path.join(temp_dir, child_dir))
                
                # This is the key test - should be child path, not parent path
                assert normalize_path(child_workspace.get_path()) == expected_child_path
                assert normalize_path(child_workspace.get_path()) != normalize_path(temp_dir)
                
                # Initialize child workspace
                child_workspace.init()
                assert os.path.exists(os.path.join(child_workspace.get_path(), ".snapbase"))
                
            finally:
                os.chdir(original_cwd)
    
    def test_multiple_workspaces_in_subdirectories(self):
        """Test creating multiple workspaces in different subdirectories"""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                # Create workspaces in different subdirs
                ws1 = Workspace("project1")
                ws2 = Workspace("project2")
                ws3 = Workspace("nested/project3")
                
                # Verify paths are correct
                assert normalize_path(ws1.get_path()) == normalize_path(os.path.join(temp_dir, "project1"))
                assert normalize_path(ws2.get_path()) == normalize_path(os.path.join(temp_dir, "project2"))
                assert normalize_path(ws3.get_path()) == normalize_path(os.path.join(temp_dir, "nested/project3"))
                
                # All should be different
                paths = [ws1.get_path(), ws2.get_path(), ws3.get_path()]
                assert len(set(paths)) == 3  # All unique
                
            finally:
                os.chdir(original_cwd)


class TestWorkspacePathBehaviorMismatch:
    """Tests that verify our fixes work and prevent regression"""
    
    def test_bug_regression_explicit_path_not_traversing_up(self):
        """Regression test: explicit paths should not traverse up to find existing workspaces"""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                # Create existing workspace in current directory
                existing_ws = Workspace()
                existing_ws.init()
                existing_path = existing_ws.get_path()
                
                # Create workspace with explicit subdirectory - should NOT find the existing one
                new_ws = Workspace("subproject")
                new_path = new_ws.get_path()
                
                # Key assertion: new workspace should be in subdirectory, not existing location
                assert normalize_path(new_path) != normalize_path(existing_path)
                assert normalize_path(new_path) == normalize_path(os.path.join(temp_dir, "subproject"))
                assert normalize_path(existing_path) == normalize_path(temp_dir)
                
            finally:
                os.chdir(original_cwd)
                
    def test_bug_regression_basic_functionality_works(self):
        """Test the basic functionality that was completely broken"""
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                
                # This is the exact use case that was failing
                workspace = Workspace("myworkspace")
                
                # Should NOT return empty string or current directory
                path = workspace.get_path()
                assert path != ""  # Was returning empty string
                assert normalize_path(path) != normalize_path(temp_dir)  # Was returning current directory instead
                assert normalize_path(path) == normalize_path(os.path.join(temp_dir, "myworkspace"))  # Should be subdirectory
                
            finally:
                os.chdir(original_cwd)