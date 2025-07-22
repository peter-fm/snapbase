"""
Basic tests for snapbase Python bindings
"""

import pytest
import snapbase
from snapbase import Workspace


class TestBasicFunctionality:
    """Test basic module functionality"""
    
    def test_module_imports(self):
        """Test that the module can be imported"""
        assert hasattr(snapbase, 'Workspace')
        assert hasattr(snapbase, '__version__')
    
    def test_version_string(self):
        """Test that version string is defined"""
        assert isinstance(snapbase.__version__, str)
        assert len(snapbase.__version__) > 0
    
    def test_all_exports(self):
        """Test that __all__ is properly defined"""
        assert hasattr(snapbase, '__all__')
        assert isinstance(snapbase.__all__, list)
        assert 'Workspace' in snapbase.__all__


class TestWorkspaceBasics:
    """Test basic Workspace functionality"""
    
    def test_workspace_creation(self, temp_workspace):
        """Test creating a workspace"""
        workspace = Workspace(str(temp_workspace))
        assert isinstance(workspace, Workspace)
    
    def test_workspace_path(self, temp_workspace):
        """Test getting workspace path"""
        workspace = Workspace(str(temp_workspace))
        path = workspace.get_path()
        assert isinstance(path, str)
        assert str(temp_workspace) in path
    
    def test_workspace_init(self, temp_workspace):
        """Test workspace initialization"""
        workspace = Workspace(str(temp_workspace))
        # This should not raise an exception
        workspace.init()
    
    def test_workspace_invalid_path(self):
        """Test workspace with invalid path"""
        # This should work - workspace should be created
        workspace = Workspace("/tmp/nonexistent/path/that/should/be/created")
        assert isinstance(workspace, Workspace)
    
    def test_list_snapshots_empty(self, temp_workspace):
        """Test listing snapshots on empty workspace"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        snapshots = workspace.list_snapshots()
        assert isinstance(snapshots, list)
        # Should be empty for new workspace
        assert len(snapshots) == 0
    
    def test_snapshot_exists_false(self, temp_workspace):
        """Test checking for non-existent snapshot"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        exists = workspace.snapshot_exists("nonexistent")
        assert isinstance(exists, bool)
        assert not exists
    
    def test_list_snapshots_for_source(self, temp_workspace):
        """Test listing snapshots for a specific source"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        snapshots = workspace.list_snapshots_for_source("test.csv")
        assert isinstance(snapshots, list)
        # Should be empty for new workspace
        assert len(snapshots) == 0