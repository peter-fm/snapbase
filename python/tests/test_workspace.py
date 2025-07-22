"""
Tests for Workspace functionality
"""

import pytest
from snapbase import Workspace


class TestWorkspaceOperations:
    """Test workspace operations"""
    
    def test_create_snapshot_real(self, temp_workspace, sample_csv_file):
        """Test creating a snapshot (real functionality)"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # Test with custom name
        result = workspace.create_snapshot(sample_csv_file.name, "test_snapshot")
        assert isinstance(result, str)
        assert "test_snapshot" in result
        assert "rows" in result
        assert "columns" in result
        
        # Verify snapshot was created
        assert workspace.snapshot_exists("test_snapshot")
        
        # Test with default name
        result = workspace.create_snapshot(sample_csv_file.name)
        assert isinstance(result, str)
        assert "rows" in result
        assert "columns" in result
    
    def test_detect_changes_real(self, temp_workspace, sample_csv_file, updated_csv_file):
        """Test detecting changes (real functionality)"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # First create a baseline snapshot of the file we want to check later
        workspace.create_snapshot(updated_csv_file.name, "baseline")
        
        # Then detect changes by comparing current state against the baseline
        result = workspace.detect_changes(updated_csv_file.name, "baseline")
        assert isinstance(result, str)
        # Result should be JSON with change detection data
        import json
        changes = json.loads(result)
        assert isinstance(changes, dict)
    
    def test_query_real(self, temp_workspace, sample_csv_file):
        """Test SQL querying (real functionality)"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # Create a snapshot first
        workspace.create_snapshot(sample_csv_file.name, "test_data")
        
        # Query the snapshot data
        result = workspace.query(sample_csv_file.name, "SELECT COUNT(*) as row_count FROM data")
        
        # Query returns a Polars DataFrame, not a string
        try:
            import polars
            assert isinstance(result, polars.DataFrame)
            assert result.height > 0, "Query should return results"
            assert "row_count" in result.columns
            # Verify the count makes sense (should be 3 rows from sample data)
            count_value = result.select("row_count").item()
            assert count_value > 0, "Row count should be positive"
        except ImportError:
            # If polars not available, just check it's not a string
            assert not isinstance(result, str), "Query should not return string"
            assert result is not None, "Query should return something"
    
    def test_workspace_lifecycle(self, temp_workspace, sample_csv_file):
        """Test full workspace lifecycle"""
        workspace = Workspace(str(temp_workspace))
        
        # Initialize workspace
        workspace.init()
        
        # Check workspace path
        path = workspace.get_path()
        assert str(temp_workspace) in path
        
        # Check snapshot doesn't exist
        assert not workspace.snapshot_exists("test")
        
        # Create a snapshot
        result = workspace.create_snapshot(sample_csv_file.name, "test")
        assert isinstance(result, str)
        assert "test" in result
        
        # Verify snapshot exists
        assert workspace.snapshot_exists("test")
        
        # List snapshots (should include our snapshot)
        snapshots = workspace.list_snapshots()
        assert "test" in snapshots
        
        # Test stats functionality
        stats_result = workspace.stats()
        assert isinstance(stats_result, str)
        import json
        stats = json.loads(stats_result)
        assert isinstance(stats, dict)
        assert "snapshot_count" in stats
    
    def test_new_stats_method(self, temp_workspace, sample_csv_file):
        """Test the new stats() method"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # Get stats from empty workspace
        stats_result = workspace.stats()
        assert isinstance(stats_result, str)
        import json
        stats = json.loads(stats_result)
        assert "snapshot_count" in stats
        
        # Create some snapshots and check stats update
        workspace.create_snapshot(sample_csv_file.name, "snap1")
        workspace.create_snapshot(sample_csv_file.name, "snap2")
        
        stats_result = workspace.stats()
        stats = json.loads(stats_result)
        assert stats["snapshot_count"] >= 2
    
    def test_new_diff_method(self, temp_workspace, sample_csv_file, updated_csv_file):
        """Test the new diff() method"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # Create two snapshots of the same source file (simulating file evolution)
        workspace.create_snapshot(sample_csv_file.name, "version1")
        workspace.create_snapshot(sample_csv_file.name, "version2")
        
        # Compare the two snapshots from the same source
        diff_result = workspace.diff(sample_csv_file.name, "version1", "version2")
        assert isinstance(diff_result, str)
        import json
        diff_data = json.loads(diff_result)
        assert isinstance(diff_data, dict)


class TestWorkspaceErrors:
    """Test error handling in workspace operations"""
    
    def test_workspace_creation_edge_cases(self):
        """Test workspace creation with edge cases"""
        # Empty path
        workspace = Workspace("")
        assert isinstance(workspace, Workspace)
        
        # Relative path
        workspace = Workspace("./test_workspace")
        assert isinstance(workspace, Workspace)
        
        # Path with spaces
        workspace = Workspace("/tmp/test workspace")
        assert isinstance(workspace, Workspace)
    
    def test_operations_on_uninitialized_workspace(self, temp_workspace):
        """Test operations on workspace that hasn't been initialized"""
        workspace = Workspace(str(temp_workspace))
        
        # These should still work even without explicit init
        path = workspace.get_path()
        assert isinstance(path, str)
        
        snapshots = workspace.list_snapshots()
        assert isinstance(snapshots, list)
        
        exists = workspace.snapshot_exists("test")
        assert isinstance(exists, bool)
    
    def test_snapshot_operations_with_nonexistent_files(self, temp_workspace):
        """Test snapshot operations with files that don't exist"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # Create snapshot of non-existent file (should raise error)
        with pytest.raises(RuntimeError, match="File not found"):
            workspace.create_snapshot("/nonexistent/file.csv", "test")
        
        # Detect changes with non-existent baseline (should raise error)
        with pytest.raises(RuntimeError, match="Failed to resolve baseline snapshot"):
            workspace.detect_changes("/nonexistent/file.csv", "nonexistent_baseline")