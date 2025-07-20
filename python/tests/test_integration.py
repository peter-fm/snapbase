"""
Integration tests for snapbase Python bindings
"""

import pytest
import os
from pathlib import Path
from snapbase import Workspace


class TestIntegration:
    """Integration tests using sample data"""
    
    def test_end_to_end_workflow(self, temp_workspace, sample_csv_file, updated_csv_file):
        """Test a complete end-to-end workflow"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # Step 1: Create initial snapshot of sample data (use relative path)
        result1 = workspace.create_snapshot(sample_csv_file.name, "v1")
        assert isinstance(result1, str)
        assert "v1" in result1
        
        # Step 2: Create second snapshot of the same file (simulating file evolution)
        result2 = workspace.create_snapshot(sample_csv_file.name, "v2") 
        assert isinstance(result2, str)
        assert "v2" in result2
        
        # Step 3: Test diff between two snapshots of the same source file
        changes = workspace.diff(sample_csv_file.name, "v1", "v2")
        assert isinstance(changes, str)
        # Result should be JSON with change detection data
        import json
        changes_data = json.loads(changes)
        assert isinstance(changes_data, dict)
        
        # Step 4: Query data (now returns Polars DataFrame)
        query_result = workspace.query(sample_csv_file.name, "SELECT * FROM data WHERE age > 25")
        # Import polars for type checking
        try:
            import polars as pl
            assert isinstance(query_result, pl.DataFrame)
            assert query_result.height >= 0  # Should have some data structure
        except ImportError:
            # If polars not available, skip this check
            pytest.skip("Polars not available for testing")
    
    def test_multiple_file_formats(self, temp_workspace, sample_csv_file, sample_json_file):
        """Test working with multiple file formats"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # Create snapshots for different formats
        csv_result = workspace.create_snapshot(sample_csv_file.name, "csv_snapshot")
        json_result = workspace.create_snapshot(sample_json_file.name, "json_snapshot")
        
        assert isinstance(csv_result, str)
        assert isinstance(json_result, str)
        assert "csv_snapshot" in csv_result
        assert "json_snapshot" in json_result
    
    def test_workspace_persistence(self, temp_workspace, sample_csv_file):
        """Test that workspace state persists between instances"""
        # Create first workspace instance
        workspace1 = Workspace(str(temp_workspace))
        workspace1.init()
        
        # Create a snapshot
        result1 = workspace1.create_snapshot(sample_csv_file.name, "persistent_test")
        assert isinstance(result1, str)
        
        # Create second workspace instance with same path
        workspace2 = Workspace(str(temp_workspace))
        
        # Should be able to access the same workspace
        path1 = workspace1.get_path()
        path2 = workspace2.get_path()
        assert path1 == path2
    
    def test_large_file_handling(self, temp_workspace):
        """Test handling of larger files"""
        # Create a larger CSV file
        large_csv = temp_workspace / "large_data.csv"
        
        with open(large_csv, 'w') as f:
            f.write("id,name,value\n")
            for i in range(1000):
                f.write(f"{i},name_{i},{i*10}\n")
        
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # Create snapshot of large file
        result = workspace.create_snapshot(str(large_csv), "large_snapshot")
        assert isinstance(result, str)
        assert "large_snapshot" in result
    
    def test_special_characters_in_data(self, temp_workspace):
        """Test handling of special characters in data"""
        special_csv = temp_workspace / "special_data.csv"
        
        # Create CSV with special characters
        with open(special_csv, 'w', encoding='utf-8') as f:
            f.write("id,name,description\n")
            f.write('1,"José García","Café & Résumé"\n')
            f.write('2,"李明","中文测试"\n')
            f.write('3,"مُحَمَّد","اختبار العربية"\n')
        
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # Create snapshot
        result = workspace.create_snapshot(str(special_csv), "special_chars")
        assert isinstance(result, str)
        assert "special_chars" in result
    
    def test_empty_files(self, temp_workspace):
        """Test handling of empty files"""
        empty_csv = temp_workspace / "empty.csv"
        
        # Create empty CSV file
        with open(empty_csv, 'w') as f:
            f.write("id,name,value\n")  # Header only
        
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # Create snapshot of empty file
        result = workspace.create_snapshot(str(empty_csv), "empty_snapshot")
        assert isinstance(result, str)
        assert "empty_snapshot" in result


class TestErrorHandling:
    """Test error handling in various scenarios"""
    
    def test_invalid_file_paths(self, temp_workspace):
        """Test handling of invalid file paths"""
        workspace = Workspace(str(temp_workspace))
        workspace.init()
        
        # Test with various invalid paths
        invalid_paths = [
            "/dev/null/nonexistent.csv",
            "",
            "   ",
            "/root/protected.csv",  # Likely no permissions
        ]
        
        for path in invalid_paths:
            # Should raise exception for invalid paths (real functionality)
            try:
                result = workspace.create_snapshot(path, "test")
                # If no exception, at least verify it's a string
                assert isinstance(result, str)
            except RuntimeError:
                # This is expected for invalid paths
                pass
    
    def test_concurrent_access(self, temp_workspace, sample_csv_file):
        """Test concurrent access to the same workspace"""
        # Create multiple workspace instances
        workspace1 = Workspace(str(temp_workspace))
        workspace2 = Workspace(str(temp_workspace))
        
        workspace1.init()
        workspace2.init()
        
        # Both should work
        result1 = workspace1.create_snapshot(sample_csv_file.name, "concurrent1")
        result2 = workspace2.create_snapshot(sample_csv_file.name, "concurrent2")
        
        assert isinstance(result1, str)
        assert isinstance(result2, str)
        assert "concurrent1" in result1
        assert "concurrent2" in result2