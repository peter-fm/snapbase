"""
Tests for the main module functionality
"""

import pytest
import sys
from unittest.mock import patch
from io import StringIO
import snapbase


class TestMainFunction:
    """Test the main function and CLI entry point"""
    
    def test_main_function_exists(self):
        """Test that main function exists"""
        assert hasattr(snapbase, 'main')
        assert callable(snapbase.main)
    
    def test_main_function_output(self):
        """Test that main function produces expected output"""
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            snapbase.main()
            output = mock_stdout.getvalue()
            assert "Hello from snapbase!" in output
    
    def test_main_function_no_args(self):
        """Test main function with no arguments"""
        # Should not raise an exception
        try:
            snapbase.main()
        except Exception as e:
            pytest.fail(f"main() raised an exception: {e}")
    
    def test_main_function_return_value(self):
        """Test main function return value"""
        result = snapbase.main()
        # Should return None (typical for main functions)
        assert result is None


class TestModuleStructure:
    """Test the overall module structure"""
    
    def test_module_attributes(self):
        """Test that required module attributes exist"""
        # Required attributes
        assert hasattr(snapbase, '__version__')
        assert hasattr(snapbase, '__all__')
        assert hasattr(snapbase, 'Workspace')
        assert hasattr(snapbase, 'hello_from_bin')
        assert hasattr(snapbase, 'main')
    
    def test_module_docstring(self):
        """Test that module has proper docstring"""
        assert snapbase.__doc__ is not None
        assert len(snapbase.__doc__.strip()) > 0
        assert "snapbase" in snapbase.__doc__.lower()
    
    def test_version_format(self):
        """Test that version follows expected format"""
        version = snapbase.__version__
        assert isinstance(version, str)
        # Should be in format like "0.1.0"
        parts = version.split('.')
        assert len(parts) >= 2  # At least major.minor
        for part in parts:
            assert part.isdigit()
    
    def test_all_exports_valid(self):
        """Test that all items in __all__ are actually exported"""
        for item in snapbase.__all__:
            assert hasattr(snapbase, item), f"Item '{item}' in __all__ but not exported"
    
    def test_no_extra_public_attributes(self):
        """Test that we don't have unexpected public attributes"""
        # Get all public attributes (not starting with _)
        public_attrs = [attr for attr in dir(snapbase) if not attr.startswith('_')]
        
        # Expected public attributes
        expected = set(snapbase.__all__ + ['main', 'test_runner'])
        actual = set(public_attrs)
        
        # Should not have extra public attributes
        extra = actual - expected
        # Filter out any attributes that might be added by the import system
        extra = {attr for attr in extra if not attr in ['annotations']}
        
        assert len(extra) == 0, f"Unexpected public attributes: {extra}"


class TestImportBehavior:
    """Test import behavior and module loading"""
    
    def test_import_speed(self):
        """Test that module imports reasonably quickly"""
        import time
        
        # Time a fresh import (remove from cache first)
        if 'snapbase' in sys.modules:
            del sys.modules['snapbase']
        
        start_time = time.time()
        import snapbase
        import_time = time.time() - start_time
        
        # Should import in less than 5 seconds (generous threshold)
        assert import_time < 5.0, f"Import took too long: {import_time:.2f}s"
    
    def test_submodule_import(self):
        """Test importing submodules"""
        # Test that we can import the core module
        from snapbase import _core
        assert _core is not None
        
        # Test that we can access the classes
        from snapbase._core import Workspace as CoreWorkspace
        assert CoreWorkspace is not None
    
    def test_import_error_handling(self):
        """Test that import errors are handled gracefully"""
        try:
            # Try to import something that doesn't exist
            from snapbase import nonexistent_module
            pytest.fail("Should have raised ImportError")
        except ImportError:
            # This is expected
            pass
        except Exception as e:
            pytest.fail(f"Unexpected exception type: {type(e).__name__}: {e}")


class TestCompatibility:
    """Test compatibility with different Python versions and environments"""
    
    def test_python_version_compatibility(self):
        """Test that we're running on a supported Python version"""
        # snapbase requires Python 3.9+
        assert sys.version_info >= (3, 9), f"Unsupported Python version: {sys.version}"
    
    def test_core_module_available(self):
        """Test that the core Rust module is available"""
        # This tests that the .so file was built correctly
        try:
            from snapbase import _core
            result = _core.hello_from_bin()
            assert result == "Hello from snapbase!"
        except ImportError as e:
            pytest.fail(f"Core module not available: {e}")
    
    def test_workspace_creation_cross_platform(self):
        """Test workspace creation works across platforms"""
        import tempfile
        import os
        
        # Test with different path styles
        if os.name == 'nt':  # Windows
            test_paths = [
                "C:\\temp\\test_workspace",
                "\\\\?\\C:\\temp\\test_workspace",  # UNC path
                "temp\\test_workspace",  # Relative path
            ]
        else:  # Unix-like
            test_paths = [
                "/tmp/test_workspace",
                "./test_workspace",
            ]
        
        for path in test_paths:
            try:
                workspace = snapbase.Workspace(str(path))
                assert isinstance(workspace, snapbase.Workspace)
            except Exception as e:
                pytest.fail(f"Failed to create workspace with path '{path}': {e}")