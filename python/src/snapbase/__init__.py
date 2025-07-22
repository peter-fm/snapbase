"""
Snapbase: A snapshot-based structured data diff tool

This package provides Python bindings for the snapbase Rust library,
allowing you to create snapshots of structured data and detect changes
between versions.
"""

from snapbase._core import Workspace

__version__ = "0.1.0"
__all__ = ["Workspace"]


def test_runner() -> None:
    """Build and run tests"""
    import subprocess
    import sys
    import os
    from pathlib import Path
    
    # Get the project root (where pyproject.toml is)
    project_root = Path(__file__).parent.parent.parent
    os.chdir(project_root)
    
    print("🔨 Building snapbase module...")
    try:
        subprocess.run([
            sys.executable, "-m", "maturin", "develop"
        ], check=True, capture_output=False)
        print("✅ Module built successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to build module: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("❌ maturin not found. Installing...")
        try:
            subprocess.run([
                sys.executable, "-m", "pip", "install", "maturin"
            ], check=True)
            subprocess.run([
                sys.executable, "-m", "maturin", "develop"
            ], check=True, capture_output=False)
            print("✅ Module built successfully")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install/build: {e}")
            sys.exit(1)
    
    print("\n🧪 Running tests...")
    try:
        result = subprocess.run([
            sys.executable, "-m", "pytest", "-v"
        ], capture_output=False)
        if result.returncode == 0:
            print("✅ All tests passed!")
        else:
            print(f"❌ Tests failed with return code: {result.returncode}")
        sys.exit(result.returncode)
    except FileNotFoundError:
        print("❌ pytest not found. Please install pytest.")
        sys.exit(1)
