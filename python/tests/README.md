# Snapbase Python Tests

This directory contains comprehensive tests for the snapbase Python bindings.

## Test Structure

- **`test_basic.py`** - Basic functionality tests (module imports, version, hello function, workspace basics)
- **`test_workspace.py`** - Workspace operations and error handling tests
- **`test_integration.py`** - Integration tests with sample data and end-to-end workflows
- **`test_main.py`** - Main function, module structure, and compatibility tests
- **`conftest.py`** - Test configuration and fixtures for sample data

## Running Tests

### Quick Run
```bash
uv run pytest
```

### Verbose Output
```bash
uv run pytest -v
```

### Run Specific Test File
```bash
uv run pytest tests/test_basic.py
```

### Run Specific Test Class
```bash
uv run pytest tests/test_basic.py::TestBasicFunctionality
```

### Run Specific Test
```bash
uv run pytest tests/test_basic.py::TestBasicFunctionality::test_hello_from_bin
```

## Test Coverage

The test suite covers:

- ✅ **Module imports and structure** (11 tests)
- ✅ **Basic workspace operations** (8 tests)
- ✅ **Integration workflows** (8 tests)
- ✅ **Error handling** (8 tests)
- ✅ **Cross-platform compatibility** (6 tests)

**Total: 41 tests** - All passing ✅

## Test Features

- **Temporary workspaces** - Each test uses isolated temporary directories
- **Sample data generation** - CSV, JSON, and special character test data
- **Error resilience** - Tests handle missing files, invalid paths, and edge cases
- **Cross-platform** - Tests work on Windows, macOS, and Linux
- **Integration scenarios** - End-to-end workflows with multiple file formats

## Current Status

All tests are currently **placeholder tests** that verify the Python bindings work correctly with the current implementation. As the core functionality is expanded, these tests can be enhanced to verify actual data processing, snapshot creation, and change detection.

## Adding New Tests

1. Add test functions to existing test files for related functionality
2. Create new test files for major new features
3. Update fixtures in `conftest.py` for new test data needs
4. Follow the existing naming conventions (`test_*` functions, `Test*` classes)
5. Use the provided fixtures for temporary workspaces and sample data