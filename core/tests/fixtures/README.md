# Test Fixtures

This directory contains test fixtures for snapbase testing.

## Structure

- `configs/` - Configuration files for different test scenarios
- `data/` - Sample data files in various formats
- `workspaces/` - Pre-configured workspace directories (created during tests)

## Data Files

- `simple.csv` - Basic 3-column CSV for simple tests
- `employees.csv` - Employee data with multiple columns and types
- `employees_updated.csv` - Updated version of employees.csv for change detection tests
- `products.json` - Product data in JSON format
- `sales.csv` - Sales transaction data for relational testing

## Config Files

- `local.toml` - Standard local storage configuration
- `local_custom.toml` - Local storage with custom naming pattern

## Usage in Tests

```rust
use std::path::Path;

// Get fixture path
let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
    .join("tests/fixtures");

// Load test data
let data_file = fixture_path.join("data/simple.csv");

// Load test config
let config_file = fixture_path.join("configs/local.toml");
```