# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Snapbase is a snapshot-based structured data diff tool written in Rust that detects schema, column-level, and row-level changes between versions of structured datasets. It supports multiple storage backends (local and S3) and various data formats (CSV, JSON, Parquet, SQL).

## Build, Test, and Development Commands

### Building
```bash
# Build the project
cargo build

# Build with release optimizations
cargo build --release

# Build with bundled DuckDB (if needed)
cargo build --features bundled
```

### Testing
```bash
# Run all tests (48 tests total)
cargo test

# Run unit tests only (38 tests in src/)
cargo test --lib

# Run integration tests (5 tests)
cargo test --test integration_test

# Run full workflow tests (5 tests with actual snapshot creation)
cargo test --test full_workflow_test

# Run specific test modules
cargo test commands::tests
cargo test hash::tests
cargo test data::tests

# Run tests with verbose output
cargo test -- --nocapture

# Run tests sequentially (useful for debugging)
cargo test -- --test-threads=1

# Test specific functionality
cargo test snapshot
cargo test workspace
cargo test change_detection
```

### Development
```bash
# Run the CLI tool
cargo run -- [subcommand] [args]

# Example: Initialize workspace
cargo run -- init

# Example: Create snapshot (using sample data)
cargo run -- snapshot test_data/test_data.csv --name initial

# Example: Using test fixtures for development
cargo run -- snapshot tests/fixtures/data/employees.csv --name employees_v1

# Run with verbose logging
cargo run -- --verbose [subcommand] [args]
```

### Java Development
```bash
# Build Java API with native bindings
./build-java.sh

# Build components separately
cd java-bindings && cargo build --release --features jni
cd ../java && mvn clean package

# Run Java tests
cd java && mvn test
```

## Architecture

### Core Components

**Data Processing Pipeline:**
- `src/data.rs` - Core data processing using DuckDB for handling CSV, JSON, Parquet, and SQL files
- `src/query_engine.rs` - DuckDB query execution engine with cloud storage support
- `src/sql.rs` - SQL file parsing and database connection management

**Storage Layer:**
- `src/storage/mod.rs` - Storage backend abstraction
- `src/storage/local.rs` - Local filesystem storage implementation
- `src/storage/s3.rs` - S3 cloud storage implementation
- Uses Hive-style partitioning: `sources/{filename}/snapshot_name={name}/snapshot_timestamp={timestamp}/`

**Snapshot Management:**
- `src/snapshot.rs` - Snapshot metadata and chain management
- `src/resolver.rs` - Snapshot resolution and reference handling
- `src/workspace.rs` - Workspace initialization and configuration

**Change Detection:**
- `src/change_detection.rs` - Comprehensive change detection between snapshots
- `src/hash.rs` - Data hashing and fingerprinting for efficient comparisons

**CLI Interface:**
- `src/cli.rs` - Command-line argument parsing using clap
- `src/commands.rs` - Command implementations and business logic
- `src/output.rs` - Output formatting (pretty printing and JSON)

### Key Features

1. **Snapshot-based tracking** - Creates immutable snapshots of data with metadata
2. **Hive-style storage** - Organizes data in partitioned directory structure
3. **Multiple storage backends** - Local filesystem and S3 support
4. **Change detection** - Detects schema changes, row additions/deletions, and cell-level modifications
5. **Rollback capability** - Can restore files to previous snapshot states (full data mode only)
6. **SQL querying** - Query historical snapshots using SQL
7. **Compression and archiving** - Efficient storage with cleanup capabilities

### Data Flow

1. **Input Processing** - Files are processed through DuckDB to extract schema and data
2. **Snapshot Creation** - Data is stored as Parquet files with JSON metadata in Hive structure
3. **Change Detection** - Compares current data against baseline snapshots
4. **Storage** - Writes to configured backend (local or S3) with proper partitioning

### Configuration

- Storage backend configured via `config` command or environment variables
- Workspace initialization creates `.snapbase/` directory (local) or S3 prefix structure
- Supports `.env` files for environment variable configuration
- DuckDB configuration handled in `src/duckdb_config.rs`

## Important Implementation Details

### Storage Architecture
- Modern Hive-style partitioning replaces legacy archive system
- Metadata stored as JSON, data as Parquet files
- Cloud storage support through async storage backend abstraction

### Performance Optimizations
- Streaming data processing for large files
- Direct DuckDB COPY operations for maximum performance

### Testing Strategy
- **48 total tests** with 100% pass rate across 4 test suites
- **Unit tests (38)**: Core functionality testing in `src/` modules
- **Integration tests (5)**: End-to-end workflow testing in `tests/integration_test.rs`
- **Full workflow tests (5)**: Complete snapshot lifecycle testing in `tests/full_workflow_test.rs`
- **Test infrastructure (2)**: Test utilities validation in `tests/common/mod.rs`
- **Committed test fixtures**: Sample data files and configurations in `tests/fixtures/`
- **Safe test environment**: Uses `TestWorkspace` with isolated temporary directories
- **Comprehensive coverage**: CLI commands, data processing, change detection, storage backends

### Test Infrastructure
- **Test fixtures** (`tests/fixtures/`): Committed sample data and configurations for automated testing
  - `data/`: CSV, JSON sample files (employees, products, sales, simple datasets)
  - `configs/`: Safe test configurations using isolated workspace paths
  - `README.md`: Documentation for test fixtures usage
- **Test utilities** (`tests/common/mod.rs`): 
  - `TestWorkspace`: Manages temporary workspaces with automatic cleanup
  - `TestFixtures`: Provides access to committed test data
  - `WorkspaceGuard`: RAII directory management for safe test execution
- **Sample data** (`test_data/`): Example files for manual testing and development
- **Test patterns**:
  - Unit tests: Test individual functions and modules in isolation
  - Integration tests: Test CLI commands and cross-module functionality
  - Workflow tests: Test complete user scenarios with real data processing

### Error Handling
- Custom error types in `src/error.rs`
- Comprehensive error messages with context
- Input validation and workspace boundary checks

## Test Execution Notes

- Use `uv run test` for tests