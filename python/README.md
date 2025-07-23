# Snapbase Python Library

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Python bindings for Snapbase - a queryable time machine for your structured data from entire databases and SQL queries to Excel, CSV, parquet and JSON files. Snapbase is data version control augmented SQL. Supports both local and cloud snapshot storage.

## Features


🚀 **Zero-Copy Arrow Performance**: Ultra-fast querying with Apache Arrow integration
✨ **Snapshot-based tracking** - Create immutable snapshots of your data with metadata  
🔍 **Comprehensive change detection** - Detect schema changes, row additions/deletions, and cell-level modifications  
📊 **Multiple format support** - Databases, SQL queries, Excel, CSV, JSON and Parquet files  
☁️ **Cloud storage support** - Store snapshots locally or in S3  
📈 **SQL querying** - Query across snapshots using SQL to monitor changes at the cell level over time.  
⚡ **Performance optimized** - Powered by Rust and DuckDB.

## 📦 Installation

### From PyPI using uv
```bash
uv add git+shttps://github.com/peter-fm/snapbase.git
```

### From Source
```bash
git clone https://github.com/peter-fm/snapbase.git
cd snapbase/python/snapbase
pip install -e .
```

### Prerequisites
- Python 3.9 or later
- Polars (automatically installed as a dependency)
- Rust toolchain (for building from source)

## 🚀 Quick Start

### Basic Usage

```python
import snapbase

# Initialize a workspace
workspace = snapbase.Workspace("/path/to/your/data")

# Create your first snapshot
snapshot = workspace.create_snapshot("data.csv", name="initial")
print(f"Created snapshot: {snapshot.name}")

# Make changes to your data file, then create another snapshot
updated_snapshot = workspace.create_snapshot("data.csv", name="updated")

# Check status against baseline - returns structured ChangeDetectionResult
result = workspace.status("data.csv", baseline="initial")
print(f"Schema changes: {result.schema_changes.has_changes()}")
print(f"Row changes: {result.row_changes.has_changes()}")
print(f"Total row changes: {result.row_changes.total_changes()}")

# List all snapshots
snapshots = workspace.list_snapshots("data.csv")
for snapshot in snapshots:
    print(f"Snapshot: {snapshot.name} (created: {snapshot.created_at})")
```

### Advanced Usage

#### Working with Different Data Formats

```python
import snapbase

workspace = snapbase.Workspace("./my_project")

# CSV files
workspace.create_snapshot("products.csv", name="products_v1")

# JSON files
workspace.create_snapshot("api_response.json", name="api_v1")

# Parquet files
workspace.create_snapshot("large_dataset.parquet", name="dataset_v1")

# SQL files (executes query and snapshots result)
workspace.create_snapshot("daily_report.sql", name="report_20240115")
```

#### Storage Configuration

```python
import snapbase

# Local storage (default)
workspace = snapbase.Workspace("./data", storage_backend="local")

# S3 storage
workspace = snapbase.Workspace(
    "./data", 
    storage_backend="s3",
    s3_bucket="my-bucket",
    s3_prefix="snapshots/"
)
```

#### Querying Historical Data

Snapbase queries return **Polars DataFrames** for maximum performance and flexibility. Polars provides zero-copy Arrow-based data processing that's much faster than JSON or CSV parsing.

```python
import snapbase
import polars as pl

workspace = snapbase.Workspace("./data")

# Query returns a Polars DataFrame (not JSON!)
df = workspace.query("data.csv", "SELECT * FROM data WHERE price > 100")

# Polars DataFrames are powerful and fast
print(f"Query returned {df.height} rows and {df.width} columns")
print(df.head())

# Use Polars operations for fast data analysis
summary = df.group_by("category").agg([
    pl.col("price").mean().alias("avg_price"),
    pl.col("price").count().alias("count")
])
print(summary)

# Query across multiple snapshots
results = workspace.query("data.csv", """
    SELECT 
        snapshot_name,
        COUNT(*) as record_count,
        AVG(price) as avg_price
    FROM data 
    GROUP BY snapshot_name
    ORDER BY snapshot_name
""")

# Iterate over results (Polars DataFrame)
for row in results.iter_rows(named=True):
    print(f"Snapshot: {row['snapshot_name']}, Records: {row['record_count']}")
```

##### Converting to Other Formats

While Polars DataFrames are highly efficient, you can easily convert to other formats when needed:

```python
# Convert to Pandas DataFrame
pandas_df = df.to_pandas()

# Convert to JSON string
json_str = df.write_json()

# Convert to Python dictionary
dict_data = df.to_dict(as_series=False)

# Convert to NumPy array
numpy_array = df.to_numpy()

# Convert to Arrow Table (zero-copy)
arrow_table = df.to_arrow()

# Save to various formats
df.write_csv("output.csv")
df.write_parquet("output.parquet")
```

##### Why Polars?

We chose Polars as the default return format because:

- **🚀 Performance**: 10-100x faster than JSON parsing for large datasets
- **🧠 Memory Efficient**: Columnar layout uses less memory than row-based formats  
- **🔄 Zero-Copy**: Direct Arrow integration eliminates data copying
- **🛠️ Rich API**: Powerful operations for filtering, grouping, and transforming data
- **🔗 Ecosystem**: Easy conversion to Pandas, NumPy, Arrow, and other formats
- **📊 Data Science**: Native integration with modern data science workflows

If you're used to Pandas, Polars has a similar API but with better performance!

#### Change Detection and Analysis

Snapbase now returns **structured objects** instead of JSON strings, providing type safety and better IDE support:

```python
import snapbase

workspace = snapbase.Workspace("./data")

# Create baseline snapshot
workspace.create_snapshot("inventory.csv", name="baseline")

# ... make changes to inventory.csv ...

# Create new snapshot and check status
workspace.create_snapshot("inventory.csv", name="current")
result = workspace.status("inventory.csv", baseline="baseline")

# Access schema changes with full type safety
schema_changes = result.schema_changes
if schema_changes.has_changes():
    print("Schema Changes Detected:")
    
    # Check for added columns
    for col_addition in schema_changes.columns_added:
        print(f"  + Added column: {col_addition.name} ({col_addition.data_type})")
    
    # Check for removed columns
    for col_removal in schema_changes.columns_removed:
        print(f"  - Removed column: {col_removal.name} ({col_removal.data_type})")
    
    # Check for column renames
    for col_rename in schema_changes.columns_renamed:
        print(f"  ~ Renamed column: {col_rename.from} → {col_rename.to}")
    
    # Check for type changes
    for type_change in schema_changes.type_changes:
        print(f"  ⚠ Type changed: {type_change.column} from {type_change.from} to {type_change.to}")

# Access row changes with detailed information
row_changes = result.row_changes
if row_changes.has_changes():
    print(f"\nRow Changes: {row_changes.total_changes()} total changes")
    
    # Analyze row additions
    print(f"Added rows: {len(row_changes.added)}")
    for addition in row_changes.added:
        print(f"  + Row {addition.row_index}: {addition.data}")
    
    # Analyze row deletions
    print(f"Removed rows: {len(row_changes.removed)}")
    for removal in row_changes.removed:
        print(f"  - Row {removal.row_index}: {removal.data}")
    
    # Analyze row modifications with cell-level details
    print(f"Modified rows: {len(row_changes.modified)}")
    for modification in row_changes.modified:
        print(f"  ~ Row {modification.row_index}:")
        for column, cell_change in modification.changes.items():
            print(f"    {column}: '{cell_change.before}' → '{cell_change.after}'")
```

#### Comparing Two Snapshots

```python
# Compare two specific snapshots
result = workspace.diff("inventory.csv", "baseline", "current")

# Same structured access as status()
print(f"Schema changes: {result.schema_changes.has_changes()}")
print(f"Row changes: {result.row_changes.total_changes()}")

# Access all change details with type safety
for modification in result.row_changes.modified:
    print(f"Row {modification.row_index} changed:")
    for column, change in modification.changes.items():
        print(f"  {column}: {change.before} → {change.after}")
```

#### Export and Backup

```python
import snapbase

workspace = snapbase.Workspace("./data")

# Export a specific snapshot to CSV
workspace.export_snapshot("data.csv", "baseline", "backup.csv")

# Export to Parquet format
workspace.export_snapshot("data.csv", "baseline", "backup.parquet")

# Export by date (finds latest snapshot before specified date)
workspace.export_by_date("data.csv", "2024-01-15", "historical_backup.csv")
```

## 📚 API Reference

### `snapbase.Workspace`

The main class for interacting with snapbase functionality.

#### Constructor
```python
Workspace(path: str, storage_backend: str = "local", **kwargs)
```

**Parameters:**
- `path`: Path to the workspace directory
- `storage_backend`: Storage backend ("local" or "s3")
- `**kwargs`: Additional configuration options (e.g., `s3_bucket`, `s3_prefix`)

#### Methods

##### `create_snapshot(file_path: str, name: str = None) -> Snapshot`
Create a new snapshot of the specified file.

**Parameters:**
- `file_path`: Path to the file to snapshot
- `name`: Optional name for the snapshot (auto-generated if not provided)

**Returns:** `Snapshot` object with metadata

##### `list_snapshots(file_path: str = None) -> List[Snapshot]`
List all snapshots, optionally filtered by file path.

**Parameters:**
- `file_path`: Optional file path to filter snapshots

**Returns:** List of `Snapshot` objects

##### `status(file_path: str, baseline: str = None) -> ChangeDetectionResult`
Check status of current file against a baseline snapshot.

**Parameters:**
- `file_path`: Path to the file to analyze
- `baseline`: Name of baseline snapshot (uses latest if not provided)

**Returns:** `ChangeDetectionResult` object with structured change information

```python
result = workspace.status("data.csv", "baseline")
# Access schema changes
schema_changes = result.schema_changes
row_changes = result.row_changes

# Check if changes exist
if schema_changes.has_changes() or row_changes.has_changes():
    print("Changes detected!")
```

##### `query(file_path: str, sql: str, limit: int = None) -> polars.DataFrame`
Execute SQL query against historical snapshots.

**Parameters:**
- `file_path`: Path to the file to query
- `sql`: SQL query to execute
- `limit`: Optional limit on number of rows returned

**Returns:** Polars DataFrame with query results

**Note:** Polars DataFrames can be easily converted to other formats:
- `df.to_pandas()` - Convert to Pandas DataFrame
- `df.write_json()` - Convert to JSON string  
- `df.to_dict()` - Convert to Python dictionary
- `df.to_arrow()` - Convert to Arrow Table (zero-copy)

##### `export_snapshot(file_path: str, snapshot_name: str, output_path: str)`
Export a snapshot to a file.

**Parameters:**
- `file_path`: Source file path
- `snapshot_name`: Name of snapshot to export
- `output_path`: Destination file path

##### `diff(source: str, from_snapshot: str, to_snapshot: str) -> ChangeDetectionResult`
Compare two specific snapshots.

**Parameters:**
- `source`: Source file path
- `from_snapshot`: Name of the baseline snapshot
- `to_snapshot`: Name of the comparison snapshot

**Returns:** `ChangeDetectionResult` object with structured change information

```python
result = workspace.diff("data.csv", "v1", "v2")
# Same structured access as status()
for modification in result.row_changes.modified:
    print(f"Row {modification.row_index} changed")
```

##### `export_by_date(file_path: str, date: str, output_path: str)`
Export the latest snapshot before a specific date.

**Parameters:**
- `file_path`: Source file path
- `date`: Date string (YYYY-MM-DD format)
- `output_path`: Destination file path

### `snapbase.Snapshot`

Represents a snapshot with metadata.

#### Properties
- `name`: Snapshot name
- `created_at`: Creation timestamp
- `file_path`: Original file path
- `size`: File size in bytes
- `schema`: Data schema information

### `snapbase.ChangeDetectionResult`

Main result object returned by `status()` and `diff()` methods.

#### Properties
- `schema_changes`: `SchemaChanges` object containing schema-level changes
- `row_changes`: `RowChanges` object containing row-level changes

### `snapbase.SchemaChanges`

Contains schema-level changes between snapshots.

#### Properties
- `column_order`: Optional `ColumnOrderChange` if column order changed
- `columns_added`: List of `ColumnAddition` objects for new columns
- `columns_removed`: List of `ColumnRemoval` objects for deleted columns  
- `columns_renamed`: List of `ColumnRename` objects for renamed columns
- `type_changes`: List of `TypeChange` objects for columns with changed data types

#### Methods
- `has_changes()`: Returns `True` if any schema changes exist

### `snapbase.RowChanges`

Contains row-level changes between snapshots.

#### Properties
- `modified`: List of `RowModification` objects for changed rows
- `added`: List of `RowAddition` objects for new rows
- `removed`: List of `RowRemoval` objects for deleted rows

#### Methods
- `has_changes()`: Returns `True` if any row changes exist
- `total_changes()`: Returns total number of changed rows

### Change Detail Objects

#### `ColumnAddition`
- `name`: Column name
- `data_type`: Data type (e.g., "VARCHAR", "INTEGER")
- `position`: Position in schema
- `nullable`: Whether column allows null values
- `default_value`: Default value (if any)

#### `ColumnRemoval`
- `name`: Column name
- `data_type`: Data type
- `position`: Position in schema
- `nullable`: Whether column allowed null values

#### `ColumnRename`
- `from`: Original column name
- `to`: New column name

#### `TypeChange`
- `column`: Column name
- `from`: Original data type
- `to`: New data type

#### `RowModification`
- `row_index`: Index of the modified row
- `changes`: Dictionary of `{column_name: CellChange}` for modified cells

#### `CellChange`
- `before`: Original cell value
- `after`: New cell value

#### `RowAddition`
- `row_index`: Index of the added row
- `data`: Dictionary of `{column_name: value}` for the new row

#### `RowRemoval`  
- `row_index`: Index of the removed row
- `data`: Dictionary of `{column_name: value}` for the removed row

## ⚙️ Configuration

### Configuration System Overview

Snapbase uses a flexible TOML-based configuration system that supports both workspace-specific and global settings:

**Configuration Files:**
- **Workspace config**: `snapbase.toml` (in each workspace)
- **Global config**: `~/.snapbase/global.toml` (user-wide defaults)
- **Environment variables**: Shell environment variables

**Priority Order:**
1. **Workspace config** (highest priority)
2. **Global config** 
3. **Environment variables**
4. **Built-in defaults** (lowest priority)

**Key Benefits:**
- Each project can have its own storage backend (local vs S3)
- Global settings serve as defaults for new workspaces
- Environment variables provide temporary overrides
- Automatic global config creation on first init
- Human-readable TOML format for easy editing

### Storage Backends

#### Local Storage (Default)
```python
workspace = snapbase.Workspace("./data", storage_backend="local")
```

#### S3 Storage
```python
workspace = snapbase.Workspace(
    "./data",
    storage_backend="s3",
    s3_bucket="my-bucket",
    s3_prefix="snapshots/",
    s3_region="us-west-2"
)
```

**Workspace vs Global Configuration:**
- **Workspace config**: Each project can have its own storage backend (local, S3, etc.)
- **Global config**: Default settings for new workspaces
- **Priority**: Workspace config takes precedence over global config

### Configuration Files

#### Local Configuration (`snapbase.toml`)

Create a `snapbase.toml` file in your project root:

```toml
[storage.Local]
path = ".snapbase"

[snapshot]
default_name_pattern = "{source}_{date}_{seq}"
```

Or for S3 storage:

```toml
[storage.S3]
bucket = "my-bucket"
prefix = "snapbase/"
region = "us-west-2"
access_key_id = "your_access_key"    # Optional - can use env vars
secret_access_key = "your_secret"   # Optional - can use env vars

[snapshot]
default_name_pattern = "{source}_{format}_{seq}"
```

#### Global Configuration (`~/.snapbase/global.toml`)

Same format as local config, but serves as defaults for all workspaces.

#### Environment Variables

Set environment variables for temporary overrides:

```bash
# S3 Configuration
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export SNAPBASE_S3_BUCKET=my-bucket
export SNAPBASE_S3_PREFIX=snapbase/
export SNAPBASE_S3_REGION=us-west-2

# Snapshot naming configuration
export SNAPBASE_DEFAULT_NAME_PATTERN={source}_{date}_{seq}

# Database connections (for SQL files)
export DB_HOST=localhost
export DB_USER=myuser
export DB_PASSWORD=mypass
export DB_NAME=mydb
```

#### .env File Support

Snapbase automatically loads `.env` files from your workspace directory for environment variables:

```bash
# .env file example
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
SNAPBASE_S3_BUCKET=my-bucket
DB_PASSWORD=secret123
```

## 📁 File Format Support

| Format | Read | Export | Notes |
|--------|------|--------|-------|
| CSV | ✅ | ✅ | Auto-detects delimiters and encoding |
| JSON | ✅ | ✅ | Flattens nested structures on export |
| Parquet | ✅ | ✅ | Native format for storage |
| SQL | ✅ | ✅ | Executes queries against databases |


## 💡 Examples

### Data Science Workflow

```python
import snapbase
import polars as pl

# Initialize workspace
workspace = snapbase.Workspace("./ml_project")

# Create snapshot of raw data
workspace.create_snapshot("raw_data.csv", name="raw_data")

# Query and process data using Polars (faster than loading files)
raw_df = workspace.query("raw_data.csv", "SELECT * FROM data")

# Process data with Polars (much faster than Pandas)
processed_df = raw_df.drop_nulls().fill_null(0)  # Your processing logic

# Save processed data
processed_df.write_csv("processed_data.csv")

# Create snapshot of processed data
workspace.create_snapshot("processed_data.csv", name="processed_v1")

# Compare versions using fast queries
raw_count = workspace.query("raw_data.csv", "SELECT COUNT(*) as count FROM data").item(0, "count")
processed_count = workspace.query("processed_data.csv", "SELECT COUNT(*) as count FROM data").item(0, "count")

print(f"Processing removed {raw_count - processed_count} rows")

# Advanced: Convert to Pandas if needed for specific libraries
if need_pandas_df:
    pandas_df = processed_df.to_pandas()
```

### ETL Pipeline Monitoring

```python
import snapbase
from datetime import datetime

workspace = snapbase.Workspace("./etl_monitoring")

def run_etl_pipeline():
    # Your ETL logic here
    extract_data()
    transform_data()
    load_data()
    
    # Create snapshot with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot = workspace.create_snapshot("output_data.csv", name=f"etl_{timestamp}")
    
    # Compare with previous run
    snapshots = workspace.list_snapshots("output_data.csv")
    if len(snapshots) > 1:
        previous_snapshot = snapshots[-2]  # Second most recent
        changes = workspace.status("output_data.csv", baseline=previous_snapshot.name)
        
        if changes:
            print(f"ETL pipeline detected {len(changes)} changes")
            # Send alert or log changes
        else:
            print("ETL pipeline: No changes detected")
    
    return snapshot

# Run ETL pipeline
snapshot = run_etl_pipeline()
```

### API Response Monitoring

```python
import snapbase
import requests
import json

workspace = snapbase.Workspace("./api_monitoring")

def monitor_api_endpoint():
    # Fetch API response
    response = requests.get("https://api.example.com/data")
    data = response.json()
    
    # Save to file
    with open("api_response.json", "w") as f:
        json.dump(data, f)
    
    # Create snapshot
    snapshot = workspace.create_snapshot("api_response.json", name=f"api_{datetime.now():%Y%m%d_%H%M%S}")
    
    # Check for changes
    changes = workspace.status("api_response.json")
    if changes:
        print(f"API structure changed! {len(changes)} changes detected")
        for change in changes:
            print(f"  - {change.description}")
    
    return snapshot
```

## ⚡ Performance Tips

1. **Use Parquet for large datasets** - Better compression and query performance
2. **Configure S3 storage** for team collaboration and scalability
3. **Use SQL summaries** for very large files instead of full snapshots
4. **Clean up old snapshots** regularly to save storage space

## 🚨 Error Handling

```python
import snapbase

try:
    workspace = snapbase.Workspace("./data")
    snapshot = workspace.create_snapshot("data.csv", name="test")
except snapbase.WorkspaceError as e:
    print(f"Workspace error: {e}")
except snapbase.SnapshotError as e:
    print(f"Snapshot error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## 🧪 Testing

```bash
# Run tests
pytest

# Run tests with coverage
pytest --cov=snapbase

# Run specific test file
pytest tests/test_basic.py
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `pytest`
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## 💬 Support

- 📖 [Documentation](https://github.com/peter-fm/snapbase/wiki)
- 🐛 [Report Issues](https://github.com/peter-fm/snapbase/issues)
- 💬 [Discussions](https://github.com/peter-fm/snapbase/discussions)