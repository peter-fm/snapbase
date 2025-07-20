# Snapbase Python Library

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Python bindings for snapbase - a snapshot-based structured data diff tool that detects schema, column-level, and cell-level changes between versions of structured datasets.

## Features

✨ **Snapshot-based tracking** - Create immutable snapshots of your data with metadata  
🔍 **Comprehensive change detection** - Detect schema changes, row additions/deletions, and cell-level modifications  
📊 **Multiple format support** - CSV, JSON, Parquet, and SQL files  
☁️ **Cloud storage support** - Store snapshots locally or in S3  
📤 **Export capability** - Export snapshots to CSV or Parquet files  
📈 **SQL querying** - Query historical snapshots using SQL, returns high-performance Polars DataFrames  
🏷️ **Automatic naming** - Configurable patterns for automatic snapshot naming  
⚡ **Performance optimized** - Streaming processing for large datasets  
🧹 **Storage management** - Archive cleanup and compression

## 📦 Installation

### From PyPI
```bash
pip install snapbase
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

# Detect changes between snapshots
changes = workspace.detect_changes("data.csv", baseline="initial")
print(f"Detected {len(changes)} changes")

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

```python
import snapbase

workspace = snapbase.Workspace("./data")

# Create baseline snapshot
workspace.create_snapshot("inventory.csv", name="baseline")

# ... make changes to inventory.csv ...

# Create new snapshot and detect changes
workspace.create_snapshot("inventory.csv", name="current")
changes = workspace.detect_changes("inventory.csv", baseline="baseline")

# Analyze changes
for change in changes:
    if change.type == "row_added":
        print(f"New row added: {change.data}")
    elif change.type == "row_removed":
        print(f"Row removed: {change.data}")
    elif change.type == "cell_modified":
        print(f"Cell changed: {change.column} from {change.old_value} to {change.new_value}")
    elif change.type == "schema_change":
        print(f"Schema change: {change.description}")
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

##### `detect_changes(file_path: str, baseline: str = None) -> List[Change]`
Detect changes between current file state and a baseline snapshot.

**Parameters:**
- `file_path`: Path to the file to analyze
- `baseline`: Name of baseline snapshot (uses latest if not provided)

**Returns:** List of `Change` objects

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

### `snapbase.Change`

Represents a detected change between snapshots.

#### Properties
- `type`: Type of change ("row_added", "row_removed", "cell_modified", "schema_change")
- `description`: Human-readable description
- `column`: Column name (for cell changes)
- `old_value`: Previous value (for cell changes)
- `new_value`: New value (for cell changes)
- `data`: Row data (for row changes)

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
        changes = workspace.detect_changes("output_data.csv", baseline=previous_snapshot.name)
        
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
    changes = workspace.detect_changes("api_response.json")
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