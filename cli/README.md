# Snapbase Command Line Interface

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A queryable time machine for your structured data from entire databases and SQL query results to Excel, CSV, parquet and JSON files. Snapbase is a data version control system augmented by SQL. Supports both local and cloud snapshot storage.

## Features

✨ **Snapshot-based tracking** - Create immutable snapshots of your data with metadata  
🔍 **Comprehensive change detection** - Detect schema changes, row additions/deletions, and cell-level modifications  
📊 **Multiple format support** - Databases, SQL queries, Excel, CSV, JSON and Parquet files  
☁️ **Cloud storage support** - Store snapshots locally, in S3, or S3 Express One Zone (Directory Buckets)  
📈 **SQL querying** - Query across snapshots using SQL to monitor changes at the cell level over time.  
⚡ **Performance optimized** - Powered by Rust and DuckDB.


## 📦 Installation

### From GitHub Releases

Download the latest release for your platform:

#### Linux (x86_64)
```bash
curl -L https://github.com/peter-fm/snapbase/releases/latest/download/snapbase-linux-x64 -o snapbase
chmod +x snapbase
sudo mv snapbase /usr/local/bin/
```

#### macOS (ARM64/Apple Silicon)
```bash
curl -L https://github.com/peter-fm/snapbase/releases/latest/download/snapbase-macos-arm64 -o snapbase
chmod +x snapbase
sudo mv snapbase /usr/local/bin/
```

#### Windows (x86_64)
```bash
curl -L https://github.com/peter-fm/snapbase/releases/latest/download/snapbase-windows-x64.exe -o snapbase.exe
# Add snapbase.exe to your PATH
```

#### Manual Download
Visit the [releases page](https://github.com/peter-fm/snapbase/releases) to download the appropriate binary for your system.


### From Source

```bash
cargo install --git https://github.com/peter-fm/snapbase.git
```

### Prerequisites

- Rust 1.70 or later

## 🚀 Quick Start

### 1. Initialize a workspace

```bash
snapbase init
```

This creates a `.snapbase/` directory in your current folder to store snapshots and configuration.

**⚠️ New in this version**: Single-source queries now require the `--source` parameter. Queries without `--source` are treated as workspace-wide cross-source queries.

### 2. Create your first snapshot

```bash
snapbase snapshot data.csv --name initial
# Or use automatic naming (generates: data_csv_1)
snapbase snapshot data.csv
```

### 3. Make changes to your data and check the status

```bash
snapbase status data.csv
```

### 4. Create another snapshot

```bash
snapbase snapshot data.csv --name updated
# Or use automatic naming (generates: data_csv_2)
snapbase snapshot data.csv
```

### 5. View all snapshots

```bash
snapbase list
```

## 📖 Usage

### Basic Commands

#### Initialize workspace
```bash
snapbase init                    # Initialize in current directory
snapbase init --from-global     # Use global config instead of local config
```

**Configuration Priority**: The `init` command uses different configuration priorities:

- **Default behavior**: Uses local `snapbase.toml` file → defaults, then saves to global config if missing
- **With `--from-global`**: Uses global config (`~/.snapbase/global.toml`) → `snapbase.toml` file → defaults

This means regular `init` prioritizes project-specific settings (`snapbase.toml`), while `--from-global` uses your saved global configuration. The global config is automatically created on first init if it doesn't exist.

#### Create snapshots
```bash
# With explicit names
snapbase snapshot data.csv --name snapshot1
snapbase snapshot data.csv --name snapshot2

# With automatic naming (uses configurable pattern)
snapbase snapshot data.csv                    # Generates: data_csv_1
snapbase snapshot sales.json                  # Generates: sales_json_1
snapbase snapshot large_file.csv  # Generates: large_file_csv_1

# Database snapshots
snapbase snapshot --database my-database --name prod_backup
snapbase snapshot --database my-database --tables users,orders --name user_data
snapbase snapshot --database my-database --exclude-tables temp_* --name clean_backup

# SQL file snapshots
snapbase snapshot queries/daily_sales.sql --name sales_report
snapbase snapshot analytics/user_metrics.sql --name user_analytics
```

#### Check status
```bash
snapbase status data.csv           # Compare against latest snapshot
snapbase status data.csv --compare-to snapshot1    # Compare against specific snapshot
snapbase status data.csv --json                    # JSON output
```

#### List snapshots
```bash
snapbase list                    # List all snapshots
snapbase list data.csv           # List snapshots for specific file
snapbase list --json            # JSON output
```

#### View snapshot details
```bash
snapbase show data.csv snapshot1
snapbase show data.csv snapshot1 --detailed
snapbase show data.csv snapshot1 --json
```

#### Export snapshot data
```bash
# Export specific snapshot to CSV
snapbase export data.csv --to snapshot1 --file exported_data.csv

# Export to Parquet format
snapbase export data.csv --to snapshot1 --file exported_data.parquet

# Export by date (finds latest snapshot before specified date)
snapbase export data.csv --to-date "2025-01-01" --file data_backup.csv
snapbase export data.csv --to-date "2025-01-01 15:00:00" --file data_backup.csv
snapbase export data.csv --to-date "2025-01-01T15:00:00+01:00" --file data_backup.csv

# Preview what would be exported
snapbase export data.csv --to snapshot1 --file output.csv --dry-run

# Force overwrite existing output file
snapbase export data.csv --to snapshot1 --file existing.csv --force
```

#### Query historical data

**Cross-Snapshot Queries** - Query across multiple sources with joins and snapshot filtering:

```bash
# Cross-source workspace queries (sources mounted as views)
snapbase query "SELECT * FROM orders_csv o JOIN users_csv u ON u.id = o.user_id" --snapshot "*_v1"
snapbase query "SELECT COUNT(*) as total_orders, snapshot_name FROM orders_csv GROUP BY snapshot_name"

# Snapshot filtering patterns
snapbase query "SELECT * FROM orders_csv" --snapshot "*_v1"      # All snapshots ending with "_v1"
snapbase query "SELECT * FROM orders_csv" --snapshot "latest"    # Latest snapshot only
snapbase query "SELECT * FROM orders_csv" --snapshot "orders_1"  # Specific snapshot name
snapbase query "SELECT * FROM orders_csv"                        # All snapshots (default: "*")
```

**Single-Source Queries** - Query individual files with snapshot filtering:

```bash
# Single-source queries (requires --source parameter)
snapbase query --source data.csv "SELECT * FROM data WHERE price > 20"
snapbase query --source data.csv "SELECT * FROM data" --format csv
snapbase query --source data.csv "SELECT * FROM data WHERE user_id = 101" --snapshot "*_v1"
snapbase query --source data.csv --list-snapshots

# Compare snapshots (see Examples section for detailed patterns)
snapbase query --source data.csv "
  SELECT * FROM data 
  WHERE id NOT IN (SELECT id FROM data WHERE snapshot_name = 'v1_0')
" # Find new records

# Query specific snapshots
snapbase query --source data.csv "
  SELECT COUNT(*) as total_records,
         AVG(price) as avg_price
  FROM data WHERE snapshot_name = 'v1_0'
"

# Query how a product price has changed over time
snapbase query --source data.csv "
  SELECT distinct product, price,
  FROM data WHERE snapshot_timestamp >= '2024-01-1' and snapshot_timestamp < '2025-01-01'
"
```

#### Cleanup old snapshots
```bash
snapbase cleanup --keep-full 5    # Keep rollback data for 5 most recent snapshots
snapbase cleanup --dry-run        # Preview cleanup
```

### Advanced Usage

#### Storage Configuration

Configure storage backend (local or S3):

```bash
# Local storage - saves to current workspace
snapbase config storage --backend local --local-path .snapbase

# S3 storage - saves to current workspace
snapbase config storage --backend s3 --s3-bucket my-bucket --s3-prefix snapbase/

# S3 Express One Zone (Directory Buckets) for high-performance operations
snapbase config storage --backend s3 --s3-bucket my-express-bucket --s3-express --s3-availability-zone use1-az5

# Save to global config instead of workspace
snapbase config storage --backend local --local-path .snapbase --global
```

**Configuration Priority**: Storage configuration uses workspace-first priority:

1. **Workspace config** (`snapbase.toml` in current workspace) - highest priority
2. **Global config** (`~/.snapbase/global.toml`) - fallback
3. **Environment variables** - fallback
4. **Defaults** - lowest priority

By default, `config storage` saves to the current workspace. Use `--global` to save to global config instead.

For S3, set environment variables:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export SNAPBASE_S3_BUCKET=my-bucket
export SNAPBASE_S3_PREFIX=snapbase/
export SNAPBASE_S3_REGION=us-west-2

# For S3 Express One Zone (Directory Buckets)
export SNAPBASE_S3_USE_EXPRESS=true
export SNAPBASE_S3_AVAILABILITY_ZONE=use1-az5
```

#### Snapshot Naming Configuration

Configure automatic snapshot naming patterns when no `--name` is provided:

```bash
# Set default pattern (default: {source}_{format}_{seq})
snapbase config default-name "{source}_{format}_{seq}"

# Examples of patterns:
snapbase config default-name "{source}_{timestamp}"         # sales_20250716_143052
snapbase config default-name "{source}_{date}_{seq}"        # sales_20250716_1
snapbase config default-name "{source}_{format}_{hash}"     # sales_csv_a7b3c9d
snapbase config default-name "backup_{source}_{seq}"        # backup_sales_1

# View current configuration
snapbase config show
```

**Available pattern variables:**
- `{source}` - source identifier:
  - **File snapshots**: filename without extension (e.g., "sales" from "sales.csv")
  - **Database snapshots**: `{database_name}_{table_name}` (e.g., "ecommerce_users" from database "ecommerce" and table "users")
- `{source_ext}` - file extension (e.g., "csv" from "sales.csv")
- `{format}` - file format (csv, json, parquet, sql, xlsx, xls, etc.)
- `{seq}` - auto-incrementing sequence number (1, 2, 3, ...)
- `{timestamp}` - current timestamp (YYYYMMDD_HHMMSS format)
- `{date}` - current date (YYYYMMDD format)
- `{time}` - current time (HHMMSS format)
- `{hash}` - 7-character random hash (e.g., "a7b3c9d")
- `{user}` - system username

**Note**: Database snapshots don't have separate `{database}` and `{table}` variables - both are combined into the `{source}` variable.

**Configuration priority:**
1. Command line `--name` parameter (highest priority)
2. `SNAPBASE_DEFAULT_NAME_PATTERN` environment variable
3. Local config file (`snapbase.toml`)
4. Global config file (`~/.snapbase/global.toml`)
5. Default pattern: `{source}_{format}_{seq}`

Set via environment variable:
```bash
export SNAPBASE_DEFAULT_NAME_PATTERN={source}_{date}_{seq}
```

#### Database Snapshot Support

Create snapshots of entire database schemas with the `--database` flag:

```bash
# Snapshot all tables in a configured database
snapbase snapshot --database my-database --name prod_backup

# Snapshot specific tables only
snapbase snapshot --database my-database --tables users,orders,products --name core_tables

# Exclude specific tables (supports wildcards)
snapbase snapshot --database my-database --exclude-tables temp_*,cache_* --name clean_backup

# Use automatic naming
snapbase snapshot --database my-database  # Generates: my-database_users_sql_1, my-database_orders_sql_1, etc.
```

**Database Configuration**: Configure databases in `snapbase.toml`:

```toml
[databases.my-database]
type = "mysql"
host = "localhost"
port = 3306
database = "myapp"
username = "dbuser"
password_env = "DB_PASSWORD"  # Environment variable containing password
tables = ["users", "orders", "products"]  # Optional: specific tables
exclude_tables = ["temp_*", "cache_*"]     # Optional: exclude patterns

[databases.prod-postgres]
type = "postgresql"
connection_string = "postgresql://user@prod.example.com:5432/proddb"
password_env = "PROD_DB_PASSWORD"
tables = ["*"]  # All tables
exclude_tables = ["logs_*"]

[databases.local-sqlite]
type = "sqlite"
database = "./data/local.db"
tables = ["*"]
```

**Supported Database Types**:
- **MySQL**: `type = "mysql"`
- **PostgreSQL**: `type = "postgresql"`
- **SQLite**: `type = "sqlite"`

**Connection Methods**:
- **Individual fields**: `host`, `port`, `database`, `username`
- **Connection string**: `connection_string` (PostgreSQL format)
- **Environment variables**: Use `password_env` for secure password handling

**Table Selection**:
- `tables = ["*"]` - All tables (default)
- `tables = ["users", "orders"]` - Specific tables
- `exclude_tables = ["temp_*", "logs_*"]` - Exclude patterns (supports wildcards)

#### SQL File Support

Track SQL query results over time:

```sql
-- queries/daily_sales.sql
-- ATTACH 'host={DB_HOST} user={DB_USER} password={DB_PASSWORD} database={DB_NAME}' AS mydb (TYPE postgres);

USE mydb;

SELECT 
    date,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count
FROM transactions 
WHERE date >= '2025-01-01'
GROUP BY date
ORDER BY date;
```

```bash
# With explicit date-based naming
snapbase snapshot queries/daily_sales.sql --name "sales_$(date +%Y%m%d)"

# Or configure automatic date-based naming
snapbase config default-name "sales_{date}"
snapbase snapshot queries/daily_sales.sql  # Generates: sales_20250716
```

#### Large File Handling

For very large files, snapbase uses DuckDB's streaming capabilities to handle them efficiently without loading into memory.

**SQL Summarization for Large Files**: For very large datasets, consider creating SQL summary queries instead of snapshoting the entire file:

```sql
-- large_file_summary.sql
SELECT 
    category, 
    COUNT(name) as count,
    AVG(price) as avg_price,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM 'large_file.csv' 
GROUP BY category;
```

```bash
# Snapshot the summary instead of the full file
snapbase snapshot large_file_summary.sql --name summary_2025_01_16
```

This approach works with any data source (CSV, JSON, Parquet, database tables) and provides efficient tracking of key metrics while avoiding the overhead of storing massive datasets.

## 💡 Examples

### Example 1: Product Catalog Changes

```bash
# Initialize workspace
snapbase init

# Create initial snapshot
snapbase snapshot products.csv --name v1.0
# Or use automatic naming: snapbase snapshot products.csv  # Generates: products_csv_1

# After making changes...
snapbase status products.csv
```

Output:
```
📊 Checking status of 'products.csv' against snapshot 'v1.0'...

📋 Schema Changes: None

📊 Row Changes:
├─ Added: 2 rows
├─ Removed: 1 row
└─ Modified: 3 rows

🔍 Cell Changes:
├─ Price changes: 3 cells
└─ Name changes: 1 cell

Summary: 6 total changes detected
```

### Example 2: Finding Differences Between Snapshots

Snapbase stores all snapshots in a queryable format. You can use SQL to find specific differences between snapshots:

#### Find Added Records
```bash
# Find records that exist in current snapshot but not in previous
snapbase query products.csv "
  SELECT 
    current.*,
    'ADDED' as change_type
  FROM (SELECT * FROM data WHERE snapshot_name = 'v2_0') current
  LEFT JOIN (SELECT * FROM data WHERE snapshot_name = 'v1_0') previous ON current.id = previous.id
  WHERE previous.id IS NULL
"
```

#### Find Removed Records
```bash
# Find records that existed in previous snapshot but not in current
snapbase query products.csv "
  SELECT 
    previous.*,
    'REMOVED' as change_type
  FROM (SELECT * FROM data WHERE snapshot_name = 'v1_0') previous
  LEFT JOIN (SELECT * FROM data WHERE snapshot_name = 'v2_0') current ON previous.id = current.id
  WHERE current.id IS NULL
"
```

#### Find Modified Records
```bash
# Find records where specific columns have changed
snapbase query products.csv "
  SELECT 
    current.id,
    current.name,
    current.price as current_price,
    previous.price as previous_price,
    current.price - previous.price as price_change,
    'MODIFIED' as change_type
  FROM (SELECT * FROM data WHERE snapshot_name = 'v2_0') current
  JOIN (SELECT * FROM data WHERE snapshot_name = 'v1_0') previous ON current.id = previous.id
  WHERE current.price != previous.price
     OR current.name != previous.name
"
```

#### Complete Diff Analysis
```bash
# Get a comprehensive view of all changes
snapbase query products.csv "
  WITH 
    current_data AS (SELECT * FROM data WHERE snapshot_name = 'v2_0'),
    previous_data AS (SELECT * FROM data WHERE snapshot_name = 'v1_0'),
    changes AS (
      -- Added records
      SELECT 
        id, name, price, 'ADDED' as change_type,
        NULL as old_price, price as new_price
      FROM current_data
      WHERE id NOT IN (SELECT id FROM previous_data)
      
      UNION ALL
      
      -- Removed records
      SELECT 
        id, name, price, 'REMOVED' as change_type,
        price as old_price, NULL as new_price
      FROM previous_data
      WHERE id NOT IN (SELECT id FROM current_data)
      
      UNION ALL
      
      -- Modified records
      SELECT 
        c.id, c.name, c.price, 'MODIFIED' as change_type,
        p.price as old_price, c.price as new_price
      FROM current_data c
      JOIN previous_data p ON c.id = p.id
      WHERE c.price != p.price OR c.name != p.name
    )
  SELECT 
    change_type,
    COUNT(*) as count,
    AVG(new_price - old_price) as avg_price_change
  FROM changes
  GROUP BY change_type
  ORDER BY change_type
"
```

### Example 3: Database Snapshot Tracking

```bash
# Create database snapshot with explicit naming
snapbase snapshot --database prod-db --name "backup_$(date +%Y%m%d)"

# Or configure automatic date-based naming
snapbase config default-name "{source}_{date}"
snapbase snapshot --database prod-db  # Generates: prod-db_users_20250716, prod-db_orders_20250716, etc.

# Snapshot specific tables
snapbase snapshot --database prod-db --tables users,orders --name user_orders_backup

# View changes between database snapshots
snapbase status --database prod-db --compare-to backup_20250715

# Query historical database snapshots
snapbase query prod-db:users "
  SELECT 
    snapshot_name,
    COUNT(*) as user_count,
    COUNT(DISTINCT email) as unique_emails
  FROM data 
  GROUP BY snapshot_name
  ORDER BY snapshot_name
"
```

### Example 4: Database Query Tracking

```bash
# Track daily sales query with explicit naming
snapbase snapshot daily_sales.sql --name "$(date +%Y%m%d)"

# Or configure automatic date-based naming
snapbase config default-name "{source}_{date}"
snapbase snapshot daily_sales.sql  # Generates: daily_sales_20250716

# Compare today vs yesterday
snapbase query daily_sales.sql "
  SELECT 
    date,
    total_sales,
    LAG(total_sales) OVER (ORDER BY date) as prev_sales,
    total_sales - LAG(total_sales) OVER (ORDER BY date) as daily_change
  FROM data 
  ORDER BY date DESC 
  LIMIT 7
"
```

### Example 5: Schema Evolution

```bash
# Initial schema
snapbase snapshot users.csv --name before_migration

# After adding new columns
snapbase snapshot users.csv --name after_migration

# Check what changed
snapbase status users.csv --compare-to before_migration
```

Output:
```
📋 Schema Changes:
├─ Added columns: email, created_at
├─ Removed columns: legacy_id
└─ Modified columns: phone (varchar(10) → varchar(15))

📊 Row Changes: 1,234 rows modified (column additions)
```

### Example 6: Cross-Snapshot Query Analysis (New!)

**Setup multiple data sources:**
```bash
# Initialize workspace and create snapshots
snapbase init

# Create snapshots for different data sources
snapbase snapshot orders.csv --name orders_v1
snapbase snapshot users.csv --name users_v1
snapbase snapshot products.csv --name products_v1
```

**Cross-source queries with joins:**
```bash
# Join orders and users data across snapshots
snapbase query "
  SELECT o.id, o.product, o.amount, u.name, u.department
  FROM orders_csv o 
  JOIN users_csv u ON u.id = o.user_id
  WHERE o.amount > 50
" --snapshot "*_v1"

# Aggregate data across multiple sources
snapbase query "
  SELECT 
    u.department,
    COUNT(o.id) as total_orders,
    SUM(CAST(o.amount AS DOUBLE)) as total_revenue,
    AVG(CAST(o.amount AS DOUBLE)) as avg_order_value
  FROM orders_csv o
  JOIN users_csv u ON u.id = o.user_id
  GROUP BY u.department
  ORDER BY total_revenue DESC
"

# Track changes across snapshots in workspace
snapbase query "
  SELECT 
    snapshot_name,
    COUNT(*) as record_count,
    AVG(CAST(amount AS DOUBLE)) as avg_amount
  FROM orders_csv 
  GROUP BY snapshot_name
  ORDER BY snapshot_name
"
```

**Snapshot filtering examples:**
```bash
# Query only latest snapshots
snapbase query "SELECT COUNT(*) FROM orders_csv" --snapshot "latest"

# Query all snapshots from a specific version pattern
snapbase query "SELECT COUNT(*) FROM orders_csv" --snapshot "*_v1"

# Query a specific snapshot by name
snapbase query "SELECT * FROM orders_csv WHERE amount > 100" --snapshot "orders_v1"

# Compare data between snapshot patterns
snapbase query "
  SELECT 
    snapshot_name,
    COUNT(*) as orders,
    SUM(CAST(amount AS DOUBLE)) as total
  FROM orders_csv 
  WHERE snapshot_name LIKE '%_v1' OR snapshot_name LIKE '%_v2'
  GROUP BY snapshot_name
" --snapshot "*"
```

**Single-source queries with filtering:**
```bash
# Filter specific snapshots for a single source
snapbase query --source orders.csv "
  SELECT product, COUNT(*) as count
  FROM data
  WHERE snapshot_name = 'orders_v1'
  GROUP BY product
" --snapshot "*_v1"

# Time-based analysis for a single source
snapbase query --source orders.csv "
  SELECT 
    DATE(snapshot_timestamp) as date,
    COUNT(*) as daily_orders
  FROM data
  GROUP BY DATE(snapshot_timestamp)
  ORDER BY date
"
```

### Example 7: Export and Restore Workflow

```bash
# Export a snapshot to CSV for manual review
snapbase export products.csv --to v1.0 --file products_v1.csv

# Export to Parquet for data analysis
snapbase export products.csv --to v1.0 --file products_v1.parquet

# Export by date for backup purposes
snapbase export products.csv --to-date "2025-01-01" --file products_backup.csv

# Preview what would be exported
snapbase export products.csv --to v1.0 --file output.csv --dry-run

# Export database snapshots
snapbase export --database prod-db --to backup_20250716 --file db_backup.csv

# Export SQL query results
snapbase export daily_sales.sql --to sales_20250716 --file sales_data.parquet
```

**Use cases for export:**
- **Data recovery**: Export historical data for manual restoration
- **Analysis**: Export to Parquet for use in data analysis tools
- **Migration**: Export data for importing into other systems
- **Backup**: Create exportable backups of specific snapshots
- **Sharing**: Export specific snapshots for sharing with team members

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

#### Local Storage
```bash
# Save to current workspace (default)
snapbase config storage --backend local --local-path .snapbase

# Save to global config
snapbase config storage --backend local --local-path .snapbase --global
```

#### S3 Storage
```bash
# Save to current workspace (default)
snapbase config storage --backend s3 --s3-bucket my-bucket --s3-prefix snapbase/

# Save to global config
snapbase config storage --backend s3 --s3-bucket my-bucket --s3-prefix snapbase/ --global
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
[storage]
backend = "s3"

[storage.s3]
bucket = "my-bucket"
prefix = "snapbase/"
region = "us-west-2"
# access_key_id and secret_access_key can be set via environment variables

[snapshot]
default_name_pattern = "{source}_{format}_{seq}"
```

For S3 Express One Zone (Directory Buckets):

```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "my-express-bucket"
prefix = "data/"
region = "us-east-1"
use_express = true
availability_zone = "use1-az5"

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

## 📁 File Format Support

| Format | Read | Export | Notes |
|--------|------|--------|-------|
| CSV | ✅ | ✅ | Auto-detects delimiters and encoding |
| JSON | ✅ | ✅ | Flattens nested structures on export |
| Parquet | ✅ | ✅ | Native format for storage |
| SQL | ✅ | ✅ | Executes queries against databases |
| Database | ✅ | ✅ | MySQL, PostgreSQL, SQLite via `--database` flag |

**Export Format Support:**
- **CSV**: All source types can be exported to CSV format
- **Parquet**: All source types can be exported to Parquet format
- **Format determined by file extension**: Use `.csv` or `.parquet` extension on the `--file` parameter

## ⚡ Performance Tips

1. **Large files are handled efficiently** using DuckDB's streaming capabilities:
   ```bash
   snapbase snapshot large_file.csv --name backup
   ```
   
   Large files are processed without loading into memory, providing excellent performance automatically.

2. **Use S3 storage** for team collaboration and better scalability. For high-performance workloads, consider S3 Express One Zone (Directory Buckets) which provides up to 200k read TPS and 100k write TPS

3. **If local, then clean up old snapshots** regularly:
   ```bash
   snapbase cleanup --keep-full 10
   ```

## 🔧 Troubleshooting

### Common Issues

**"No snapshots found to compare against"**
- Create your first snapshot: `snapbase snapshot data.csv --name initial`

**"File is outside the workspace directory"**
- Only files within the workspace directory can be tracked
- Use absolute paths or move files to the workspace

**"Snapshot not found" during export**
- Verify the snapshot exists with: `snapbase list source.csv`
- Check that the snapshot name matches exactly

**S3 connection issues**
- Verify AWS credentials and permissions
- Check bucket name and region configuration
- Ensure IAM user has S3 read/write permissions

**Windows Panic**

If you get an panic like this in windows:
```bash
thread 'main' panicked at C:\Users\runneradmin\.cargo\registry\src\index.crates.io-1949cf8c6b5b557f\duckdb-1.3.2\src\config.rs:127:13:
assertion left == right failed
  left: 1
 right: 0
```
Then you may need to Download Microsoft’s “latest supported Visual C++ Redistributable” (vc_redist.x64.exe) from the official [permalink](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist?view=msvc-170&utm_source=chatgpt.com)

### Debug Mode

Run with verbose logging to diagnose issues:

```bash
snapbase --verbose status data.csv
```

### Performance Issues

For large files:
1. Large files are handled efficiently using DuckDB's streaming capabilities
2. Consider S3 storage for better I/O performance

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `cargo test`
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## 💬 Support

- 📖 [Documentation](https://github.com/peter-fm/snapbase/wiki)
- 🐛 [Report Issues](https://github.com/peter-fm/snapbase/issues)
- 💬 [Discussions](https://github.com/peter-fm/snapbase/discussions)