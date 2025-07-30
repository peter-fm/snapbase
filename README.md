# Snapbase

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[![Latest Release](https://img.shields.io/github/v/release/peter-fm/snapbase?label=latest%20release)](https://github.com/peter-fm/snapbase/releases/latest)

A queryable time machine for your structured data from entire databases and SQL query results to Excel, CSV, parquet and JSON files. Snapbase is a data version control system augmented by SQL. Supports both local and cloud snapshot storage.

## Features

✨ **Snapshot-based tracking** - Create immutable snapshots of your data with metadata  
🔍 **Comprehensive change detection** - Detect schema changes, row additions/deletions, and cell-level modifications  
📊 **Multiple format support** - Databases, SQL queries, Excel, CSV, JSON and Parquet files  
☁️ **Cloud storage support** - Store snapshots locally or in S3  
📈 **SQL querying** - Query across snapshots using SQL to monitor changes at the cell level over time.  
⚡ **Performance optimized** - Powered by Rust and DuckDB.

## Components

Snapbase is available as a command-line tool and programming libraries:

### 🖥️ CLI Tool
Full-featured command-line interface for snapshot management and change detection.

**[📖 CLI Documentation](cli/README.md)**

### 🐍 Python Library
Python bindings for programmatic access to snapbase functionality.

**[📖 Python Documentation](python/README.md)**

### ☕ Java Library
Java API with native performance through JNI bindings.

**[📖 Java Documentation](java/README.md)**

## Quick Start

### CLI Installation
```bash
# From GitHub releases - Linux (x86_64)
curl -L https://github.com/peter-fm/snapbase/releases/latest/download/snapbase-linux-x86_64-v0.1.5 -o snapbase
chmod +x snapbase && sudo mv snapbase /usr/local/bin/

# From GitHub releases - Linux (ARM64)
curl -L https://github.com/peter-fm/snapbase/releases/latest/download/snapbase-linux-arm64-v0.1.5 -o snapbase
chmod +x snapbase && sudo mv snapbase /usr/local/bin/

# From GitHub releases - macOS (Apple Silicon)
curl -L https://github.com/peter-fm/snapbase/releases/latest/download/snapbase-macos-apple-silicon-v0.1.5 -o snapbase
chmod +x snapbase && sudo mv snapbase /usr/local/bin/

# From GitHub releases - macOS (Intel)
curl -L https://github.com/peter-fm/snapbase/releases/latest/download/snapbase-macos-intel-v0.1.5 -o snapbase
chmod +x snapbase && sudo mv snapbase /usr/local/bin/

# From GitHub releases - Windows
# Download snapbase-windows-x86_64-v0.1.5.exe from releases page

# From source
cargo install --git https://github.com/peter-fm/snapbase.git
```

### Python Installation (uv)
```bash
uv add "git+https://github.com/peter-fm/snapbase.git@main#subdirectory=python"
```

### Java Installation
```xml
<dependency>
    <groupId>com.snapbase</groupId>
    <artifactId>snapbase-java</artifactId>
    <version>0.1.5</version>
</dependency>
```

### Basic Usage

**CLI:**
```bash
# Initialize workspace
snapbase init
# Create snapshots
snapbase snapshot data.csv --name initial
# Later see if the file has changed
snapbase status data.csv
# Snapshot the new state
snapbase snapshot data.csv --name updated
# Explore back in time!
snapbase query "select * from data_csv where snapshot_name = 'initial'" 
# or
snapbase query "select * from data_csv" --snapshot initial
# Revert csv back
snapbase export data.csv --to initial --file data.csv --force
```

**Python:**
```python
import snapbase
# Initialize workspace
workspace = snapbase.Workspace("/path/to/workspace")
# Create snapshots
workspace.create_snapshot("data.csv", name="initial")
# Later see if the file has changed
changes = workspace.status("data.csv", baseline="initial")
# Snapshot the new state
workspace.create_snapshot("data.csv", name="updated")
# Explore back in time!
df = workspace.query("select * from data_csv where snapshot_name = 'initial'")
print(df)
workspace.export("data.csv", output_file="data.csv", to_snapshot="initial", force=True)
```

**Java:**
```java
import com.snapbase.SnapbaseWorkspace;

// Initialize workspace
try (SnapbaseWorkspace workspace = new SnapbaseWorkspace("/path/to/workspace")) {
    workspace.init();
    
    // Create snapshots
    workspace.createSnapshot("data.csv", "initial");
    
    // Later see if the file has changed
    String status = workspace.status("data.csv", "initial");
    System.out.println(status);
    
    // Snapshot the new state
    workspace.createSnapshot("data.csv", "updated");
    
    // Explore back in time!
    String results = workspace.query("select * from data_csv where snapshot_name = 'initial'");
    System.out.println(results);
    
    // Revert csv back
    workspace.export("data.csv", "data.csv", "initial", true);
}
```

## File Format Support

| Format | Read | Export | Notes |
|--------|------|--------|-------|
| Excel | ✅ | ✅ | Backup versions of excel data |
| CSV | ✅ | ✅ | Auto-detects delimiters and encoding |
| JSON | ✅ | ✅ | Flattens nested structures on export |
| Parquet | ✅ | ✅ | Native format for storage |
| SQL | ✅ | ✅ | Executes queries against databases |
| Database | ✅ | ✅ | MySQL, PostgreSQL, SQLite |

## Architecture

- **Core**: Rust library (`core/`) providing the main functionality
- **CLI**: Command-line interface (`cli/`) built on the core library
- **Python**: Python bindings (`python/`) using PyO3 for Rust integration
- **Java**: Java API (`java/`) with JNI bindings to the Rust core

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `cargo test` (core), `pytest` (python), or `mvn test` (java)
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

- 📖 [Documentation](https://github.com/peter-fm/snapbase/wiki)
- 🐛 [Report Issues](https://github.com/peter-fm/snapbase/issues)
- 💬 [Discussions](https://github.com/peter-fm/snapbase/discussions)