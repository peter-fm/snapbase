# Snapbase

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A snapshot-based structured data diff tool that detects schema, column-level, and cell-level changes between versions of structured datasets.

## Features

✨ **Snapshot-based tracking** - Create immutable snapshots of your data with metadata  
🔍 **Comprehensive change detection** - Detect schema changes, row additions/deletions, and cell-level modifications  
📊 **Multiple format support** - CSV, JSON, Parquet, and SQL files  
☁️ **Cloud storage support** - Store snapshots locally or in S3  
📈 **SQL querying** - Query historical snapshots using SQL  
⚡ **Performance optimized** - Streaming processing for large datasets  

## Components

Snapbase is available as a command-line tool and programming libraries:

### 🖥️ CLI Tool
Full-featured command-line interface for snapshot management and change detection.

**[📖 CLI Documentation](cli/README.md)**

### 🐍 Python Library
Python bindings for programmatic access to snapbase functionality.

**[📖 Python Documentation](python/snapbase/README.md)**

### ☕ Java Library
Java API with native performance through JNI bindings.

**[📖 Java Documentation](java/README.md)**

## Quick Start

### CLI Installation
```bash
# From GitHub releases
curl -L https://github.com/peter-fm/snapbase/releases/latest/download/snapbase-linux-x64 -o snapbase
chmod +x snapbase && sudo mv snapbase /usr/local/bin/

# From source
cargo install --git https://github.com/peter-fm/snapbase
```

### Python Installation
```bash
pip install snapbase
# or
uv add snapbase
```

### Java Installation
```xml
<dependency>
    <groupId>com.snapbase</groupId>
    <artifactId>snapbase-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Basic Usage

**CLI:**
```bash
# Initialize workspace
snapbase init

# Create snapshots
snapbase snapshot data.csv --name initial
snapbase snapshot data.csv --name updated

# Check changes
snapbase status data.csv
```

**Python:**
```python
import snapbase

# Initialize workspace
workspace = snapbase.Workspace("/path/to/workspace")

# Create snapshots
workspace.create_snapshot("data.csv", name="initial")
# Later after changes ...
workspace.create_snapshot("data.csv", name="updated")

# Detect changes
changes = workspace.detect_changes("data.csv", baseline="initial")
```

**Java:**
```java
import com.snapbase.SnapbaseWorkspace;

// Initialize workspace
try (SnapbaseWorkspace workspace = new SnapbaseWorkspace("/path/to/workspace")) {
    workspace.init();
    
    // Create snapshots
    workspace.createSnapshot("data.csv", "initial");
    // Later after changes ...
    workspace.createSnapshot("data.csv", "updated");
    
    // Detect changes
    String changes = workspace.detectChanges("data.csv", "initial");
}
```

## File Format Support

| Format | Read | Export | Notes |
|--------|------|--------|-------|
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