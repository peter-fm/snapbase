# Snapbase Java API

Java bindings for the Snapbase core library, providing snapshot-based structured data diff functionality.

## Features

- **🚀 Zero-Copy Arrow Performance**: Ultra-fast querying with Apache Arrow integration
- **Workspace Management**: Initialize and manage Snapbase workspaces
- **Snapshot Creation**: Create immutable snapshots of structured data files
- **Change Detection**: Detect schema, column, and row-level changes between data versions
- **Historical Querying**: Query historical snapshots using SQL with columnar data access
- **Async Support**: Asynchronous operations with CompletableFuture
- **Multiple Formats**: Support for CSV, JSON, Parquet, and SQL files
- **Storage Backends**: Local filesystem and S3 cloud storage
- **Memory Efficient**: Direct columnar data access without serialization overhead

## Requirements

- Java 11 or higher
- Maven 3.6 or higher
- **JVM Flags**: Add `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED --enable-native-access=ALL-UNNAMED` for Arrow memory access

## Installation

### Using Maven

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.snapbase</groupId>
    <artifactId>snapbase-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

**Required for Arrow support**: Add JVM arguments to your application:

```xml
<!-- In Maven Surefire for tests -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <configuration>
        <argLine>--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED --enable-native-access=ALL-UNNAMED</argLine>
    </configuration>
</plugin>
```

Or when running your application:
```bash
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED --enable-native-access=ALL-UNNAMED -jar your-app.jar
```

### Installing to Local Maven Repository

To install the JAR to your local Maven repository for use in other projects:

```bash
cd java
mvn clean install
```

This will:
1. Build the complete JAR with all dependencies and native libraries
2. Install it to your local Maven repository (`~/.m2/repository/`)
3. Make it available to any Maven project on your system

Once installed, you can use it in any Maven project with the dependency above. The JAR is self-contained and includes everything needed to run Snapbase.

## Quick Start

```java
import com.snapbase.SnapbaseWorkspace;
import com.snapbase.SnapbaseException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;

// Create and initialize workspace
try (SnapbaseWorkspace workspace = new SnapbaseWorkspace("/path/to/workspace")) {
    workspace.init();
    
    // Create a snapshot
    String result = workspace.createSnapshot("data.csv", "v1");
    System.out.println(result);
    
    // Check status against baseline
    String changes = workspace.status("data.csv", "v1");
    System.out.println(changes);
    
    // Query historical data with zero-copy Arrow performance
    try (VectorSchemaRoot result = workspace.query("data.csv", "SELECT * FROM data LIMIT 10")) {
        System.out.println("Rows: " + result.getRowCount());
        System.out.println("Columns: " + result.getFieldVectors().size());
        
        // Direct columnar access - super fast!
        FieldVector idColumn = result.getVector("id");
        if (idColumn != null) {
            System.out.println("ID column type: " + idColumn.getClass().getSimpleName());
        }
    }
}
```

## API Reference

### SnapbaseWorkspace

The main class for interacting with Snapbase functionality.

#### Constructor
- `SnapbaseWorkspace(String workspacePath)` - Create workspace at specified path
- `SnapbaseWorkspace(Path workspacePath)` - Create workspace at specified path

#### Core Methods
- `void init()` - Initialize workspace (creates config and directory structure)
- `String createSnapshot(String filePath)` - Create snapshot with auto-generated name
- `String createSnapshot(String filePath, String name)` - Create snapshot with specific name
- `CompletableFuture<String> createSnapshotAsync(String filePath, String name)` - Async snapshot creation

#### Status Checking
- `String status(String filePath, String baseline)` - Check status against baseline as JSON string
- `JsonNode statusAsJson(String filePath, String baseline)` - Check status against baseline as parsed JSON

#### Zero-Copy Arrow Querying
- `VectorSchemaRoot query(String source, String sql)` - Query snapshots returning Arrow data (zero-copy)
- `VectorSchemaRoot query(String source, String sql, Integer limit)` - Query with result limit (zero-copy)
- `int queryRowCount(String source, String sql)` - Get row count efficiently
- `FieldVector queryColumn(String source, String sql, String columnName)` - Access specific column data

#### Snapshot Management
- `List<String> listSnapshots()` - List all snapshots
- `List<String> listSnapshotsForSource(String sourcePath)` - List snapshots for specific source
- `boolean snapshotExists(String name)` - Check if snapshot exists

#### Comparison
- `String diff(String source, String fromSnapshot, String toSnapshot)` - Compare snapshots
- `JsonNode diffAsJson(String source, String fromSnapshot, String toSnapshot)` - Compare with parsed result

#### Utilities
- `String getPath()` - Get workspace path
- `String stats()` - Get workspace statistics as JSON
- `JsonNode statsAsJson()` - Get workspace statistics as parsed JSON
- `void close()` - Close workspace and free resources

### SnapbaseException

Exception thrown by Snapbase operations when errors occur.

## Examples

### Basic Usage

```java
import com.snapbase.SnapbaseWorkspace;

try (SnapbaseWorkspace workspace = new SnapbaseWorkspace("/my/workspace")) {
    // Initialize workspace
    workspace.init();
    
    // Create snapshot
    String result = workspace.createSnapshot("sales_data.csv", "monthly_snapshot");
    System.out.println(result);
    
    // List all snapshots
    List<String> snapshots = workspace.listSnapshots();
    snapshots.forEach(System.out::println);
}
```

### Status Checking

```java
// Create baseline snapshot
workspace.createSnapshot("customers.csv", "baseline");

// ... modify the file ...

// Check what changed against baseline
JsonNode changes = workspace.statusAsJson("customers.csv", "baseline");
if (changes.has("added_rows")) {
    System.out.println("Added rows: " + changes.get("added_rows").size());
}
if (changes.has("modified_rows")) {
    System.out.println("Modified rows: " + changes.get("modified_rows").size());
}
```

### Query Limits and Performance

```java
// Get all data (no limit)
try (VectorSchemaRoot allData = workspace.query("huge.csv", "SELECT * FROM data")) {
    System.out.println("All " + allData.getRowCount() + " rows loaded");
}

// Limit results for large datasets
try (VectorSchemaRoot sample = workspace.query("huge.csv", "SELECT * FROM data", 1000)) {
    System.out.println("First 1000 rows: " + sample.getRowCount());
}

// Streaming pattern for huge datasets
int batchSize = 10000;
int offset = 0;
while (true) {
    try (VectorSchemaRoot batch = workspace.query("huge.csv", 
            "SELECT * FROM data LIMIT " + batchSize + " OFFSET " + offset)) {
        
        if (batch.getRowCount() == 0) break;
        
        // Process batch efficiently
        processBatch(batch);
        offset += batchSize;
    }
}
```

### Zero-Copy Arrow Queries

```java
// Ultra-fast columnar data access with Apache Arrow
try (VectorSchemaRoot results = workspace.query("products.csv", 
        "SELECT * FROM data WHERE price > 100", 1000)) {
    
    System.out.println("Found " + results.getRowCount() + " expensive products");
    
    // Direct column access - zero serialization overhead!
    FieldVector priceColumn = results.getVector("price");
    FieldVector nameColumn = results.getVector("name");
    
    // Process data efficiently
    for (int i = 0; i < results.getRowCount(); i++) {
        // Access row data directly from Arrow vectors
        System.out.println("Product: " + nameColumn.getObject(i) + 
                          ", Price: " + priceColumn.getObject(i));
    }
}

// Get just row count efficiently
int totalProducts = workspace.queryRowCount("products.csv", "SELECT * FROM data");
System.out.println("Total products: " + totalProducts);

// Access single column data
try (FieldVector prices = workspace.queryColumn("products.csv", 
        "SELECT price FROM data WHERE category = 'Electronics'", "price")) {
    System.out.println("Electronics prices column has " + prices.getValueCount() + " values");
}
```

### Async Operations

```java
// Create snapshots asynchronously
CompletableFuture<String> future1 = workspace.createSnapshotAsync("file1.csv", "snap1");
CompletableFuture<String> future2 = workspace.createSnapshotAsync("file2.csv", "snap2");

// Wait for both to complete
CompletableFuture.allOf(future1, future2).join();

System.out.println("Snapshot 1: " + future1.get());
System.out.println("Snapshot 2: " + future2.get());
```

### Error Handling

```java
try (SnapbaseWorkspace workspace = new SnapbaseWorkspace("/workspace")) {
    workspace.init();
    workspace.createSnapshot("data.csv", "v1");
} catch (SnapbaseException e) {
    System.err.println("Snapbase operation failed: " + e.getMessage());
    e.printStackTrace();
}
```

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
```toml
[storage.Local]
path = ".snapbase"
```

#### S3 Storage
```toml
[storage.S3]
bucket = "my-bucket"
prefix = "snapshots/"
region = "us-west-2"
access_key_id = "your_access_key"    # Optional - can use env vars
secret_access_key = "your_secret"   # Optional - can use env vars
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

### File Format Support

| Format | Read | Export | Notes |
|--------|------|--------|-------|
| CSV | ✅ | ✅ | Auto-detects delimiters and encoding |
| JSON | ✅ | ✅ | Flattens nested structures on export |
| Parquet | ✅ | ✅ | Native format for storage |
| SQL | ✅ | ✅ | Executes queries against databases |
| Database | ✅ | ✅ | MySQL, PostgreSQL, SQLite via configuration |

## Building

To build the JAR with native libraries included:

```bash
cd java
mvn clean package
```

This will:
1. Compile the Rust JNI bindings
2. Compile the Java code
3. Run tests
4. Package everything into a JAR with the native library embedded

## Development

### Prerequisites

- Rust toolchain (for JNI bindings)
- Java 11+ and Maven
- Native library dependencies as per core snapbase requirements

### Building from Source

```bash
# Build Rust JNI bindings
cd java-bindings
cargo build --release --features jni

# Build Java components
cd ../java
mvn compile

# Run tests
mvn test

# Package JAR
mvn package
```

### Testing

```bash
mvn test
```

The test suite includes:
- Basic workspace operations
- Snapshot creation and management
- Change detection
- SQL querying
- Async operations
- Error handling
- Resource management

## Architecture

The Java API uses JNI (Java Native Interface) to call into the Rust core library:

1. **Java Layer**: Provides idiomatic Java API with proper exception handling
2. **JNI Layer**: Rust code that bridges Java and the core library
3. **Core Layer**: The main Snapbase functionality written in Rust

Key design principles:
- **Memory Safety**: Proper resource management with RAII patterns
- **Error Handling**: Rust errors are converted to Java exceptions
- **Async Support**: Native async operations bridged to Java CompletableFuture
- **Type Safety**: Strong typing throughout the API
- **Performance**: Direct native calls minimize overhead

## License

MIT License - see LICENSE file for details.