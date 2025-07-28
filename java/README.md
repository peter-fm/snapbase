# Snapbase Java API

Java bindings for the Snaphase - a queryable time machine for your structured data from entire databases and SQL query results to Excel, CSV, parquet and JSON files. Snapbase is a data version control system augmented by SQL. Supports both local and cloud snapshot storage.

## Features

🚀 **Zero-Copy Arrow Performance**: Ultra-fast querying with Apache Arrow integration
✨ **Snapshot-based tracking** - Create immutable snapshots of your data with metadata  
🔍 **Comprehensive change detection** - Detect schema changes, row additions/deletions, and cell-level modifications  
📊 **Multiple format support** - Databases, SQL queries, Excel, CSV, JSON and Parquet files  
☁️ **Cloud storage support** - Store snapshots locally, in S3, or S3 Express One Zone (Directory Buckets)  
📈 **SQL querying** - Query across snapshots using SQL to monitor changes at the cell level over time.  
⚡ **Performance optimized** - Powered by Rust and DuckDB.


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
    <version>latest</version>
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
    
    // Check status against baseline - returns structured ChangeDetectionResult
    ChangeDetectionResult result = workspace.status("data.csv", "v1");
    System.out.println("Schema changes: " + result.getSchemaChanges().hasChanges());
    System.out.println("Row changes: " + result.getRowChanges().hasChanges());
    System.out.println("Total changes: " + result.getRowChanges().getTotalChanges());
    
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
- `ChangeDetectionResult status(String filePath, String baseline)` - Check status against baseline returning structured result

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
- `ChangeDetectionResult diff(String source, String fromSnapshot, String toSnapshot)` - Compare snapshots returning structured result

#### Utilities
- `String getPath()` - Get workspace path
- `String stats()` - Get workspace statistics as JSON
- `JsonNode statsAsJson()` - Get workspace statistics as parsed JSON
- `void close()` - Close workspace and free resources

### SnapbaseException

Exception thrown by Snapbase operations when errors occur.

### Structured Result Objects

#### ChangeDetectionResult

Main result object returned by `status()` and `diff()` methods.

**Methods:**
- `SchemaChanges getSchemaChanges()` - Get schema-level changes
- `RowChanges getRowChanges()` - Get row-level changes

#### SchemaChanges

Contains schema-level changes between snapshots.

**Methods:**
- `boolean hasChanges()` - Returns true if any schema changes exist
- `ColumnOrderChange getColumnOrder()` - Get column order changes (nullable)
- `List<ColumnAddition> getColumnsAdded()` - Get list of added columns
- `List<ColumnRemoval> getColumnsRemoved()` - Get list of removed columns
- `List<ColumnRename> getColumnsRenamed()` - Get list of renamed columns
- `List<TypeChange> getTypeChanges()` - Get list of columns with changed data types

#### RowChanges

Contains row-level changes between snapshots.

**Methods:**
- `boolean hasChanges()` - Returns true if any row changes exist
- `int getTotalChanges()` - Returns total number of changed rows
- `List<RowModification> getModified()` - Get list of modified rows
- `List<RowAddition> getAdded()` - Get list of added rows
- `List<RowRemoval> getRemoved()` - Get list of removed rows

#### Change Detail Objects

##### ColumnAddition
- `String getName()` - Column name
- `String getDataType()` - Data type (e.g., "VARCHAR", "INTEGER")
- `int getPosition()` - Position in schema
- `boolean isNullable()` - Whether column allows null values
- `String getDefaultValue()` - Default value (if any, nullable)

##### ColumnRemoval
- `String getName()` - Column name
- `String getDataType()` - Data type
- `int getPosition()` - Position in schema
- `boolean isNullable()` - Whether column allowed null values

##### ColumnRename
- `String getFrom()` - Original column name
- `String getTo()` - New column name

##### TypeChange
- `String getColumn()` - Column name
- `String getFrom()` - Original data type
- `String getTo()` - New data type

##### RowModification
- `int getRowIndex()` - Index of the modified row
- `Map<String, CellChange> getChanges()` - Map of column names to cell changes

##### CellChange
- `String getBefore()` - Original cell value
- `String getAfter()` - New cell value

##### RowAddition
- `int getRowIndex()` - Index of the added row
- `Map<String, String> getData()` - Map of column names to values for the new row

##### RowRemoval
- `int getRowIndex()` - Index of the removed row
- `Map<String, String> getData()` - Map of column names to values for the removed row

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

### Change Detection and Analysis

Snapbase now returns **structured objects** instead of JSON strings, providing type safety and better IDE support:

```java
import com.snapbase.*;

// Create baseline snapshot
workspace.createSnapshot("customers.csv", "baseline");

// ... modify the file ...

// Check what changed against baseline - returns structured result
ChangeDetectionResult result = workspace.status("customers.csv", "baseline");

// Access schema changes with full type safety
SchemaChanges schemaChanges = result.getSchemaChanges();
if (schemaChanges.hasChanges()) {
    System.out.println("Schema Changes Detected:");
    
    // Check for added columns
    for (ColumnAddition addition : schemaChanges.getColumnsAdded()) {
        System.out.println("  + Added column: " + addition.getName() + 
                          " (" + addition.getDataType() + ")");
    }
    
    // Check for removed columns
    for (ColumnRemoval removal : schemaChanges.getColumnsRemoved()) {
        System.out.println("  - Removed column: " + removal.getName() + 
                          " (" + removal.getDataType() + ")");
    }
    
    // Check for column renames
    for (ColumnRename rename : schemaChanges.getColumnsRenamed()) {
        System.out.println("  ~ Renamed column: " + rename.getFrom() + 
                          " → " + rename.getTo());
    }
    
    // Check for type changes
    for (TypeChange typeChange : schemaChanges.getTypeChanges()) {
        System.out.println("  ⚠ Type changed: " + typeChange.getColumn() + 
                          " from " + typeChange.getFrom() + " to " + typeChange.getTo());
    }
}

// Access row changes with detailed information
RowChanges rowChanges = result.getRowChanges();
if (rowChanges.hasChanges()) {
    System.out.println("\nRow Changes: " + rowChanges.getTotalChanges() + " total changes");
    
    // Analyze row additions
    System.out.println("Added rows: " + rowChanges.getAdded().size());
    for (RowAddition addition : rowChanges.getAdded()) {
        System.out.println("  + Row " + addition.getRowIndex() + ": " + addition.getData());
    }
    
    // Analyze row deletions
    System.out.println("Removed rows: " + rowChanges.getRemoved().size());
    for (RowRemoval removal : rowChanges.getRemoved()) {
        System.out.println("  - Row " + removal.getRowIndex() + ": " + removal.getData());
    }
    
    // Analyze row modifications with cell-level details
    System.out.println("Modified rows: " + rowChanges.getModified().size());
    for (RowModification modification : rowChanges.getModified()) {
        System.out.println("  ~ Row " + modification.getRowIndex() + ":");
        for (Map.Entry<String, CellChange> entry : modification.getChanges().entrySet()) {
            CellChange cellChange = entry.getValue();
            System.out.println("    " + entry.getKey() + ": '" + 
                             cellChange.getBefore() + "' → '" + cellChange.getAfter() + "'");
        }
    }
}
```

### Comparing Two Snapshots

```java
// Compare two specific snapshots
ChangeDetectionResult result = workspace.diff("customers.csv", "baseline", "current");

// Same structured access as status()
System.out.println("Schema changes: " + result.getSchemaChanges().hasChanges());
System.out.println("Row changes: " + result.getRowChanges().getTotalChanges());

// Access all change details with type safety
for (RowModification modification : result.getRowChanges().getModified()) {
    System.out.println("Row " + modification.getRowIndex() + " changed:");
    for (Map.Entry<String, CellChange> entry : modification.getChanges().entrySet()) {
        CellChange change = entry.getValue();
        System.out.println("  " + entry.getKey() + ": " + 
                          change.getBefore() + " → " + change.getAfter());
    }
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

## Working with Query Results

Unlike Python where Snapbase returns Polars DataFrames, the Java API returns Apache Arrow `VectorSchemaRoot` objects for zero-copy performance. Here's how to manipulate these results to extract data into standard Java types.

### Basic Data Extraction from VectorSchemaRoot

```java
// Query returns VectorSchemaRoot - Apache Arrow's columnar format
try (VectorSchemaRoot result = workspace.query("employees.csv", "SELECT * FROM data")) {
    
    // Get individual columns as FieldVector
    FieldVector idColumn = result.getVector("id");
    FieldVector nameColumn = result.getVector("name");
    FieldVector salaryColumn = result.getVector("salary");
    
    // Extract values into Java types using getObject()
    for (int i = 0; i < result.getRowCount(); i++) {
        // Always check for null values first
        Integer id = idColumn.isNull(i) ? null : (Integer) idColumn.getObject(i);
        String name = nameColumn.isNull(i) ? null : (String) nameColumn.getObject(i);
        Long salary = salaryColumn.isNull(i) ? null : (Long) salaryColumn.getObject(i);
        
        System.out.printf("ID: %d, Name: %s, Salary: %d%n", id, name, salary);
    }
}
```

### Converting to Java Collections

Convert Arrow results to familiar Java data structures:

```java
// Convert entire result to List<Map<String, Object>> - similar to DataFrame rows
public static List<Map<String, Object>> toRowMaps(VectorSchemaRoot result) {
    List<Map<String, Object>> rows = new ArrayList<>();
    
    for (int i = 0; i < result.getRowCount(); i++) {
        Map<String, Object> row = new HashMap<>();
        
        // Iterate over all columns
        for (FieldVector vector : result.getFieldVectors()) {
            String columnName = vector.getField().getName();
            Object value = vector.isNull(i) ? null : vector.getObject(i);
            row.put(columnName, value);
        }
        rows.add(row);
    }
    return rows;
}

// Usage example
try (VectorSchemaRoot result = workspace.query("data.csv", "SELECT * FROM data")) {
    List<Map<String, Object>> rows = toRowMaps(result);
    
    // Now you can use standard Java collection operations
    for (Map<String, Object> row : rows) {
        System.out.println("Row: " + row);
    }
}
```

### Filtering Data

**Recommended: SQL-based filtering (most efficient)**
```java
// Let DuckDB do the heavy lifting - much faster than Java filtering
try (VectorSchemaRoot filtered = workspace.query("employees.csv", 
    "SELECT * FROM data WHERE salary > 70000 AND department = 'Engineering'")) {
    
    System.out.println("High-paid engineers: " + filtered.getRowCount());
    // Process filtered results...
}
```

**Alternative: Programmatic filtering after retrieval**
```java
// Convert to Java collections first, then filter
try (VectorSchemaRoot result = workspace.query("employees.csv", "SELECT * FROM data")) {
    List<Map<String, Object>> rows = toRowMaps(result);
    
    // Filter using Java Streams (similar to pandas/polars filtering)
    List<Map<String, Object>> highSalaryEmployees = rows.stream()
        .filter(row -> {
            Long salary = (Long) row.get("salary");
            return salary != null && salary > 70000L;
        })
        .collect(Collectors.toList());
    
    System.out.println("Filtered " + highSalaryEmployees.size() + " employees");
}
```

### Grouping and Aggregation

**Recommended: SQL-based aggregation**
```java
// Use SQL for grouping and statistics - leverages DuckDB's performance
try (VectorSchemaRoot grouped = workspace.query("employees.csv", 
    "SELECT department, " +
    "       COUNT(*) as employee_count, " +
    "       AVG(salary) as avg_salary, " +
    "       MIN(salary) as min_salary, " +
    "       MAX(salary) as max_salary " +
    "FROM data " +
    "GROUP BY department " +
    "ORDER BY avg_salary DESC")) {
    
    // Extract aggregated results
    FieldVector deptVector = grouped.getVector("department");
    FieldVector countVector = grouped.getVector("employee_count");
    FieldVector avgVector = grouped.getVector("avg_salary");
    FieldVector minVector = grouped.getVector("min_salary");
    FieldVector maxVector = grouped.getVector("max_salary");
    
    for (int i = 0; i < grouped.getRowCount(); i++) {
        String dept = (String) deptVector.getObject(i);
        Long count = (Long) countVector.getObject(i);
        Double avgSalary = (Double) avgVector.getObject(i);
        Long minSalary = (Long) minVector.getObject(i);
        Long maxSalary = (Long) maxVector.getObject(i);
        
        System.out.printf("Department: %s%n", dept);
        System.out.printf("  Employees: %d%n", count);
        System.out.printf("  Avg Salary: $%.2f%n", avgSalary);
        System.out.printf("  Salary Range: $%d - $%d%n", minSalary, maxSalary);
        System.out.println();
    }
}
```

**Alternative: Java-based grouping**
```java
// Group data using Java Streams (similar to pandas groupby)
try (VectorSchemaRoot result = workspace.query("employees.csv", "SELECT * FROM data")) {
    List<Map<String, Object>> rows = toRowMaps(result);
    
    // Group by department
    Map<String, List<Map<String, Object>>> groupedByDept = rows.stream()
        .collect(Collectors.groupingBy(row -> (String) row.get("department")));
    
    // Calculate statistics for each group
    for (Map.Entry<String, List<Map<String, Object>>> entry : groupedByDept.entrySet()) {
        String department = entry.getKey();
        List<Map<String, Object>> deptEmployees = entry.getValue();
        
        // Calculate average salary for this department
        double avgSalary = deptEmployees.stream()
            .mapToLong(emp -> (Long) emp.get("salary"))
            .average()
            .orElse(0.0);
        
        System.out.printf("Department %s: %d employees, avg salary: $%.2f%n", 
                         department, deptEmployees.size(), avgSalary);
    }
}
```

### Statistical Operations

**Calculate statistics via SQL (recommended)**
```java
// Get comprehensive statistics in a single query
try (VectorSchemaRoot stats = workspace.query("employees.csv", 
    "SELECT " +
    "    COUNT(*) as total_employees, " +
    "    AVG(salary) as mean_salary, " +
    "    MIN(salary) as min_salary, " +
    "    MAX(salary) as max_salary, " +
    "    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) as median_salary, " +
    "    STDDEV(salary) as salary_stddev " +
    "FROM data")) {
    
    if (stats.getRowCount() > 0) {
        Long total = (Long) stats.getVector("total_employees").getObject(0);
        Double mean = (Double) stats.getVector("mean_salary").getObject(0);
        Long min = (Long) stats.getVector("min_salary").getObject(0);
        Long max = (Long) stats.getVector("max_salary").getObject(0);
        Double median = (Double) stats.getVector("median_salary").getObject(0);
        Double stddev = (Double) stats.getVector("salary_stddev").getObject(0);
        
        System.out.println("=== Salary Statistics ===");
        System.out.printf("Total Employees: %d%n", total);
        System.out.printf("Mean Salary: $%.2f%n", mean);
        System.out.printf("Median Salary: $%.2f%n", median);
        System.out.printf("Salary Range: $%d - $%d%n", min, max);
        System.out.printf("Standard Deviation: $%.2f%n", stddev);
    }
}
```

### Advanced Data Manipulation Patterns

**Working with temporal data across snapshots:**
```java
// Analyze salary changes over time across different snapshots
try (VectorSchemaRoot changes = workspace.query("employees.csv", 
    "SELECT e1.name, e1.salary as old_salary, e2.salary as new_salary, " +
    "       (e2.salary - e1.salary) as salary_change " +
    "FROM data e1 " +
    "JOIN data e2 ON e1.id = e2.id " +
    "WHERE e1.snapshot_name = 'baseline' AND e2.snapshot_name = 'current' " +
    "AND e1.salary != e2.salary " +
    "ORDER BY salary_change DESC")) {
    
    System.out.println("=== Salary Changes ===");
    for (int i = 0; i < changes.getRowCount(); i++) {
        String name = (String) changes.getVector("name").getObject(i);
        Long oldSalary = (Long) changes.getVector("old_salary").getObject(i);
        Long newSalary = (Long) changes.getVector("new_salary").getObject(i);
        Long change = (Long) changes.getVector("salary_change").getObject(i);
        
        System.out.printf("%s: $%d → $%d (%+d)%n", name, oldSalary, newSalary, change);
    }
}
```

**Type-safe column extraction utility:**
```java
// Utility methods for type-safe data extraction
public static class ArrowUtils {
    
    public static List<String> getStringColumn(VectorSchemaRoot result, String columnName) {
        FieldVector vector = result.getVector(columnName);
        List<String> values = new ArrayList<>();
        
        for (int i = 0; i < vector.getValueCount(); i++) {
            values.add(vector.isNull(i) ? null : (String) vector.getObject(i));
        }
        return values;
    }
    
    public static List<Long> getLongColumn(VectorSchemaRoot result, String columnName) {
        FieldVector vector = result.getVector(columnName);
        List<Long> values = new ArrayList<>();
        
        for (int i = 0; i < vector.getValueCount(); i++) {
            values.add(vector.isNull(i) ? null : (Long) vector.getObject(i));
        }
        return values;
    }
    
    public static OptionalDouble getColumnAverage(VectorSchemaRoot result, String columnName) {
        return getLongColumn(result, columnName).stream()
            .filter(Objects::nonNull)
            .mapToLong(Long::longValue)
            .average();
    }
}

// Usage
try (VectorSchemaRoot result = workspace.query("data.csv", "SELECT * FROM data")) {
    List<String> names = ArrowUtils.getStringColumn(result, "name");
    List<Long> salaries = ArrowUtils.getLongColumn(result, "salary");
    OptionalDouble avgSalary = ArrowUtils.getColumnAverage(result, "salary");
    
    System.out.println("Names: " + names);
    System.out.println("Average salary: $" + avgSalary.orElse(0.0));
}
```

### Key Differences from Python/Polars

**What Java Has:**
- ✅ Zero-copy performance via Apache Arrow
- ✅ SQL-based filtering, grouping, and aggregation (very powerful)
- ✅ Type-safe column access via `FieldVector`
- ✅ Efficient temporal queries across snapshots
- ✅ Memory-efficient streaming for large datasets

**What Java Lacks (compared to Polars DataFrames):**
- ❌ No built-in DataFrame-like API with method chaining
- ❌ No operations like `.filter().group_by().agg()` chaining
- ❌ No built-in statistical methods on data structures
- ❌ No direct data visualization capabilities

### Performance Recommendations

1. **Use SQL for heavy lifting** - Let DuckDB handle filtering, grouping, and aggregation
2. **Extract only what you need** - Use `SELECT` to limit columns and `LIMIT` for row counts
3. **Stream large datasets** - Use batching with `LIMIT` and `OFFSET` for huge results
4. **Leverage Arrow's columnar format** - Access columns directly rather than converting to row-based formats when possible

```java
// ✅ Efficient: Use SQL for complex operations
try (VectorSchemaRoot result = workspace.query("huge_dataset.csv", 
    "SELECT department, AVG(salary) as avg_sal FROM data " +
    "WHERE hire_date > '2023-01-01' GROUP BY department")) {
    // Process aggregated results
}

// ❌ Inefficient: Load all data then process in Java
try (VectorSchemaRoot all = workspace.query("huge_dataset.csv", "SELECT * FROM data")) {
    List<Map<String, Object>> rows = toRowMaps(all); // Memory intensive!
    // Then filter/group in Java collections
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
[storage]
backend = "s3"

[storage.s3]
bucket = "my-bucket"
prefix = "snapshots/"
region = "us-west-2"
# access_key_id and secret_access_key can be set via environment variables
```

#### S3 Express One Zone (Directory Buckets)
```toml
[storage]
backend = "s3"

[storage.s3]
bucket = "my-express-bucket"
prefix = "data/"
region = "us-east-1"
use_express = true
availability_zone = "use1-az5"
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

# For S3 Express One Zone (Directory Buckets)
export SNAPBASE_S3_USE_EXPRESS=true
export SNAPBASE_S3_AVAILABILITY_ZONE=use1-az5

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