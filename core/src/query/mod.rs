//! SQL Query Engine for historical snapshot analysis

use crate::error::{Result, SnapbaseError};
use crate::workspace::SnapbaseWorkspace;
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use duckdb::Connection;
use serde::{Deserialize, Serialize};

/// Query result structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<QueryValue>>,
    pub row_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

/// SQL Query Engine for snapshot analysis
pub struct SnapshotQueryEngine {
    connection: Connection,
    workspace: SnapbaseWorkspace,
}

impl SnapshotQueryEngine {
    pub fn new(workspace: SnapbaseWorkspace) -> Result<Self> {
        // Use the new query engine configuration
        let connection = crate::query_engine::create_configured_connection(&workspace)?;

        // Additional DuckDB settings for query engine
        connection.execute("SET enable_progress_bar=false", [])?;
        connection.execute("SET preserve_insertion_order=false", [])?;
        connection.execute("SET enable_object_cache=true", [])?;

        Ok(Self {
            connection,
            workspace,
        })
    }

    /// Execute SQL query against source snapshots, returning Arrow RecordBatch for zero-copy performance
    pub fn query_arrow(&mut self, source_file: &str, sql: &str) -> Result<RecordBatch> {
        // Register Hive-partitioned view for the source
        self.register_source_view(source_file)?;

        // Use DuckDB's native Arrow interface for zero-copy performance
        let mut stmt = self
            .connection
            .prepare(sql)
            .map_err(|e| SnapbaseError::invalid_input(format!("Failed to prepare query: {e}")))?;

        let arrow_iterator = stmt
            .query_arrow([])
            .map_err(|e| SnapbaseError::invalid_input(format!("Query execution failed: {e}")))?;

        // Collect all record batches from the iterator
        let record_batches: Vec<RecordBatch> = arrow_iterator.collect();

        // If no batches, return an error
        if record_batches.is_empty() {
            return Err(SnapbaseError::invalid_input(
                "Query returned no results".to_string(),
            ));
        }

        // If only one batch, return it directly
        if record_batches.len() == 1 {
            return Ok(record_batches.into_iter().next().unwrap());
        }

        // If multiple batches, concatenate them into a single RecordBatch
        let schema = record_batches[0].schema();
        let concatenated = concat_batches(&schema, &record_batches).map_err(|e| {
            SnapbaseError::invalid_input(format!("Failed to concatenate record batches: {e}"))
        })?;

        Ok(concatenated)
    }

    /// Execute SQL query against source snapshots using DESCRIBE approach
    pub fn query(&mut self, source_file: &str, sql: &str) -> Result<QueryResult> {
        // Register Hive-partitioned view for the source
        self.register_source_view(source_file)?;

        // Use the shared helper function for consistent query execution
        execute_query_with_describe(&self.connection, sql)
    }

    /// Register a Hive-partitioned view for a source file
    fn register_source_view(&mut self, source_file: &str) -> Result<()> {
        // Use the new query engine to register the view
        crate::query_engine::register_hive_view(
            &self.connection,
            &self.workspace,
            source_file,
            "data",
        )?;
        Ok(())
    }

    /// Get available snapshots for a source file
    pub fn list_snapshots(&self, source_file: &str) -> Result<Vec<SnapshotInfo>> {
        // Use the storage backend to list snapshots
        let rt = tokio::runtime::Runtime::new()?;
        let snapshot_metadata =
            rt.block_on(async { self.workspace.storage().list_snapshots(source_file).await })?;

        let mut snapshots = Vec::new();
        for metadata in snapshot_metadata {
            snapshots.push(SnapshotInfo {
                name: metadata.name,
                timestamp: metadata.created,
                source: source_file.to_string(),
            });
        }

        // Sort by timestamp
        snapshots.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        Ok(snapshots)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub name: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub source: String,
}

/// Execute a DuckDB query using the DESCRIBE approach for proper type handling
/// This avoids the temporary CSV file approach and provides native DuckDB type conversion
pub fn execute_query_with_describe(connection: &Connection, sql: &str) -> Result<QueryResult> {
    // Use DESCRIBE to get column information first
    let describe_sql = format!("DESCRIBE {sql}");
    let mut describe_stmt = connection
        .prepare(&describe_sql)
        .map_err(|e| SnapbaseError::invalid_input(format!("Failed to describe query: {e}")))?;

    let mut columns = Vec::new();
    let describe_rows = describe_stmt
        .query_map([], |row| {
            let column_name: String = row.get(0)?;
            Ok(column_name)
        })
        .map_err(|e| SnapbaseError::invalid_input(format!("Failed to get query schema: {e}")))?;

    for row_result in describe_rows {
        let column_name = row_result.map_err(|e| {
            SnapbaseError::invalid_input(format!("Failed to read column info: {e}"))
        })?;
        columns.push(column_name);
    }

    // Now execute the actual query
    let mut stmt = connection
        .prepare(sql)
        .map_err(|e| SnapbaseError::invalid_input(format!("Failed to prepare query: {e}")))?;

    let column_count = columns.len();
    let rows_result = stmt
        .query_map([], |row| {
            let mut row_values = Vec::new();
            for i in 0..column_count {
                let value = match row.get::<usize, duckdb::types::Value>(i) {
                    Ok(duckdb::types::Value::Null) => QueryValue::Null,
                    Ok(duckdb::types::Value::Boolean(b)) => QueryValue::Boolean(b),
                    Ok(duckdb::types::Value::TinyInt(i)) => QueryValue::Integer(i as i64),
                    Ok(duckdb::types::Value::SmallInt(i)) => QueryValue::Integer(i as i64),
                    Ok(duckdb::types::Value::Int(i)) => QueryValue::Integer(i as i64),
                    Ok(duckdb::types::Value::BigInt(i)) => QueryValue::Integer(i),
                    Ok(duckdb::types::Value::UTinyInt(i)) => QueryValue::Integer(i as i64),
                    Ok(duckdb::types::Value::USmallInt(i)) => QueryValue::Integer(i as i64),
                    Ok(duckdb::types::Value::UInt(i)) => QueryValue::Integer(i as i64),
                    Ok(duckdb::types::Value::UBigInt(i)) => QueryValue::Integer(i as i64),
                    Ok(duckdb::types::Value::HugeInt(i)) => {
                        // HugeInt is i128, try to convert to i64, or use string representation if too big
                        if let Ok(i64_val) = i.try_into() {
                            QueryValue::Integer(i64_val)
                        } else {
                            QueryValue::String(i.to_string())
                        }
                    }
                    Ok(duckdb::types::Value::Float(f)) => QueryValue::Float(f as f64),
                    Ok(duckdb::types::Value::Double(f)) => QueryValue::Float(f),
                    Ok(duckdb::types::Value::Text(s)) => QueryValue::String(s),
                    Ok(duckdb::types::Value::Blob(b)) => {
                        QueryValue::String(format!("BLOB({} bytes)", b.len()))
                    }
                    Ok(duckdb::types::Value::Date32(d)) => {
                        QueryValue::String(format!("Date({})", d))
                    }
                    Ok(duckdb::types::Value::Time64(t, _)) => {
                        QueryValue::String(format!("Time({:?})", t))
                    }
                    Ok(duckdb::types::Value::Timestamp(ts, _)) => {
                        QueryValue::String(format!("Timestamp({:?})", ts))
                    }
                    Ok(duckdb::types::Value::Interval {
                        months,
                        days,
                        nanos,
                    }) => QueryValue::String(format!(
                        "Interval({} months, {} days, {} nanos)",
                        months, days, nanos
                    )),
                    Ok(duckdb::types::Value::Decimal(d)) => QueryValue::String(d.to_string()),
                    Ok(duckdb::types::Value::Enum(s)) => QueryValue::String(s),
                    Ok(duckdb::types::Value::List(l)) => {
                        QueryValue::String(format!("List({} items)", l.len()))
                    }
                    Ok(duckdb::types::Value::Struct(s)) => {
                        QueryValue::String(format!("Struct({} fields)", s.iter().count()))
                    }
                    Ok(duckdb::types::Value::Map(m)) => {
                        QueryValue::String(format!("Map({} entries)", m.iter().count()))
                    }
                    Ok(duckdb::types::Value::Array(a)) => {
                        QueryValue::String(format!("Array({} items)", a.len()))
                    }
                    Ok(duckdb::types::Value::Union(u)) => {
                        QueryValue::String(format!("Union({:?})", u))
                    }
                    Err(_) => QueryValue::Null,
                };
                row_values.push(value);
            }
            Ok(row_values)
        })
        .map_err(|e| SnapbaseError::invalid_input(format!("Query execution failed: {e}")))?;

    // Collect all rows
    let mut rows = Vec::new();
    for row_result in rows_result {
        let row = row_result.map_err(|e| {
            SnapbaseError::invalid_input(format!("Failed to process query row: {e}"))
        })?;
        rows.push(row);
    }

    Ok(QueryResult {
        columns,
        row_count: rows.len(),
        rows,
    })
}
