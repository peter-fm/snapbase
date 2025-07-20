//! SQL Query Engine for historical snapshot analysis

use crate::error::{Result, SnapbaseError};
use crate::workspace::SnapbaseWorkspace;
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use tempfile;
use arrow::record_batch::RecordBatch;
use arrow::compute::concat_batches;

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
        
        Ok(Self { connection, workspace })
    }
    
    /// Execute SQL query against source snapshots, returning Arrow RecordBatch for zero-copy performance
    pub fn query_arrow(&mut self, source_file: &str, sql: &str) -> Result<RecordBatch> {
        // Register Hive-partitioned view for the source
        self.register_source_view(source_file)?;
        
        // Use DuckDB's native Arrow interface for zero-copy performance
        let mut stmt = self.connection.prepare(sql)
            .map_err(|e| SnapbaseError::invalid_input(format!("Failed to prepare query: {e}")))?;
        
        let arrow_iterator = stmt.query_arrow([])
            .map_err(|e| SnapbaseError::invalid_input(format!("Query execution failed: {e}")))?;
        
        // Collect all record batches from the iterator
        let record_batches: Vec<RecordBatch> = arrow_iterator.collect();
        
        // If no batches, return an error
        if record_batches.is_empty() {
            return Err(SnapbaseError::invalid_input("Query returned no results".to_string()));
        }
        
        // If only one batch, return it directly
        if record_batches.len() == 1 {
            return Ok(record_batches.into_iter().next().unwrap());
        }
        
        // If multiple batches, concatenate them into a single RecordBatch
        let schema = record_batches[0].schema();
        let concatenated = concat_batches(&schema, &record_batches)
            .map_err(|e| SnapbaseError::invalid_input(format!("Failed to concatenate record batches: {e}")))?;
        
        Ok(concatenated)
    }
    
    /// Execute SQL query against source snapshots (legacy JSON format)
    pub fn query(&mut self, source_file: &str, sql: &str) -> Result<QueryResult> {
        // Register Hive-partitioned view for the source
        self.register_source_view(source_file)?;
        
        // Use CSV export approach to avoid prepared statements entirely
        // Query execution using CSV export to avoid prepared statement issues
        
        // Create temporary file for query results
        let temp_file = tempfile::NamedTempFile::new()
            .map_err(|e| SnapbaseError::invalid_input(format!("Failed to create temporary file: {e}")))?;
        let temp_csv_path = temp_file.path().to_string_lossy().to_string();
        
        // Close the file handle to prevent locking issues on Windows
        drop(temp_file);
        
        let export_sql = format!("COPY ({sql}) TO '{temp_csv_path}' (FORMAT CSV, HEADER true)");
        
        self.connection.execute(&export_sql, [])
            .map_err(|e| SnapbaseError::invalid_input(format!("Query execution failed: {e}")))?;
        
        // Read the CSV file back to get results
        let csv_content = std::fs::read_to_string(&temp_csv_path)
            .map_err(|e| SnapbaseError::invalid_input(format!("Failed to read query results: {e}")))?;
        
        // Parse the CSV content
        let mut lines = csv_content.lines();
        
        // Get column names from header
        let columns = if let Some(header_line) = lines.next() {
            header_line.split(',').map(|s| s.trim_matches('"').to_string()).collect()
        } else {
            return Err(SnapbaseError::invalid_input("Empty query result".to_string()));
        };
        
        // Parse data rows
        let mut rows = Vec::new();
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            
            let mut row = Vec::new();
            for field in line.split(',') {
                let cleaned_field = field.trim_matches('"');
                
                // Try to parse as different types
                let value = if cleaned_field == "NULL" || cleaned_field.is_empty() {
                    QueryValue::Null
                } else if let Ok(i) = cleaned_field.parse::<i64>() {
                    QueryValue::Integer(i)
                } else if let Ok(f) = cleaned_field.parse::<f64>() {
                    QueryValue::Float(f)
                } else if let Ok(b) = cleaned_field.parse::<bool>() {
                    QueryValue::Boolean(b)
                } else {
                    QueryValue::String(cleaned_field.to_string())
                };
                
                row.push(value);
            }
            rows.push(row);
        }
        
        Ok(QueryResult {
            columns,
            row_count: rows.len(),
            rows,
        })
    }
    
    /// Register a Hive-partitioned view for a source file
    fn register_source_view(&mut self, source_file: &str) -> Result<()> {
        // Use the new query engine to register the view
        crate::query_engine::register_hive_view(&self.connection, &self.workspace, source_file, "data")?;
        Ok(())
    }
    
    /// Get available snapshots for a source file
    pub fn list_snapshots(&self, source_file: &str) -> Result<Vec<SnapshotInfo>> {
        // Use the storage backend to list snapshots
        let rt = tokio::runtime::Runtime::new()?;
        let snapshot_metadata = rt.block_on(async {
            self.workspace.storage().list_snapshots(source_file).await
        })?;
        
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