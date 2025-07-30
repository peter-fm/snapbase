//! Python bindings for snapbase-core
//! 
//! This module provides Python bindings for the snapbase core library using PyO3.

use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::path::{Path, PathBuf};
use pyo3_arrow::PyRecordBatch;

use snapbase_core::{
    SnapbaseWorkspace, 
    Result as SnapbaseResult,
    change_detection::StreamingChangeDetector,
    resolver::SnapshotResolver,
    snapshot::SnapshotMetadata,
    query::execute_query_with_describe,
    query_engine::{create_configured_connection, register_workspace_source_views},
    naming::SnapshotNamer,
    config::get_snapshot_config_with_workspace,
    UnifiedExporter, ExportOptions, ExportFormat,
};


/// Python wrapper for SnapbaseWorkspace
#[pyclass]
pub struct Workspace {
    workspace: SnapbaseWorkspace,
}

#[pymethods]
impl Workspace {
    #[new]
    #[pyo3(signature = (workspace_path=None))]
    fn new(workspace_path: Option<&str>) -> PyResult<Self> {
        log::debug!("🚨 PYTHON WORKSPACE CONSTRUCTOR CALLED with path: {:?}", workspace_path);
        let workspace = if let Some(path_str) = workspace_path {
            let path = PathBuf::from(path_str);
            // Use create_at_path to avoid directory traversal when explicit path is provided
            SnapbaseWorkspace::create_at_path(&path)
        } else {
            // Use find_or_create for default behavior (current directory)
            SnapbaseWorkspace::find_or_create(None)
        }.map_err(|e| PyRuntimeError::new_err(format!("Failed to create workspace: {}", e)))?;
        
        Ok(Workspace { workspace })
    }

    /// Initialize a new workspace (creates config and directory structure)
    fn init(&mut self) -> PyResult<()> {
        self.workspace.create_config_with_force(false)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to initialize workspace: {}", e)))?;

        Ok(())
    }

    /// Create a snapshot of the given file
    #[pyo3(signature = (file_path, name=None))]
    fn create_snapshot(&mut self, file_path: &str, name: Option<&str>) -> PyResult<String> {
        // Debug: Print workspace and storage config info

        // Convert file path to absolute path
        let input_path = if Path::new(file_path).is_absolute() {
            PathBuf::from(file_path)
        } else {
            self.workspace.root().join(file_path)
        };
        
        // Generate snapshot name if not provided
        let snapshot_name = if let Some(name) = name {
            name.to_string()
        } else {
            // Get existing snapshots for this source to generate unique name
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;
            
            let canonical_path = input_path.canonicalize()
                .unwrap_or_else(|_| input_path.clone())
                .to_string_lossy()
                .to_string();
                
            let existing_snapshots = rt.block_on(async {
                let all_snapshots = self.workspace.storage().list_snapshots_for_all_sources().await?;
                Ok::<Vec<String>, snapbase_core::error::SnapbaseError>(
                    all_snapshots.get(&canonical_path).cloned().unwrap_or_default()
                )
            }).map_err(|e| PyRuntimeError::new_err(format!("Failed to list existing snapshots: {}", e)))?;

            // Use configured pattern to generate name
            let snapshot_config = get_snapshot_config_with_workspace(Some(self.workspace.root()))
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to get snapshot config: {}", e)))?;
            let namer = SnapshotNamer::new(snapshot_config.default_name_pattern);
            namer.generate_name(file_path, &existing_snapshots)
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to generate snapshot name: {}", e)))?
        };

        // Check if snapshot with this name already exists for this source
        let snapshot_exists = self.workspace.snapshot_exists_for_source(file_path, &snapshot_name)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to check existing snapshots: {}", e)))?;
        
        if snapshot_exists {
            return Err(PyRuntimeError::new_err(format!(
                "Snapshot '{}' already exists. Use a different name or remove the existing snapshot.", 
                snapshot_name
            )));
        }

        // Create the snapshot using the same logic as CLI
        // Extract just the filename for the source name to avoid path separators in view names
        let source_name = input_path.file_name()
            .and_then(|os_str| os_str.to_str())
            .unwrap_or(file_path);
        let metadata = create_hive_snapshot(&self.workspace, &input_path, source_name, &snapshot_name)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create snapshot: {}", e)))?;

        Ok(format!("Created snapshot '{}' with {} rows, {} columns", 
                  metadata.name, metadata.row_count, metadata.column_count))
    }

    /// Check status of current file against a baseline snapshot
    fn status(&self, file_path: &str, baseline: &str) -> PyResult<snapbase_core::change_detection::ChangeDetectionResult> {
        let resolver = SnapshotResolver::new(self.workspace.clone());
        
        // Convert file path to absolute path
        let input_path = if Path::new(file_path).is_absolute() {
            PathBuf::from(file_path)
        } else {
            self.workspace.root().join(file_path)
        };
        
        let _canonical_input_path = input_path.canonicalize()
            .unwrap_or_else(|_| input_path.clone())
            .to_string_lossy()
            .to_string();
        
        // Resolve baseline snapshot using source name (like CLI does)
        let baseline_snapshot = resolver.resolve_by_name_for_source(baseline, Some(file_path))
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to resolve baseline snapshot: {}", e)))?;
        
        // Create runtime for async operations
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;
        
        // Get baseline data path and create data sources for streaming comparison
        let data_path = baseline_snapshot.data_path.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Baseline snapshot has no data path"))?;
        let baseline_source = snapbase_core::change_detection::DataSource::StoredSnapshot {
            path: data_path.clone(),
            workspace: self.workspace.clone(),
        };
        let current_source = snapbase_core::change_detection::DataSource::File(input_path);
        
        // Configure comparison options
        let options = snapbase_core::change_detection::ComparisonOptions::default();
        
        // Perform streaming change detection
        let changes = rt.block_on(async {
            StreamingChangeDetector::compare_data_sources(
                baseline_source,
                current_source,
                options,
                None, // No progress callback for now
            ).await
        }).map_err(|e| PyRuntimeError::new_err(format!("Failed to detect changes: {}", e)))?;
        
        Ok(changes)
    }


    /// Query across all workspace sources using SQL, returning Polars DataFrame
    /// This enables cross-source joins and snapshot filtering
    #[pyo3(signature = (sql, snapshot_pattern="*", limit=None))]
    fn query(&self, sql: &str, snapshot_pattern: Option<&str>, limit: Option<usize>) -> PyResult<PyObject> {
        // Create DuckDB connection configured for the workspace storage backend
        let connection = create_configured_connection(&self.workspace)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create connection: {}", e)))?;

        // Use default pattern if not provided
        let pattern = snapshot_pattern.unwrap_or("*");

        // Register workspace-wide views
        let registered_views = register_workspace_source_views(&connection, &self.workspace, pattern)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to register workspace views: {}", e)))?;

        if registered_views.is_empty() {
            return Err(PyRuntimeError::new_err("No sources found in workspace"));
        }

        // Apply limit if specified
        let mut final_sql = sql.to_string();
        if let Some(limit_value) = limit {
            final_sql = format!("{final_sql} LIMIT {limit_value}");
        }

        // Execute query using the shared helper function
        let result = execute_query_with_describe(&connection, &final_sql)
            .map_err(|e| PyRuntimeError::new_err(format!("Workspace query failed: {}", e)))?;

        // Convert QueryResult to Arrow RecordBatch for Python consumption with proper types
        use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        // Determine column types by examining the first non-null value in each column
        let mut column_types: Vec<DataType> = vec![DataType::Utf8; result.columns.len()];
        
        for row in &result.rows {
            for (col_idx, value) in row.iter().enumerate() {
                if column_types[col_idx] == DataType::Utf8 {
                    // Only update if we haven't determined the type yet (still default string)
                    match value {
                        snapbase_core::query::QueryValue::Integer(_) => {
                            column_types[col_idx] = DataType::Int64;
                        }
                        snapbase_core::query::QueryValue::Float(_) => {
                            column_types[col_idx] = DataType::Float64;
                        }
                        snapbase_core::query::QueryValue::Boolean(_) => {
                            column_types[col_idx] = DataType::Boolean;
                        }
                        snapbase_core::query::QueryValue::String(_) => {
                            // Keep as Utf8 (default)
                        }
                        snapbase_core::query::QueryValue::Null => {
                            // Skip nulls when determining type
                        }
                    }
                }
            }
        }

        // Build schema with proper data types
        let fields: Vec<Field> = result.columns.iter()
            .zip(column_types.iter())
            .map(|(name, data_type)| Field::new(name, data_type.clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Create column builders for each column based on its determined type
        let mut column_data: Vec<Vec<snapbase_core::query::QueryValue>> = vec![vec![]; result.columns.len()];
        
        // Collect all values for each column
        for row in &result.rows {
            for (col_idx, value) in row.iter().enumerate() {
                column_data[col_idx].push(value.clone());
            }
        }

        // Build Arrow arrays based on column types
        let arrays: Vec<ArrayRef> = column_types.iter()
            .enumerate()
            .map(|(col_idx, col_type)| {
                let column_values = &column_data[col_idx];
                
                match col_type {
                    DataType::Int64 => {
                        let int_values: Vec<Option<i64>> = column_values.iter().map(|v| match v {
                            snapbase_core::query::QueryValue::Integer(i) => Some(*i),
                            snapbase_core::query::QueryValue::Null => None,
                            _ => None, // Should not happen if type detection worked
                        }).collect();
                        Arc::new(Int64Array::from(int_values)) as ArrayRef
                    }
                    DataType::Float64 => {
                        let float_values: Vec<Option<f64>> = column_values.iter().map(|v| match v {
                            snapbase_core::query::QueryValue::Float(f) => Some(*f),
                            snapbase_core::query::QueryValue::Null => None,
                            _ => None, // Should not happen if type detection worked
                        }).collect();
                        Arc::new(Float64Array::from(float_values)) as ArrayRef
                    }
                    DataType::Boolean => {
                        let bool_values: Vec<Option<bool>> = column_values.iter().map(|v| match v {
                            snapbase_core::query::QueryValue::Boolean(b) => Some(*b),
                            snapbase_core::query::QueryValue::Null => None,
                            _ => None, // Should not happen if type detection worked
                        }).collect();
                        Arc::new(BooleanArray::from(bool_values)) as ArrayRef
                    }
                    DataType::Utf8 => {
                        let string_values: Vec<Option<String>> = column_values.iter().map(|v| match v {
                            snapbase_core::query::QueryValue::String(s) => Some(s.clone()),
                            snapbase_core::query::QueryValue::Null => None,
                            _ => None, // Should not happen if type detection worked
                        }).collect();
                        Arc::new(StringArray::from(string_values)) as ArrayRef
                    }
                    _ => {
                        // Fallback to string for unknown types
                        let string_values: Vec<Option<String>> = column_values.iter().map(|v| match v {
                            snapbase_core::query::QueryValue::String(s) => Some(s.clone()),
                            snapbase_core::query::QueryValue::Integer(i) => Some(i.to_string()),
                            snapbase_core::query::QueryValue::Float(f) => Some(f.to_string()),
                            snapbase_core::query::QueryValue::Boolean(b) => Some(b.to_string()),
                            snapbase_core::query::QueryValue::Null => None,
                        }).collect();
                        Arc::new(StringArray::from(string_values)) as ArrayRef
                    }
                }
            })
            .collect();

        let arrow_result = RecordBatch::try_new(schema, arrays)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create Arrow batch: {}", e)))?;

        // Convert Arrow RecordBatch to PyArrow, then to Polars DataFrame
        Python::with_gil(|py| {
            // Create PyRecordBatch and convert to PyArrow
            let py_record_batch = PyRecordBatch::new(arrow_result);
            let pyarrow_batch = py_record_batch.to_pyarrow(py)?;
            
            // Import polars module
            let polars = py.import("polars")?;
            
            // Convert PyArrow RecordBatch to Polars DataFrame using from_arrow
            let polars_df = polars.call_method1("from_arrow", (pyarrow_batch,))?;
            
            Ok(polars_df.into())
        })
    }


    /// Get workspace path
    fn get_path(&self) -> PyResult<String> {
        Ok(self.workspace.root().to_string_lossy().to_string())
    }

    /// List all snapshots
    fn list_snapshots(&self) -> PyResult<Vec<String>> {
        self.workspace.list_snapshots()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to list snapshots: {}", e)))
    }

    /// List snapshots for a specific source
    fn list_snapshots_for_source(&self, source_path: &str) -> PyResult<Vec<String>> {
        self.workspace.list_snapshots_for_source(source_path)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to list snapshots for source: {}", e)))
    }

    /// Check if a snapshot exists
    fn snapshot_exists(&self, name: &str) -> PyResult<bool> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;
        
        let exists = rt.block_on(async {
            let all_snapshots = self.workspace.storage().list_all_snapshots().await?;
            Ok::<bool, snapbase_core::error::SnapbaseError>(
                all_snapshots.iter().any(|snapshot| snapshot.name == name)
            )
        }).map_err(|e| PyRuntimeError::new_err(format!("Failed to check snapshot existence: {}", e)))?;
        
        Ok(exists)
    }

    /// Get workspace statistics
    fn stats(&self) -> PyResult<String> {
        let stats = self.workspace.stats()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get workspace stats: {}", e)))?;
        
        let stats_json = serde_json::json!({
            "snapshot_count": stats.snapshot_count,
            "diff_count": stats.diff_count,
            "total_archive_size": stats.total_archive_size,
            "total_json_size": stats.total_json_size,
            "total_diff_size": stats.total_diff_size
        });
        
        Ok(serde_json::to_string_pretty(&stats_json)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to serialize stats: {}", e)))?)
    }

    /// Compare two snapshots
    fn diff(&self, source: &str, from_snapshot: &str, to_snapshot: &str) -> PyResult<snapbase_core::change_detection::ChangeDetectionResult> {
        let resolver = SnapshotResolver::new(self.workspace.clone());
        
        // Resolve both snapshots
        let from_resolved = resolver.resolve_by_name_for_source(from_snapshot, Some(source))
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to resolve from snapshot: {}", e)))?;
        let to_resolved = resolver.resolve_by_name_for_source(to_snapshot, Some(source))
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to resolve to snapshot: {}", e)))?;
        
        // Create data sources for streaming comparison
        let from_data_path = from_resolved.data_path.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("From snapshot has no data path"))?;
        let to_data_path = to_resolved.data_path.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("To snapshot has no data path"))?;
            
        let baseline_source = snapbase_core::change_detection::DataSource::StoredSnapshot {
            path: from_data_path.clone(),
            workspace: self.workspace.clone(),
        };
        let current_source = snapbase_core::change_detection::DataSource::StoredSnapshot {
            path: to_data_path.clone(),
            workspace: self.workspace.clone(),
        };
        
        // Configure comparison options
        let options = snapbase_core::change_detection::ComparisonOptions::default();
        
        // Perform streaming change detection
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create runtime: {}", e)))?;
            
        let changes = rt.block_on(async {
            StreamingChangeDetector::compare_data_sources(
                baseline_source,
                current_source,
                options,
                None, // No progress callback for now
            ).await
        }).map_err(|e| PyRuntimeError::new_err(format!("Failed to detect changes: {}", e)))?;
        
        Ok(changes)
    }

    /// Export snapshot data to a file using unified export functionality
    #[pyo3(signature = (source, output_file, to_snapshot, force=false))]
    fn export(&self, source: &str, output_file: &str, to_snapshot: &str, force: bool) -> PyResult<String> {
        // Build export options
        let options = ExportOptions {
            include_header: true,
            delimiter: ',',
            force,
            snapshot_name: Some(to_snapshot.to_string()),
            snapshot_date: None,
        };

        let output_path = Path::new(output_file);
        
        // Determine output format for reporting
        let export_format = ExportFormat::from_extension(output_path)
            .map_err(|e| PyRuntimeError::new_err(format!("Invalid output format: {}", e)))?;

        // Use the unified exporter
        let mut exporter = UnifiedExporter::new(self.workspace.clone())
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create exporter: {}", e)))?;
        
        exporter.export(source, output_path, options)
            .map_err(|e| PyRuntimeError::new_err(format!("Export failed: {}", e)))?;

        Ok(format!("Exported snapshot '{}' from '{}' to '{}' ({:?} format)", 
                  to_snapshot, source, output_file, export_format))
    }

    /// Get configuration resolution information for debugging
    fn get_config_info(&self) -> PyResult<String> {
        use snapbase_core::config::get_config_resolution_info;
        
        let resolution_info = get_config_resolution_info(Some(self.workspace.root()))
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to get config info: {}", e)))?;
        
        let info_json = serde_json::json!({
            "config_source": resolution_info.config_source,
            "config_path": resolution_info.config_path,
            "workspace_path": resolution_info.workspace_path,
            "resolution_order": resolution_info.resolution_order
        });
        
        Ok(serde_json::to_string_pretty(&info_json)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to serialize config info: {}", e)))?)
    }
}


/// Convert SnapbaseResult to PyResult
fn _convert_result<T>(result: SnapbaseResult<T>) -> PyResult<T> {
    result.map_err(|e| PyRuntimeError::new_err(format!("{}", e)))
}

/// Create a Hive snapshot (adapted from CLI implementation)
fn create_hive_snapshot(
    workspace: &SnapbaseWorkspace,
    input_path: &Path,
    source_name: &str,
    snapshot_name: &str,
) -> SnapbaseResult<SnapshotMetadata> {
    use snapbase_core::data::DataProcessor;
    use snapbase_core::path_utils;
    use chrono::Utc;

    // Debug: Print storage backend info

    // Create timestamp
    let timestamp = Utc::now();
    let timestamp_str = timestamp.format("%Y%m%dT%H%M%S%.6fZ").to_string();
    
    // Create Hive directory structure path
    let hive_path_str = path_utils::join_for_storage_backend(&[
        "sources",
        source_name,
        &format!("snapshot_name={snapshot_name}"),
        &format!("snapshot_timestamp={timestamp_str}")
    ], workspace.storage());
    
    log::debug!("🔍 DEBUG: Hive path: {}", hive_path_str);
    
    // Use async runtime to handle storage backend operations
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        log::debug!("🔍 DEBUG: Ensuring directory: {}", hive_path_str);
        workspace.storage().ensure_directory(&hive_path_str).await
    })?;
    
    // Process data with workspace-configured processor
    let mut processor = DataProcessor::new_with_workspace(workspace)?;
    let data_info = processor.load_file(input_path)?;
    
    // Create Parquet file using DuckDB COPY
    let parquet_relative_path = format!("{hive_path_str}/data.parquet");
    let parquet_path = workspace.storage().get_duckdb_path(&parquet_relative_path);
    
    log::debug!("🔍 DEBUG: Parquet relative path: {}", parquet_relative_path);
    log::debug!("🔍 DEBUG: DuckDB parquet path: {}", parquet_path);
    
    // Export to Parquet using the same method as CLI
    let temp_path = std::path::Path::new(&parquet_path);
    log::debug!("🔍 DEBUG: Exporting to parquet: {}", temp_path.display());
    processor.export_to_parquet(temp_path)?;
    
    // Create metadata
    let metadata = SnapshotMetadata {
        format_version: "1.0.0".to_string(),
        name: snapshot_name.to_string(),
        created: timestamp,
        source: input_path.to_string_lossy().to_string(),
        row_count: data_info.row_count,
        column_count: data_info.columns.len(),
        columns: data_info.columns.clone(),
        archive_size: None,
        parent_snapshot: None,
        sequence_number: 0,
        delta_from_parent: None,
        can_reconstruct_parent: false,
        source_path: Some(input_path.to_string_lossy().to_string()),
    };
    
    let metadata_json = serde_json::to_string_pretty(&metadata)?;
    let metadata_path = format!("{hive_path_str}/metadata.json");
    
    // Write metadata using storage backend
    let metadata_bytes = metadata_json.as_bytes();
    rt.block_on(async {
        log::debug!("🔍 DEBUG: Writing metadata to: {}", metadata_path);
        workspace.storage().write_file(&metadata_path, metadata_bytes).await
    })?;
    
    Ok(metadata)
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<Workspace>()?;
    
    // Add change detection result types
    m.add_class::<snapbase_core::change_detection::ChangeDetectionResult>()?;
    m.add_class::<snapbase_core::change_detection::SchemaChanges>()?;
    m.add_class::<snapbase_core::change_detection::RowChanges>()?;
    m.add_class::<snapbase_core::change_detection::ColumnOrderChange>()?;
    m.add_class::<snapbase_core::change_detection::ColumnAddition>()?;
    m.add_class::<snapbase_core::change_detection::ColumnRemoval>()?;
    m.add_class::<snapbase_core::change_detection::ColumnRename>()?;
    m.add_class::<snapbase_core::change_detection::TypeChange>()?;
    m.add_class::<snapbase_core::change_detection::RowModification>()?;
    m.add_class::<snapbase_core::change_detection::CellChange>()?;
    m.add_class::<snapbase_core::change_detection::RowAddition>()?;
    m.add_class::<snapbase_core::change_detection::RowRemoval>()?;
    
    Ok(())
}
