//! Unified data export functionality using DuckDB COPY command
//! 
//! This module provides efficient export capabilities for snapshot data to various formats
//! including CSV, Parquet, JSON, and Excel using DuckDB's native COPY command.

use crate::error::{Result, SnapbaseError};
use crate::workspace::SnapbaseWorkspace;
use crate::query_engine;
use duckdb::Connection;
use std::path::Path;

/// Supported export formats
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    /// Comma-separated values format
    Csv,
    /// Apache Parquet columnar format
    Parquet,
    /// JSON format
    Json,
    /// Microsoft Excel format (XLSX)
    Excel,
}

impl ExportFormat {
    /// Get the DuckDB format string for COPY command
    pub fn duckdb_format(&self) -> &'static str {
        match self {
            ExportFormat::Csv => "CSV",
            ExportFormat::Parquet => "PARQUET", 
            ExportFormat::Json => "JSON",
            ExportFormat::Excel => "XLSX",
        }
    }

    /// Determine format from file extension
    pub fn from_extension(path: &Path) -> Result<Self> {
        let extension = path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_lowercase());
            
        match extension.as_deref() {
            Some("csv") => Ok(ExportFormat::Csv),
            Some("parquet") => Ok(ExportFormat::Parquet),
            Some("json") => Ok(ExportFormat::Json),
            Some("xlsx") => Ok(ExportFormat::Excel),
            Some(ext) => Err(SnapbaseError::invalid_input(format!("Unsupported file extension: {}", ext))),
            None => Err(SnapbaseError::invalid_input("No file extension provided")),
        }
    }
}

/// Export options for customizing output
#[derive(Debug, Clone)]
pub struct ExportOptions {
    /// Whether to include CSV header (only applies to CSV format)
    pub include_header: bool,
    /// CSV delimiter character (only applies to CSV format)
    pub delimiter: char,
    /// Whether to force overwrite existing files
    pub force: bool,
    /// Specific snapshot name to export
    pub snapshot_name: Option<String>,
    /// Date to export snapshot from
    pub snapshot_date: Option<String>,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            include_header: true,
            delimiter: ',',
            force: false,
            snapshot_name: None,
            snapshot_date: None,
        }
    }
}

/// Unified export engine using DuckDB COPY command
pub struct UnifiedExporter {
    connection: Connection,
    workspace: SnapbaseWorkspace,
}

impl UnifiedExporter {
    /// Create a new UnifiedExporter
    pub fn new(workspace: SnapbaseWorkspace) -> Result<Self> {
        let connection = query_engine::create_configured_connection(&workspace)?;
        Ok(Self { connection, workspace })
    }

    /// Export snapshot data to file using DuckDB COPY command
    pub fn export(
        &mut self,
        source_file: &str,
        output_path: &Path,
        options: ExportOptions,
    ) -> Result<()> {
        // Determine output format from file extension
        let format = ExportFormat::from_extension(output_path)?;
        
        // Check if output file exists and handle force option
        if output_path.exists() && !options.force {
            return Err(SnapbaseError::invalid_input(format!(
                "Output file already exists: {}. Use force option to overwrite.",
                output_path.display()
            )));
        }

        // Register the source as a view in DuckDB
        self.register_source_view(source_file, &options)?;

        // Build the COPY command
        let copy_command = self.build_copy_command(output_path, format, &options)?;

        // Execute the COPY command
        self.connection.execute(&copy_command, [])
            .map_err(|e| SnapbaseError::invalid_input(format!(
                "Export failed: {}", e
            )))?;

        Ok(())
    }

    /// Register source as a DuckDB view with appropriate snapshot filtering
    fn register_source_view(&mut self, source_file: &str, options: &ExportOptions) -> Result<()> {
        // Get the storage backend configuration
        let storage_config = self.workspace.config().clone();
        
        // Configure DuckDB for the storage backend (S3 or local)
        query_engine::configure_duckdb_for_storage(&self.connection, &storage_config)?;

        // Build the source path based on storage configuration
        let base_path = match &storage_config {
            crate::config::StorageConfig::S3 { bucket, prefix, .. } => {
                let prefix_part = if !prefix.is_empty() {
                    format!("{}/", prefix.trim_end_matches('/'))
                } else {
                    String::new()
                };
                format!("s3://{bucket}/{prefix_part}sources/{source_file}/**/*.parquet")
            }
            crate::config::StorageConfig::Local { path } => {
                format!("{}/sources/{source_file}/**/*.parquet", path.display())
            }
        };

        // Create the view with Hive-style partitioning
        let mut view_sql = format!(
            "CREATE OR REPLACE VIEW export_data AS SELECT * EXCLUDE (snapshot_name, snapshot_timestamp, __snapbase_added, __snapbase_modified) FROM read_parquet('{}')",
            base_path
        );

        // Add snapshot filtering if specified
        if let Some(snapshot_name) = &options.snapshot_name {
            view_sql = format!(
                "CREATE OR REPLACE VIEW export_data AS SELECT * EXCLUDE (snapshot_name, snapshot_timestamp, __snapbase_added, __snapbase_modified) FROM read_parquet('{}') WHERE snapshot_name = '{}'",
                base_path, snapshot_name
            );
        } else if let Some(snapshot_date) = &options.snapshot_date {
            // For date-based filtering, we'd need to parse the timestamp
            view_sql = format!(
                "CREATE OR REPLACE VIEW export_data AS SELECT * EXCLUDE (snapshot_name, snapshot_timestamp, __snapbase_added, __snapbase_modified) FROM read_parquet('{}') WHERE DATE(snapshot_timestamp) = '{}'",
                base_path, snapshot_date
            );
        }

        self.connection.execute(&view_sql, [])
            .map_err(|e| SnapbaseError::invalid_input(format!(
                "Failed to register source view: {}", e
            )))?;

        Ok(())
    }

    /// Build the DuckDB COPY command for the specified format and options
    fn build_copy_command(
        &self,
        output_path: &Path,
        format: ExportFormat,
        options: &ExportOptions,
    ) -> Result<String> {
        let path_str = output_path.to_string_lossy();
        
        match format {
            ExportFormat::Csv => {
                let header = if options.include_header { "true" } else { "false" };
                Ok(format!(
                    "COPY (SELECT * FROM export_data) TO '{}' (FORMAT CSV, HEADER {}, DELIMITER '{}')",
                    path_str, header, options.delimiter
                ))
            }
            ExportFormat::Parquet => {
                Ok(format!(
                    "COPY (SELECT * FROM export_data) TO '{}' (FORMAT PARQUET)",
                    path_str
                ))
            }
            ExportFormat::Json => {
                Ok(format!(
                    "COPY (SELECT * FROM export_data) TO '{}' (FORMAT JSON)",
                    path_str
                ))
            }
            ExportFormat::Excel => {
                // Note: Excel format might require additional DuckDB extensions
                Ok(format!(
                    "COPY (SELECT * FROM export_data) TO '{}' (FORMAT XLSX)",
                    path_str
                ))
            }
        }
    }

    /// Export with custom SQL query (for advanced use cases)
    pub fn export_query(
        &mut self,
        source_file: &str,
        query: &str,
        output_path: &Path,
        options: ExportOptions,
    ) -> Result<()> {
        let format = ExportFormat::from_extension(output_path)?;
        
        // Check if output file exists and handle force option
        if output_path.exists() && !options.force {
            return Err(SnapbaseError::invalid_input(format!(
                "Output file already exists: {}. Use force option to overwrite.",
                output_path.display()
            )));
        }

        // Register the source view
        self.register_source_view(source_file, &options)?;

        // Build COPY command with custom query
        let path_str = output_path.to_string_lossy();
        let copy_command = match format {
            ExportFormat::Csv => {
                let header = if options.include_header { "true" } else { "false" };
                format!(
                    "COPY ({}) TO '{}' (FORMAT CSV, HEADER {}, DELIMITER '{}')",
                    query, path_str, header, options.delimiter
                )
            }
            ExportFormat::Parquet => {
                format!("COPY ({}) TO '{}' (FORMAT PARQUET)", query, path_str)
            }
            ExportFormat::Json => {
                format!("COPY ({}) TO '{}' (FORMAT JSON)", query, path_str)
            }
            ExportFormat::Excel => {
                format!("COPY ({}) TO '{}' (FORMAT XLSX)", query, path_str)
            }
        };

        // Execute the COPY command
        self.connection.execute(&copy_command, [])
            .map_err(|e| SnapbaseError::invalid_input(format!(
                "Export query failed: {}", e
            )))?;

        Ok(())
    }
}

/// Convenience function to export snapshot data
pub fn export_snapshot(
    workspace: SnapbaseWorkspace,
    source_file: &str,
    output_path: &Path,
    options: ExportOptions,
) -> Result<()> {
    let mut exporter = UnifiedExporter::new(workspace)?;
    exporter.export(source_file, output_path, options)
}

/// Convenience function to export with custom query
pub fn export_snapshot_query(
    workspace: SnapbaseWorkspace,
    source_file: &str,
    query: &str,
    output_path: &Path,
    options: ExportOptions,
) -> Result<()> {
    let mut exporter = UnifiedExporter::new(workspace)?;
    exporter.export_query(source_file, query, output_path, options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_export_format_from_extension() {
        assert_eq!(
            ExportFormat::from_extension(&PathBuf::from("test.csv")).unwrap(),
            ExportFormat::Csv
        );
        assert_eq!(
            ExportFormat::from_extension(&PathBuf::from("test.parquet")).unwrap(),
            ExportFormat::Parquet
        );
        assert_eq!(
            ExportFormat::from_extension(&PathBuf::from("test.json")).unwrap(),
            ExportFormat::Json
        );
        assert_eq!(
            ExportFormat::from_extension(&PathBuf::from("test.xlsx")).unwrap(),
            ExportFormat::Excel
        );
    }

    #[test]
    fn test_duckdb_format() {
        assert_eq!(ExportFormat::Csv.duckdb_format(), "CSV");
        assert_eq!(ExportFormat::Parquet.duckdb_format(), "PARQUET");
        assert_eq!(ExportFormat::Json.duckdb_format(), "JSON");
        assert_eq!(ExportFormat::Excel.duckdb_format(), "XLSX");
    }

    #[test]
    fn test_export_options_default() {
        let options = ExportOptions::default();
        assert_eq!(options.include_header, true);
        assert_eq!(options.delimiter, ',');
        assert_eq!(options.force, false);
        assert_eq!(options.snapshot_name, None);
        assert_eq!(options.snapshot_date, None);
    }
}