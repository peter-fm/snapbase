//! Tests for the unified export functionality

use std::path::Path;
use tempfile::TempDir;
use snapbase_core::{
    UnifiedExporter, ExportOptions, ExportFormat,
    workspace::SnapbaseWorkspace
};

mod common;
use common::TestWorkspace;

/// Test basic export functionality without requiring full snapshots
#[test]
fn test_export_basic() {
    let test_workspace = TestWorkspace::new("local.toml");
    let _guard = test_workspace.change_to_workspace();
    
    // Initialize workspace
    let workspace = SnapbaseWorkspace::find_or_create(Some(test_workspace.path())).unwrap();
    
    // Test creating an exporter
    let exporter = UnifiedExporter::new(workspace.clone());
    assert!(exporter.is_ok(), "Should be able to create UnifiedExporter");
}

/// Test export format detection
#[test]
fn test_export_format_detection() {
    // Test various file extensions
    assert_eq!(ExportFormat::from_extension(Path::new("test.csv")).unwrap(), ExportFormat::Csv);
    assert_eq!(ExportFormat::from_extension(Path::new("test.parquet")).unwrap(), ExportFormat::Parquet);
    assert_eq!(ExportFormat::from_extension(Path::new("test.json")).unwrap(), ExportFormat::Json);
    assert_eq!(ExportFormat::from_extension(Path::new("test.xlsx")).unwrap(), ExportFormat::Excel);
    
    // Test unsupported extensions
    assert!(ExportFormat::from_extension(Path::new("test.txt")).is_err());
    assert!(ExportFormat::from_extension(Path::new("test")).is_err());
}

/// Test export options
#[test]
fn test_export_options() {
    let options = ExportOptions::default();
    assert_eq!(options.include_header, true);
    assert_eq!(options.delimiter, ',');
    assert_eq!(options.force, false);
    assert_eq!(options.snapshot_name, None);
    assert_eq!(options.snapshot_date, None);
    
    let custom_options = ExportOptions {
        include_header: false,
        delimiter: ';',
        force: true,
        snapshot_name: Some("test".to_string()),
        snapshot_date: Some("2024-01-01".to_string()),
    };
    
    assert_eq!(custom_options.include_header, false);
    assert_eq!(custom_options.delimiter, ';');
    assert_eq!(custom_options.force, true);
    assert_eq!(custom_options.snapshot_name, Some("test".to_string()));
    assert_eq!(custom_options.snapshot_date, Some("2024-01-01".to_string()));
}

/// Test error handling
#[test]
fn test_export_error_handling() {
    let test_workspace = TestWorkspace::new("local.toml");
    let _guard = test_workspace.change_to_workspace();
    
    // Initialize workspace
    let workspace = SnapbaseWorkspace::find_or_create(Some(test_workspace.path())).unwrap();
    
    let output_dir = TempDir::new().unwrap();
    let invalid_output = output_dir.path().join("export_test.invalid");
    
    let options = ExportOptions::default();
    
    // Test invalid format
    let format_error = ExportFormat::from_extension(&invalid_output);
    assert!(format_error.is_err());
    
    // Test export with non-existent source
    let mut exporter = UnifiedExporter::new(workspace.clone()).unwrap();
    let result = exporter.export("non_existent_source", &invalid_output, options);
    assert!(result.is_err());
}