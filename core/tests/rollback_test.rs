use snapbase_core::hash::ColumnInfo;
use snapbase_core::Result;

#[test]
fn test_metadata_column_filtering() -> Result<()> {
    // Create schema with metadata columns mixed in
    let schema = vec![
        ColumnInfo {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
        },
        ColumnInfo {
            name: "name".to_string(),
            data_type: "TEXT".to_string(),
            nullable: true,
        },
        ColumnInfo {
            name: "__snapbase_removed".to_string(),
            data_type: "BOOLEAN".to_string(),
            nullable: false,
        },
        ColumnInfo {
            name: "__snapbase_added".to_string(),
            data_type: "BOOLEAN".to_string(),
            nullable: false,
        },
        ColumnInfo {
            name: "__snapbase_modified".to_string(),
            data_type: "BOOLEAN".to_string(),
            nullable: false,
        },
        ColumnInfo {
            name: "age".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
        },
        ColumnInfo {
            name: "snapshot_name".to_string(),
            data_type: "TEXT".to_string(),
            nullable: false,
        },
        ColumnInfo {
            name: "snapshot_timestamp".to_string(),
            data_type: "TEXT".to_string(),
            nullable: false,
        },
    ];
    
    // Test filtering metadata columns
    let original_columns: Vec<&ColumnInfo> = schema
        .iter()
        .filter(|col| {
            !col.name.starts_with("__snapbase_") && 
            col.name != "snapshot_name" && 
            col.name != "snapshot_timestamp"
        })
        .collect();
    
    // Should have only 3 original columns: id, name, age
    assert_eq!(original_columns.len(), 3, "Should have 3 original columns");
    
    let column_names: Vec<&str> = original_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(column_names, vec!["id", "name", "age"], "Should have original columns only");
    
    // Test that metadata columns are correctly identified
    let metadata_columns: Vec<&ColumnInfo> = schema
        .iter()
        .filter(|col| {
            col.name.starts_with("__snapbase_") || 
            col.name == "snapshot_name" || 
            col.name == "snapshot_timestamp"
        })
        .collect();
    
    assert_eq!(metadata_columns.len(), 5, "Should have 5 metadata columns");
    
    println!("✅ Metadata column filtering test passed");
    Ok(())
}