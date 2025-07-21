use std::fs;
use snapbase_core::data::{DataProcessor, BaselineData};
use snapbase_core::hash::ColumnInfo;
use snapbase_core::Result;
use tempfile::TempDir;

#[test]
fn test_flag_columns_exist_in_parquet_no_baseline() -> Result<()> {
    // Create a temporary directory for testing
    let temp_dir = TempDir::new()?;
    let test_csv = temp_dir.path().join("test.csv");
    let output_parquet = temp_dir.path().join("output.parquet");
    
    // Create a simple CSV file
    fs::write(&test_csv, "id,name,age\n1,Alice,25\n2,Bob,30\n3,Charlie,35")?;
    
    // Create data processor and load the CSV
    let mut data_processor = DataProcessor::new()?;
    let _data_info = data_processor.load_file(&test_csv)?;
    
    // Export with no baseline (first snapshot)
    data_processor.export_to_parquet_with_flags(&output_parquet, None)?;
    
    // Verify the parquet file was created
    assert!(output_parquet.exists(), "Parquet file should exist");
    
    // Check column names using the same method as manual tests
    let reader_processor = DataProcessor::new()?;
    let describe_sql = "SELECT column_name FROM information_schema.columns WHERE table_name = 'temp_parquet_view' ORDER BY ordinal_position".to_string();
    
    // Load the parquet file and create a view
    let load_sql = format!(
        "CREATE OR REPLACE VIEW temp_parquet_view AS SELECT * FROM read_parquet('{}')",
        output_parquet.to_string_lossy()
    );
    reader_processor.connection.execute(&load_sql, [])?;
    
    // Get column names
    let mut stmt = reader_processor.connection.prepare(&describe_sql)?;
    let mut columns = Vec::new();
    let rows = stmt.query_map([], |row| {
        let column_name: String = row.get(0)?;
        Ok(column_name)
    })?;
    
    for row in rows {
        columns.push(row?);
    }
    
    // Should have 6 columns: id, name, age, __snapbase_removed, __snapbase_added, __snapbase_modified
    println!("DEBUG: Found columns: {columns:?}");
    assert_eq!(columns.len(), 6, "Should have 6 columns including 3 flag columns, got: {columns:?}");
    assert_eq!(columns[0], "id");
    assert_eq!(columns[1], "name");
    assert_eq!(columns[2], "age");
    assert_eq!(columns[3], "__snapbase_removed");
    assert_eq!(columns[4], "__snapbase_added");
    assert_eq!(columns[5], "__snapbase_modified");
    
    // Check flag values - all should be added=true for first snapshot
    let data_sql = "SELECT __snapbase_removed, __snapbase_added, __snapbase_modified FROM temp_parquet_view".to_string();
    let mut stmt = reader_processor.connection.prepare(&data_sql)?;
    let flag_rows = stmt.query_map([], |row| {
        let removed: bool = row.get(0)?;
        let added: bool = row.get(1)?;
        let modified: bool = row.get(2)?;
        Ok((removed, added, modified))
    })?;
    
    // Check each row
    let mut row_count = 0;
    for row in flag_rows {
        let (removed, added, modified) = row?;
        row_count += 1;
        assert!(!removed, "Row {row_count} should not be removed");
        assert!(added, "Row {row_count} should be added");
        assert!(!modified, "Row {row_count} should not be modified");
    }
    
    assert_eq!(row_count, 3, "Should have 3 rows");
    
    Ok(())
}

#[test]
fn test_flag_columns_with_baseline_changes() -> Result<()> {
    // Create a temporary directory for testing
    let temp_dir = TempDir::new()?;
    let test_csv = temp_dir.path().join("test.csv");
    let output_parquet = temp_dir.path().join("output.parquet");
    
    // Create a CSV file with changes
    fs::write(&test_csv, "id,name,age\n1,Alice,26\n2,Bob,30\n4,David,40")?;
    
    // Create baseline data (simulating previous snapshot)
    let baseline_schema = vec![
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
            name: "age".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
        },
    ];
    
    let baseline_data = vec![
        vec!["1".to_string(), "Alice".to_string(), "25".to_string()],
        vec!["2".to_string(), "Bob".to_string(), "30".to_string()],
        vec!["3".to_string(), "Charlie".to_string(), "35".to_string()],
    ];
    
    let baseline = BaselineData {
        schema: baseline_schema,
        data: baseline_data,
    };
    
    // Create data processor and load the CSV
    let mut data_processor = DataProcessor::new()?;
    let _data_info = data_processor.load_file(&test_csv)?;
    
    // Export with baseline
    data_processor.export_to_parquet_with_flags(&output_parquet, Some(&baseline))?;
    
    // Verify the parquet file was created
    assert!(output_parquet.exists(), "Parquet file should exist");
    
    // Check that flag columns exist
    let reader_processor = DataProcessor::new()?;
    let load_sql = format!(
        "CREATE OR REPLACE VIEW temp_parquet_view AS SELECT * FROM read_parquet('{}')",
        output_parquet.to_string_lossy()
    );
    reader_processor.connection.execute(&load_sql, [])?;
    
    let describe_sql = "SELECT column_name FROM information_schema.columns WHERE table_name = 'temp_parquet_view' ORDER BY ordinal_position".to_string();
    
    let mut stmt = reader_processor.connection.prepare(&describe_sql)?;
    let mut columns = Vec::new();
    let rows = stmt.query_map([], |row| {
        let column_name: String = row.get(0)?;
        Ok(column_name)
    })?;
    
    for row in rows {
        columns.push(row?);
    }
    
    // Should have flag columns
    assert!(columns.contains(&"__snapbase_removed".to_string()), "Should have __snapbase_removed column");
    assert!(columns.contains(&"__snapbase_added".to_string()), "Should have __snapbase_added column");
    assert!(columns.contains(&"__snapbase_modified".to_string()), "Should have __snapbase_modified column");
    
    // Check flag values for expected changes
    let data_sql = "SELECT id, name, age, __snapbase_removed, __snapbase_added, __snapbase_modified FROM temp_parquet_view ORDER BY id".to_string();
    let mut stmt = reader_processor.connection.prepare(&data_sql)?;
    let flag_rows = stmt.query_map([], |row| {
        let id: i32 = row.get(0)?;
        let name: String = row.get(1)?;
        let age: i32 = row.get(2)?;
        let removed: bool = row.get(3)?;
        let added: bool = row.get(4)?;
        let modified: bool = row.get(5)?;
        Ok((id, name, age, removed, added, modified))
    })?;
    
    let mut found_rows = Vec::new();
    for row in flag_rows {
        found_rows.push(row?);
    }
    
    // Should have rows for current data + removed data
    assert!(found_rows.len() >= 3, "Should have at least 3 rows (current data)");
    
    // Check specific expected changes:
    // - Alice (id=1): age changed from 25 to 26 -> modified=true
    // - Bob (id=2): unchanged -> all flags false
    // - Charlie (id=3): removed from current -> removed=true
    // - David (id=4): added to current -> added=true
    
    let alice_row = found_rows.iter().find(|(id, _, _, _, _, _)| *id == 1);
    let bob_row = found_rows.iter().find(|(id, _, _, _, _, _)| *id == 2);
    let david_row = found_rows.iter().find(|(id, _, _, _, _, _)| *id == 4);
    
    if let Some((_, _, _, removed, added, modified)) = alice_row {
        assert!(!(*removed), "Alice should not be removed");
        assert!(!(*added), "Alice should not be added (existed before)");
        assert!(*modified, "Alice should be modified (age changed)");
    } else {
        panic!("Alice row not found");
    }
    
    if let Some((_, _, _, removed, added, modified)) = bob_row {
        assert!(!(*removed), "Bob should not be removed");
        assert!(!(*added), "Bob should not be added");
        assert!(!(*modified), "Bob should not be modified");
    } else {
        panic!("Bob row not found");
    }
    
    if let Some((_, _, _, removed, added, modified)) = david_row {
        assert!(!(*removed), "David should not be removed");
        assert!(*added, "David should be added");
        assert!(!(*modified), "David should not be modified");
    } else {
        panic!("David row not found");
    }
    
    Ok(())
}

#[test]
fn test_baseline_data_loading() -> Result<()> {
    // Create baseline data
    let baseline_schema = vec![
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
    ];
    
    let baseline_data = vec![
        vec!["1".to_string(), "Alice".to_string()],
        vec!["2".to_string(), "Bob".to_string()],
    ];
    
    let baseline = BaselineData {
        schema: baseline_schema,
        data: baseline_data,
    };
    
    // Create data processor and load baseline
    let mut data_processor = DataProcessor::new()?;
    data_processor.load_baseline_data(&baseline)?;
    
    // Query the baseline data to verify it was loaded correctly
    let mut stmt = data_processor.connection.prepare("SELECT COUNT(*) FROM baseline_data")?;
    let count: i64 = stmt.query_row([], |row| row.get(0))?;
    
    assert_eq!(count, 2);
    
    Ok(())
}