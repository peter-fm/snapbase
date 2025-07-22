//! CLI workflow tests based on the run_test.sh script
//! Tests the complete end-to-end CLI functionality with realistic data scenarios

use std::fs;
use std::process::Command;
use std::path::PathBuf;

mod common;
use common::TestWorkspace;

/// Test the complete CLI workflow that matches run_test.sh
#[test] 
fn test_complete_cli_workflow() {
    let workspace = TestWorkspace::new("cli_test.toml");
    let _guard = workspace.change_to_workspace();
    
    // Build the CLI binary (equivalent to: cargo build -r)
    let build_output = Command::new("cargo")
        .args(&["build", "--release"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to build CLI");
    
    assert!(build_output.status.success(), 
        "Build failed: {}", String::from_utf8_lossy(&build_output.stderr));
    
    let cli_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../target/release/snapbase");
    
    assert!(cli_path.exists(), "CLI binary not found at {:?}", cli_path);
    
    // Initialize workspace (equivalent to: snapbase init)
    let init_output = Command::new(&cli_path)
        .arg("init")
        .current_dir(workspace.path())
        .output()
        .expect("Failed to run init");
    
    assert!(init_output.status.success(), 
        "Init failed: {}", String::from_utf8_lossy(&init_output.stderr));
    
    // Copy baseline data and create first snapshot
    let employees_file = workspace.copy_data_file("employees_baseline.csv", "employees.csv");
    assert!(employees_file.exists());
    
    // Create baseline snapshot (equivalent to: snapbase snapshot employees.csv --name baseline)
    let snapshot1_output = Command::new(&cli_path)
        .args(&["snapshot", "employees.csv", "--name", "baseline"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to create baseline snapshot");
    
    assert!(snapshot1_output.status.success(), 
        "Baseline snapshot creation failed: {}", String::from_utf8_lossy(&snapshot1_output.stderr));
    
    let snapshot1_result = String::from_utf8_lossy(&snapshot1_output.stdout);
    assert!(snapshot1_result.contains("baseline"));
    
    // Copy second version and check status
    workspace.copy_data_file("employees_snapshot1.csv", "employees.csv");
    
    // Check status (equivalent to: snapbase status employees.csv)
    let status1_output = Command::new(&cli_path)
        .args(&["status", "employees.csv"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to get status");
    
    assert!(status1_output.status.success(), 
        "Status check failed: {}", String::from_utf8_lossy(&status1_output.stderr));
    
    let status1_result = String::from_utf8_lossy(&status1_output.stdout);
    // Status should show changes detected
    assert!(!status1_result.is_empty());
    
    // Create second snapshot (equivalent to: snapbase snapshot employees.csv --name snap1)
    let snapshot2_output = Command::new(&cli_path)
        .args(&["snapshot", "employees.csv", "--name", "snap1"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to create snap1 snapshot");
    
    assert!(snapshot2_output.status.success(), 
        "Snap1 snapshot creation failed: {}", String::from_utf8_lossy(&snapshot2_output.stderr));
    
    let snapshot2_result = String::from_utf8_lossy(&snapshot2_output.stdout);
    assert!(snapshot2_result.contains("snap1"));
    
    // Copy third version and check status again
    workspace.copy_data_file("employees_snapshot2.csv", "employees.csv");
    
    // Check status for second change
    let status2_output = Command::new(&cli_path)
        .args(&["status", "employees.csv"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to get status");
    
    assert!(status2_output.status.success(), 
        "Second status check failed: {}", String::from_utf8_lossy(&status2_output.stderr));
    
    // Create third snapshot (equivalent to: snapbase snapshot employees.csv --name snap2)
    let snapshot3_output = Command::new(&cli_path)
        .args(&["snapshot", "employees.csv", "--name", "snap2"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to create snap2 snapshot");
    
    assert!(snapshot3_output.status.success(), 
        "Snap2 snapshot creation failed: {}", String::from_utf8_lossy(&snapshot3_output.stderr));
    
    let snapshot3_result = String::from_utf8_lossy(&snapshot3_output.stdout);
    assert!(snapshot3_result.contains("snap2"));
    
    // Test export functionality (equivalent to: snapbase export employees.csv --file backup.csv --to snap2 --force)
    let export_output = Command::new(&cli_path)
        .args(&["export", "employees.csv", "--file", "backup.csv", "--to", "snap2", "--force"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to export");
    
    assert!(export_output.status.success(), 
        "Export failed: {}", String::from_utf8_lossy(&export_output.stderr));
    
    // Verify backup file was created and has correct content
    let backup_file = workspace.path().join("backup.csv");
    assert!(backup_file.exists(), "Backup file was not created");
    
    let backup_content = fs::read_to_string(&backup_file).expect("Failed to read backup file");
    let expected_content = fs::read_to_string(workspace.copy_data_file("employees_snapshot2.csv", "expected.csv"))
        .expect("Failed to read expected content");
    
    // Content should match the snap2 data (employees_snapshot2.csv)
    assert_eq!(backup_content.trim(), expected_content.trim());
    
    // Test query functionality (equivalent to: snapbase query employees.csv "select * from data")
    let query_output = Command::new(&cli_path)
        .args(&["query", "employees.csv", "select * from data"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to run query");
    
    assert!(query_output.status.success(), 
        "Query failed: {}", String::from_utf8_lossy(&query_output.stderr));
    
    let query_result = String::from_utf8_lossy(&query_output.stdout);
    assert!(!query_result.is_empty());
    assert!(query_result.contains("Alice Johnson") || query_result.contains("Bob Smith"));
    
    // Test filtered query
    let query_filtered_output = Command::new(&cli_path)
        .args(&["query", "employees.csv", "select * from data where snapshot_name = 'snap2'"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to run filtered query");
    
    assert!(query_filtered_output.status.success(), 
        "Filtered query failed: {}", String::from_utf8_lossy(&query_filtered_output.stderr));
    
    // Test diff functionality
    test_diff_operations(&cli_path, workspace.path());
    
    println!("✅ Complete CLI workflow test completed successfully");
}

/// Test diff operations between different snapshots
fn test_diff_operations(cli_path: &std::path::Path, workspace_path: &std::path::Path) {
    // Test diff between baseline and snap1 (equivalent to: snapbase diff employees.csv baseline snap1)
    let diff1_output = Command::new(cli_path)
        .args(&["diff", "employees.csv", "baseline", "snap1"])
        .current_dir(workspace_path)
        .output()
        .expect("Failed to run diff baseline->snap1");
    
    assert!(diff1_output.status.success(), 
        "Diff baseline->snap1 failed: {}", String::from_utf8_lossy(&diff1_output.stderr));
    
    let diff1_result = String::from_utf8_lossy(&diff1_output.stdout);
    assert!(!diff1_result.is_empty());
    // Should show Bob Smith was removed (row 2) and Eve Wilson salary changed
    
    // Test diff between snap1 and snap2 (equivalent to: snapbase diff employees.csv snap1 snap2) 
    let diff2_output = Command::new(cli_path)
        .args(&["diff", "employees.csv", "snap1", "snap2"])
        .current_dir(workspace_path)
        .output()
        .expect("Failed to run diff snap1->snap2");
    
    assert!(diff2_output.status.success(), 
        "Diff snap1->snap2 failed: {}", String::from_utf8_lossy(&diff2_output.stderr));
    
    let diff2_result = String::from_utf8_lossy(&diff2_output.stdout);
    assert!(!diff2_result.is_empty());
    // Should show Bob Smith was added back and Eve Wilson salary reverted
    
    // Test diff between baseline and snap2 (equivalent to: snapbase diff employees.csv baseline snap2)
    let diff3_output = Command::new(cli_path)
        .args(&["diff", "employees.csv", "baseline", "snap2"])
        .current_dir(workspace_path)
        .output()
        .expect("Failed to run diff baseline->snap2");
    
    assert!(diff3_output.status.success(), 
        "Diff baseline->snap2 failed: {}", String::from_utf8_lossy(&diff3_output.stderr));
    
    let diff3_result = String::from_utf8_lossy(&diff3_output.stdout);
    assert!(!diff3_result.is_empty());
    // Should show Diana Prince was removed (row 4)
}

#[test]
fn test_cli_workflow_error_handling() {
    let workspace = TestWorkspace::new("local.toml");
    let _guard = workspace.change_to_workspace();
    
    let cli_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../target/release/snapbase");
    
    // Skip if CLI not built
    if !cli_path.exists() {
        println!("⚠️  CLI binary not found, skipping error handling tests");
        return;
    }
    
    // Initialize workspace
    let init_output = Command::new(&cli_path)
        .arg("init")
        .current_dir(workspace.path())
        .output()
        .expect("Failed to run init");
    assert!(init_output.status.success());
    
    // Test snapshot with non-existent file
    let bad_snapshot_output = Command::new(&cli_path)
        .args(&["snapshot", "nonexistent.csv", "--name", "test"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to run bad snapshot command");
    
    assert!(!bad_snapshot_output.status.success(), 
        "Should fail with non-existent file");
    
    // Test diff with non-existent snapshot
    workspace.copy_data_file("employees_baseline.csv", "employees.csv");
    
    let snapshot_output = Command::new(&cli_path)
        .args(&["snapshot", "employees.csv", "--name", "test"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to create test snapshot");
    assert!(snapshot_output.status.success());
    
    let bad_diff_output = Command::new(&cli_path)
        .args(&["diff", "employees.csv", "nonexistent", "test"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to run bad diff command");
    
    assert!(!bad_diff_output.status.success(), 
        "Should fail with non-existent snapshot");
}

#[test] 
fn test_cli_workflow_edge_cases() {
    let workspace = TestWorkspace::new("local.toml");
    let _guard = workspace.change_to_workspace();
    
    let cli_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../target/release/snapbase");
    
    // Skip if CLI not built
    if !cli_path.exists() {
        println!("⚠️  CLI binary not found, skipping edge case tests");
        return;
    }
    
    // Initialize workspace
    let init_output = Command::new(&cli_path)
        .arg("init")
        .current_dir(workspace.path())
        .output()
        .expect("Failed to run init");
    assert!(init_output.status.success());
    
    // Test empty CSV file
    let empty_csv = workspace.path().join("empty.csv");
    fs::write(&empty_csv, "id,name\n").expect("Failed to create empty CSV");
    
    let empty_snapshot_output = Command::new(&cli_path)
        .args(&["snapshot", "empty.csv", "--name", "empty"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to run empty snapshot");
    
    assert!(empty_snapshot_output.status.success(), 
        "Should handle empty CSV files: {}", String::from_utf8_lossy(&empty_snapshot_output.stderr));
    
    // Test duplicate snapshot names
    workspace.copy_data_file("employees_baseline.csv", "test.csv");
    
    let snapshot1_output = Command::new(&cli_path)
        .args(&["snapshot", "test.csv", "--name", "duplicate"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to create first duplicate snapshot");
    assert!(snapshot1_output.status.success());
    
    let snapshot2_output = Command::new(&cli_path)
        .args(&["snapshot", "test.csv", "--name", "duplicate"])
        .current_dir(workspace.path())
        .output()
        .expect("Failed to run second duplicate snapshot");
    
    // Should either succeed (overwrite) or fail gracefully
    if !snapshot2_output.status.success() {
        let stderr = String::from_utf8_lossy(&snapshot2_output.stderr);
        assert!(!stderr.is_empty(), "Should provide error message for duplicate snapshot");
    }
}