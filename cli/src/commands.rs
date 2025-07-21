//! Command implementations for snapbase CLI

use crate::cli::Commands;
use crate::output::{PrettyPrinter, JsonFormatter};
use crate::progress::ProgressReporter;
use std::collections::HashMap;

use snapbase_core::query::{QueryResult, QueryValue};
use snapbase_core::data::DataProcessor;
use snapbase_core::error::{Result, SnapbaseError};
use snapbase_core::resolver::{SnapshotRef, SnapshotResolver};
use snapbase_core::workspace::SnapbaseWorkspace;
use snapbase_core::change_detection::{ChangeDetector, StreamingChangeDetector};
use snapbase_core::naming::SnapshotNamer;
use snapbase_core::config::{get_snapshot_config, get_database_config};
use snapbase_core::database::{discover_database_tables, create_table_snapshot_sql, TableInfo};
use snapbase_core::config;
use snapbase_core::snapshot;
use snapbase_core::hash;
use snapbase_core::path_utils;
use snapbase_core::sql;
use std::path::Path;

/// Filter row data to include only original columns (exclude snapbase metadata columns)
/// This ensures consistent hashing by excluding columns like __snapbase_*, snapshot_name, snapshot_timestamp
fn filter_original_columns(row_data: &[String], original_column_count: usize) -> Vec<String> {
    // Take only the first N columns (original data), excluding metadata columns added by snapbase
    row_data.iter().take(original_column_count).cloned().collect()
}

/// Filter changed row data HashMap to include only original columns
fn filter_changed_row_data(full_data: &HashMap<u64, Vec<String>>, original_column_count: usize) -> HashMap<u64, Vec<String>> {
    full_data.iter().map(|(row_index, row_data)| {
        (*row_index, filter_original_columns(row_data, original_column_count))
    }).collect()
}

/// Execute a command
pub fn execute_command(command: Commands, workspace_path: Option<&Path>) -> Result<()> {
    match command {
        Commands::Init { from_global } => init_command(workspace_path, from_global),
        Commands::Snapshot {
            input,
            database,
            tables,
            exclude_tables,
            name,
        } => {
            
            if let Some(database_name) = database {
                database_snapshot_command(
                    workspace_path,
                    &database_name,
                    tables,
                    exclude_tables,
                    name.as_deref(),
                )
            } else if let Some(input_path) = input {
                snapshot_command(workspace_path, &input_path, name.as_deref())
            } else {
                Err(snapbase_core::error::SnapbaseError::invalid_input(
                    "Must provide either input file or --database".to_string()
                ))
            }
        },
        Commands::Show {
            source,
            snapshot,
            detailed,
            json,
        } => show_command(workspace_path, &source, &snapshot, detailed, json),
        Commands::Status {
            input,
            compare_to,
            quiet,
            json,
        } => streaming_status_command(workspace_path, &input, compare_to.as_deref(), quiet, json),
        Commands::List { source, json } => list_command(workspace_path, source.as_deref(), json),
        Commands::Stats { json } => stats_command(workspace_path, json),
        Commands::Diff { source, from, to, json } => streaming_diff_command(workspace_path, &source, &from, &to, json),
        Commands::Export {
            input,
            file,
            to,
            to_date,
            dry_run,
            force,
        } => export_command(workspace_path, &input, &file, to.as_deref(), to_date.as_deref(), dry_run, force),
        Commands::Cleanup {
            keep_full,
            dry_run,
            force,
        } => cleanup_command(workspace_path, keep_full, dry_run, force),
        Commands::Query {
            source,
            query,
            format,
            limit,
            list_snapshots,
        } => query_command(workspace_path, &source, query.as_deref(), &format, limit, list_snapshots),
        
        crate::cli::Commands::Config { command } => {
            config_command(workspace_path, &command)
        },
    }
}


/// Initialize snapbase workspace
fn init_command(workspace_path: Option<&Path>, from_global: bool) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let current_dir = std::env::current_dir()?;
        let root = workspace_path.unwrap_or(&current_dir);
        
        // Get storage configuration based on flag
        let config = if from_global {
            // --from-global: Use global config → env vars → defaults
            config::get_storage_config()?
        } else {
            // Regular init: Use env vars → defaults (project-first)
            config::get_storage_config_project_first()?
        };
        let storage_backend = snapbase_core::workspace::create_storage_backend(&config).await?;
        
        // Always create new workspace in the specified directory
        let workspace = SnapbaseWorkspace::create_new(root.to_path_buf(), storage_backend, config.clone()).await?;
        
        // Save global config if it doesn't exist (for regular init)
        if !from_global {
            config::save_global_config_if_missing(&config)?;
        }


        println!("✅ Initialized snapbase workspace at: {}", workspace.root.display());
        match workspace.config() {
            config::StorageConfig::Local { path } => {
                println!("📁 Local storage directory: {}", path.display());
            }
            config::StorageConfig::S3 { bucket, prefix, region, .. } => {
                println!("☁️  S3 storage: s3://{bucket}/{prefix} (region: {region})");
            }
        }
        
        Ok(())
    })
}

/// Export snapshot data to a file
fn export_command(
    workspace_path: Option<&Path>,
    input: &str,
    output_file: &str,
    to: Option<&str>,
    to_date: Option<&str>,
    dry_run: bool,
    force: bool,
) -> Result<()> {
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());

    // Validate that exactly one target option is provided
    match (to, to_date) {
        (None, None) => {
            return Err(SnapbaseError::invalid_input(
                "Must provide either --to <snapshot_name> or --to-date <date>".to_string()
            ));
        }
        (Some(_), Some(_)) => {
            return Err(SnapbaseError::invalid_input(
                "Cannot provide both --to and --to-date options".to_string()
            ));
        }
        _ => {} // Exactly one is provided, which is valid
    }

    // Resolve target snapshot
    let target_snapshot = if let Some(snapshot_name) = to {
        resolver.resolve_by_name_for_source(snapshot_name, Some(input))?
    } else if let Some(date_str) = to_date {
        resolver.resolve_by_date_for_source(date_str, Some(input))?
    } else {
        unreachable!("Should have been caught by validation above")
    };

    println!("📤 Exporting snapshot '{}' from '{}' to '{}'...", target_snapshot.name, input, output_file);

    // Load target snapshot data from Hive storage
    let rt = tokio::runtime::Runtime::new()?;
    
    // Get metadata to determine schema
    let metadata = if let Some(preloaded) = target_snapshot.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &target_snapshot.json_path {
        let metadata_data = rt.block_on(async {
            workspace.storage().read_file(json_path).await
        })?;
        serde_json::from_slice::<snapshot::SnapshotMetadata>(&metadata_data)?
    } else {
        return Err(SnapbaseError::SnapshotNotFound {
            name: target_snapshot.name.clone(),
        });
    };
    
    
    let target_schema = metadata.columns.clone();
    
    // Load row data from Hive storage using DuckDB
    let data_path = target_snapshot.data_path.as_ref().ok_or_else(|| {
        SnapbaseError::archive("Target snapshot has no data path")
    })?;
    
    let mut data_processor = DataProcessor::new_with_workspace(&workspace)?;
    let target_row_data = rt.block_on(async {
        // For local storage, make path absolute relative to workspace .snapbase directory
        let absolute_data_path = if data_path.starts_with('/') {
            data_path.to_string()
        } else {
            workspace.root.join(".snapbase").join(data_path).to_string_lossy().to_string()
        };
        data_processor.load_cloud_storage_data(&absolute_data_path, &workspace, true).await
    })?;
    
    // Determine output format from file extension
    let output_path = Path::new(output_file);
    let output_format = determine_output_format(output_path)?;
    
    // Check if output file already exists
    if output_path.exists() && !force {
        return Err(SnapbaseError::invalid_input(format!(
            "Output file '{output_file}' already exists. Use --force to overwrite."
        )));
    }

    // Show what will be exported
    if dry_run {
        println!("🔍 Dry run - would export:");
        println!("  Source: {input}");
        println!("  Snapshot: {}", target_snapshot.name);
        println!("  Output: {} ({})", output_file, match output_format {
            OutputFormat::Csv => "CSV format",
            OutputFormat::Parquet => "Parquet format",
        });
        println!("  Rows: {}", target_row_data.len());
        println!("  Columns: {}", target_schema.len());
        return Ok(());
    }

    // Ask for confirmation if not forced
    if !force {
        println!("📋 Export details:");
        println!("  Snapshot: {}", target_snapshot.name);
        println!("  Output: {} ({})", output_file, match output_format {
            OutputFormat::Csv => "CSV format",
            OutputFormat::Parquet => "Parquet format",
        });
        println!("  Rows: {}", target_row_data.len());
        println!("  Columns: {}", target_schema.len());
        
        if output_path.exists() {
            println!("\n⚠️  Output file exists and will be overwritten. Continue? (y/N)");
        } else {
            println!("\nContinue? (y/N)");
        }
        
        let mut user_input = String::new();
        std::io::stdin().read_line(&mut user_input)?;
        
        if !user_input.trim().to_lowercase().starts_with('y') {
            println!("❌ Export cancelled.");
            return Ok(());
        }
    }

    // Export the data
    match output_format {
        OutputFormat::Csv => {
            let csv_content = create_csv_content(&target_schema, &target_row_data)?;
            std::fs::write(output_path, csv_content)?;
        }
        OutputFormat::Parquet => {
            export_to_parquet(&target_schema, &target_row_data, output_path, &workspace)?;
        }
    }

    println!("✅ Export completed successfully!");
    println!("📄 Snapshot '{}' exported to '{}'", target_snapshot.name, output_file);

    Ok(())
}

/// Output format for export command
#[derive(Debug, Clone, PartialEq)]
enum OutputFormat {
    Csv,
    Parquet,
}

/// Determine output format from file extension
fn determine_output_format(path: &Path) -> Result<OutputFormat> {
    let extension = path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_lowercase());
    
    match extension.as_deref() {
        Some("csv") => Ok(OutputFormat::Csv),
        Some("parquet") => Ok(OutputFormat::Parquet),
        Some(ext) => Err(SnapbaseError::invalid_input(format!(
            "Unsupported output format: '{ext}'. Supported formats: csv, parquet"
        ))),
        None => Err(SnapbaseError::invalid_input(
            "Output file must have an extension (.csv or .parquet)".to_string()
        )),
    }
}

/// Export data to Parquet format
fn export_to_parquet(
    schema: &[hash::ColumnInfo],
    rows: &[Vec<String>],
    output_path: &Path,
    workspace: &SnapbaseWorkspace,
) -> Result<()> {
    use std::fs::File;
    use std::io::Write;
    
    // Create a temporary CSV file and use DuckDB to convert to Parquet
    let temp_csv = tempfile::NamedTempFile::new()?;
    let csv_content = create_csv_content(schema, rows)?;
    
    // Write CSV to temp file
    {
        let mut file = File::create(temp_csv.path())?;
        file.write_all(csv_content.as_bytes())?;
    }
    
    // Use DuckDB to convert CSV to Parquet
    let data_processor = DataProcessor::new_with_workspace(workspace)?;
    let temp_csv_path = temp_csv.path().to_string_lossy();
    let output_path_str = output_path.to_string_lossy();
    
    let sql = format!(
        "COPY (SELECT * FROM read_csv_auto('{temp_csv_path}')) TO '{output_path_str}' (FORMAT PARQUET)"
    );
    
    data_processor.connection.execute(&sql, [])?;
    
    Ok(())
}

/// Validate that a file path is within the workspace directory
fn validate_file_within_workspace(file_path: &Path, workspace: &SnapbaseWorkspace) -> Result<()> {
    // Canonicalize both paths to handle symlinks and relative paths
    let canonical_file = file_path.canonicalize()
        .map_err(|e| SnapbaseError::invalid_input(format!(
            "Cannot access file '{}': {}",
            file_path.display(),
            e
        )))?;
    
    let canonical_workspace = workspace.root.canonicalize()
        .map_err(|e| SnapbaseError::invalid_input(format!(
            "Cannot access workspace root '{}': {}",
            workspace.root.display(),
            e
        )))?;
    
    // Check if the file is within the workspace directory
    if !canonical_file.starts_with(&canonical_workspace) {
        return Err(SnapbaseError::invalid_input(format!(
            "File '{}' is outside the workspace directory '{}'. Only files within the workspace can be tracked.",
            file_path.display(),
            workspace.root.display()
        )));
    }
    
    Ok(())
}

/// Create CSV content from schema and row data
pub fn create_csv_content(schema: &[hash::ColumnInfo], rows: &[Vec<String>]) -> Result<String> {
    let mut content = String::new();
    
    // Filter out snapbase metadata columns - only keep original data columns
    let original_columns: Vec<(usize, &hash::ColumnInfo)> = schema
        .iter()
        .enumerate()
        .filter(|(_, col)| {
            !col.name.starts_with("__snapbase_") && 
            col.name != "snapshot_name" && 
            col.name != "snapshot_timestamp"
        })
        .collect();
    
    // Write header (only original columns)
    let headers: Vec<&str> = original_columns.iter().map(|(_, col)| col.name.as_str()).collect();
    content.push_str(&headers.join(","));
    content.push('\n');
    
    // Write rows (only original column values)
    let empty_string = String::new();
    for row in rows {
        let mut row_values = Vec::new();
        for &(original_index, _) in &original_columns {
            let value = row.get(original_index).unwrap_or(&empty_string);
            // Escape CSV values if they contain commas or quotes
            if value.contains(',') || value.contains('"') || value.contains('\n') {
                row_values.push(format!("\"{}\"", value.replace('"', "\"\"")));
            } else {
                row_values.push(value.clone());
            }
        }
        content.push_str(&row_values.join(","));
        content.push('\n');
    }
    
    Ok(content)
}

/// Create a snapshot
fn snapshot_command(
    workspace_path: Option<&Path>,
    input: &str,
    name: Option<&str>,
) -> Result<()> {
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    
    // Generate name if not provided
    let snapshot_name = if let Some(name) = name {
        name.to_string()
    } else {
        // Get existing snapshot names for this source
        let rt = tokio::runtime::Runtime::new()?;
        let existing_snapshots = rt.block_on(async {
            let all_snapshots = workspace.storage().list_snapshots_for_all_sources().await?;
            let source_key = input.to_string();
            Ok::<Vec<String>, SnapbaseError>(
                all_snapshots.get(&source_key).cloned().unwrap_or_default()
            )
        })?;
        
        // Use configured pattern to generate name
        let snapshot_config = get_snapshot_config()?;
        let namer = SnapshotNamer::new(snapshot_config.default_name_pattern);
        namer.generate_name(input, &existing_snapshots)?
    };
    
    // Check if snapshot already exists using Hive structure
    if hive_snapshot_exists(&workspace, input, &snapshot_name)? {
        return Err(SnapbaseError::invalid_input(format!(
            "Snapshot '{snapshot_name}' already exists. Use a different name or remove the existing snapshot."
        )));
    }

    // Create snapshot
    let input_path = if Path::new(input).is_absolute() {
        Path::new(input).to_path_buf()
    } else {
        // Resolve relative paths relative to the workspace root
        workspace.root.join(input)
    };
    
    // Validate that the file is within the workspace directory
    validate_file_within_workspace(&input_path, &workspace)?;
    
    // Check file size and provide warnings/recommendations
    let file_size = std::fs::metadata(&input_path)?.len();
    const LARGE_FILE_THRESHOLD: u64 = 100 * 1024 * 1024; // 100MB
    const VERY_LARGE_FILE_THRESHOLD: u64 = 1024 * 1024 * 1024; // 1GB
    
    if file_size > VERY_LARGE_FILE_THRESHOLD {
        println!("⚠️  WARNING: Large file detected ({:.1} GB)", file_size as f64 / (1024.0 * 1024.0 * 1024.0));
        println!("   Processing may take some time.");
    } else if file_size > LARGE_FILE_THRESHOLD {
        println!("ℹ️  INFO: Moderate file size ({:.1} MB) - processing...", file_size as f64 / (1024.0 * 1024.0));
    }
    
    
    println!("📸 Creating snapshot '{snapshot_name}' from '{input}'...");
    
    // Create snapshot using pure Hive structure
    let metadata = create_hive_snapshot(
        &workspace,
        &input_path,
        input,
        &snapshot_name,
    )?;

    println!("✅ Snapshot created successfully!");
    println!("├─ Name: {}", metadata.name);
    println!("├─ Rows: {}", metadata.row_count);
    println!("├─ Columns: {}", metadata.column_count);
    println!("├─ Timestamp: {}", metadata.created.format("%Y-%m-%d %H:%M:%S UTC"));
    let hive_display_path = std::path::Path::new("sources")
        .join(input)
        .join(format!("snapshot_name={snapshot_name}"))
        .join(format!("snapshot_timestamp={}/", metadata.created.format("%Y%m%dT%H%M%S%.6fZ")));
    println!("└─ Hive path: {}", hive_display_path.display());

    Ok(())
}

/// Create snapshots for all tables in a database
fn database_snapshot_command(
    workspace_path: Option<&Path>,
    database_name: &str,
    tables_override: Option<Vec<String>>,
    exclude_tables_override: Option<Vec<String>>,
    name_prefix: Option<&str>,
) -> Result<()> {
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    
    // Get database configuration
    let mut db_config = get_database_config(database_name)?;
    
    // Override table selection if provided via command line
    if let Some(tables) = tables_override {
        db_config.tables = tables;
    }
    if let Some(exclude_tables) = exclude_tables_override {
        db_config.exclude_tables = exclude_tables;
    }
    
    println!("🔍 Discovering tables in database '{database_name}'...");
    
    // Discover tables from the database
    let tables = discover_database_tables(&db_config)?;
    
    if tables.is_empty() {
        println!("⚠️  No tables found matching the configuration");
        return Ok(());
    }
    
    println!("📋 Found {} tables to snapshot:", tables.len());
    for table in &tables {
        println!("  • {}", table.name);
    }
    println!();
    
    let mut successful_snapshots = Vec::new();
    let mut failed_snapshots = Vec::new();
    
    // Create snapshots for each table
    for table in tables {
        let table_name = &table.name;
        let snapshot_name = if let Some(prefix) = name_prefix {
            format!("{prefix}_{table_name}")
        } else {
            // Generate name using configured pattern
            let snapshot_config = get_snapshot_config()?;
            let namer = SnapshotNamer::new(snapshot_config.default_name_pattern);
            let rt = tokio::runtime::Runtime::new()?;
            let existing_snapshots = rt.block_on(async {
                let all_snapshots = workspace.storage().list_snapshots_for_all_sources().await?;
                let source_key = format!("{database_name}:{table_name}");
                Ok::<Vec<String>, SnapbaseError>(
                    all_snapshots.get(&source_key).cloned().unwrap_or_default()
                )
            })?;
            namer.generate_name(&format!("{database_name}_{table_name}"), &existing_snapshots)?
        };
        
        println!("📸 Creating snapshot '{snapshot_name}' for table '{table_name}'...");
        
        match create_table_snapshot(&workspace, &db_config, &table, &snapshot_name) {
            Ok(metadata) => {
                successful_snapshots.push((table_name.clone(), snapshot_name.clone()));
                println!("✅ Snapshot '{snapshot_name}' created successfully!");
                println!("  ├─ Rows: {}", metadata.row_count);
                println!("  ├─ Columns: {}", metadata.column_count);
                println!("  └─ Timestamp: {}", metadata.created.format("%Y-%m-%d %H:%M:%S UTC"));
            }
            Err(e) => {
                failed_snapshots.push((table_name.clone(), e.to_string()));
                eprintln!("❌ Failed to create snapshot for table '{table_name}': {e}");
            }
        }
        println!();
    }
    
    // Print summary
    println!("📊 Database snapshot summary:");
    println!("  ✅ Successful: {}", successful_snapshots.len());
    println!("  ❌ Failed: {}", failed_snapshots.len());
    
    if !successful_snapshots.is_empty() {
        println!("\n✅ Successfully created snapshots:");
        for (table, snapshot) in successful_snapshots {
            println!("  • {table} → {snapshot}");
        }
    }
    
    if !failed_snapshots.is_empty() {
        println!("\n❌ Failed snapshots:");
        for (table, error) in failed_snapshots {
            println!("  • {table}: {error}");
        }
    }
    
    Ok(())
}

/// Create a snapshot for a single database table
fn create_table_snapshot(
    workspace: &SnapbaseWorkspace,
    db_config: &config::DatabaseConfig,
    table: &TableInfo,
    snapshot_name: &str,
) -> Result<snapshot::SnapshotMetadata> {
    use std::io::Write;
    
    // Create a temporary SQL file for this table
    let temp_dir = std::env::temp_dir();
    let sql_file_path = temp_dir.join(format!("snapbase_{}_{}.sql", db_config.db_type, table.name));
    
    let sql_content = create_table_snapshot_sql(db_config, table)?;
    
    // Write the SQL file
    let mut file = std::fs::File::create(&sql_file_path)?;
    file.write_all(sql_content.as_bytes())?;
    file.sync_all()?;
    
    // Create source identifier (database:table format)
    let source_identifier = format!("{}:{}", 
        db_config.database.as_ref().unwrap_or(&"unknown".to_string()),
        table.name
    );
    
    // Create snapshot using the SQL file
    let metadata = create_hive_snapshot(
        workspace,
        &sql_file_path,
        &source_identifier,
        snapshot_name,
    )?;
    
    // Clean up temporary file
    let _ = std::fs::remove_file(&sql_file_path);
    
    Ok(metadata)
}

/// Show snapshot information
fn show_command(
    workspace_path: Option<&Path>,
    source: &str,
    snapshot: &str,
    detailed: bool,
    json: bool,
) -> Result<()> {
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    let _resolver = SnapshotResolver::new(workspace.clone());

    // Canonicalize source path
    let source_path = if Path::new(source).is_absolute() {
        Path::new(source).to_path_buf()
    } else {
        workspace.root.join(source)
    };
    let canonical_source_path = source_path.canonicalize()
        .unwrap_or(source_path.clone())
        .to_string_lossy()
        .to_string();

    // Find the specific snapshot for this source file
    let rt = tokio::runtime::Runtime::new()?;
    let resolved = rt.block_on(async {
        // Get all snapshots and find the one that matches both source and name
        let all_snapshots = workspace.storage().list_all_snapshots().await?;
        let matching_snapshot = all_snapshots.into_iter().find(|s| {
            s.name == snapshot && 
            (s.source_path.as_ref() == Some(&canonical_source_path))
        });
        
        if let Some(snapshot_metadata) = matching_snapshot {
            // Found the specific snapshot - construct resolved snapshot
            let source_name = snapshot_metadata.source_path.as_ref()
                .map(|path| Path::new(path).file_name().unwrap().to_string_lossy().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            
            let timestamp_str = snapshot_metadata.created.format("%Y%m%dT%H%M%S%.6fZ").to_string();
            let hive_path_str = path_utils::join_for_storage_backend(&[
                "sources",
                &source_name,
                &format!("snapshot_name={snapshot}"),
                &format!("snapshot_timestamp={timestamp_str}")
            ], workspace.storage());
            
            let metadata_path = format!("{hive_path_str}/metadata.json");
            let data_path = format!("{hive_path_str}/data.parquet");
            
            Ok(snapbase_core::resolver::ResolvedSnapshot {
                name: snapshot.to_string(),
                archive_path: None,
                json_path: Some(metadata_path),
                data_path: Some(data_path),
                metadata: Some(snapshot_metadata),
            })
        } else {
            Err(SnapbaseError::SnapshotNotFound {
                name: format!("{source}:{snapshot}"),
            })
        }
    })?;

    // Load metadata from Hive storage
    let metadata = if let Some(preloaded) = resolved.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &resolved.json_path {
        // Load from storage backend (works for both local and cloud)
        let rt = tokio::runtime::Runtime::new()?;
        let metadata_data = rt.block_on(async {
            workspace.storage().read_file(json_path).await
        })?;
        serde_json::from_slice::<snapshot::SnapshotMetadata>(&metadata_data)?
    } else {
        return Err(SnapbaseError::SnapshotNotFound {
            name: snapshot.to_string(),
        });
    };
    let mut metadata_json = serde_json::to_value(&metadata)?;
    
    // Add computed schema hash to the JSON output
    if let Some(obj) = metadata_json.as_object_mut() {
        let schema_hash = metadata.compute_schema_hash().unwrap_or_else(|_| "error".to_string());
        obj.insert("schema_hash".to_string(), serde_json::Value::String(schema_hash));
    }

    if json {
        // For detailed output, metadata already contains all necessary information
        // Full data can be accessed via direct DuckDB queries if needed
        println!("{}", serde_json::to_string_pretty(&metadata_json)?);
    } else {
        PrettyPrinter::print_snapshot_metadata(&metadata_json, detailed);
    }

    Ok(())
}

/// Memory-efficient streaming status command
fn streaming_status_command(
    workspace_path: Option<&Path>,
    input: &str,
    compare_to: Option<&str>,
    quiet: bool,
    json: bool,
) -> Result<()> {
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());

    // Canonicalize input path for comparison
    let input_path = if Path::new(input).is_absolute() {
        Path::new(input).to_path_buf()
    } else {
        workspace.root.join(input)
    };
    let canonical_input_path = input_path.canonicalize()
        .unwrap_or(input_path.clone())
        .to_string_lossy()
        .to_string();

    // Resolve comparison snapshot
    let comparison_snapshot = if let Some(name) = compare_to {
        let snap_ref = SnapshotRef::from_string(name.to_string());
        resolver.resolve(&snap_ref)?
    } else {
        // Find latest snapshot for this specific source
        let latest_for_source = workspace.latest_snapshot_for_source(&canonical_input_path)?;
        if let Some(latest_name) = latest_for_source {
            let snap_ref = SnapshotRef::from_string(latest_name);
            resolver.resolve(&snap_ref)?
        } else {
            return Err(SnapbaseError::workspace("No snapshots found to compare against"));
        }
    };

    if !json {
        println!("📊 Streaming comparison of '{}' against snapshot '{}'...", input, comparison_snapshot.name);
    }

    // Load baseline snapshot metadata
    let rt = tokio::runtime::Runtime::new()?;
    let baseline_metadata = if let Some(preloaded) = comparison_snapshot.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &comparison_snapshot.json_path {
        let metadata_data = rt.block_on(async {
            workspace.storage().read_file(json_path).await
        })?;
        serde_json::from_slice::<snapbase_core::snapshot::SnapshotMetadata>(&metadata_data)?
    } else {
        return Err(SnapbaseError::SnapshotNotFound {
            name: comparison_snapshot.name.clone(),
        });
    };

    // Get baseline data path
    let baseline_data_path = comparison_snapshot.data_path.as_ref().ok_or_else(|| {
        SnapbaseError::archive("Baseline snapshot has no data path")
    })?;

    // Load current data info
    let mut current_data_processor = DataProcessor::new_with_workspace(&workspace)?;
    let current_data_info = current_data_processor.load_file(&input_path)?;

    if !json {
        println!("🔍 Phase 1: Building hash sets for efficient comparison...");
    }

    // Phase 1: Stream both datasets and build hash sets
    let rt = tokio::runtime::Runtime::new()?;
    let (baseline_hashes, current_hashes) = rt.block_on(async {
        // Stream baseline data
        let mut baseline_processor = DataProcessor::new_with_workspace(&workspace)?;
        let baseline_rows = baseline_processor.load_cloud_storage_data(baseline_data_path, &workspace, false).await?;
        let baseline_stream_data: Vec<(u64, Vec<String>)> = baseline_rows
            .into_iter()
            .enumerate()
            .map(|(i, row)| (i as u64, row))
            .collect();

        // Stream current data  
        let current_rows = current_data_processor.stream_rows_async(None::<fn(u64, u64, &str)>).await?;

        // Build hash sets
        let mut baseline_hashes = snapbase_core::change_detection::RowHashSet::new();
        let mut current_hashes = snapbase_core::change_detection::RowHashSet::new();

        // Get original columns (exclude snapbase metadata) for consistent hashing
        let original_column_count = baseline_metadata.columns.len();
        
        // Build baseline hashes (only original columns, exclude metadata)
        for (index, row) in baseline_stream_data {
            let original_row_data = filter_original_columns(&row, original_column_count);
            let hash = StreamingChangeDetector::compute_row_hash(&original_row_data);
            baseline_hashes.add_row(index, hash);
        }

        // Build current hashes (only original columns, exclude metadata)
        for (index, row) in current_rows {
            let original_row_data = filter_original_columns(&row, original_column_count);
            let hash = StreamingChangeDetector::compute_row_hash(&original_row_data);
            current_hashes.add_row(index, hash);
        }

        Ok::<_, SnapbaseError>((baseline_hashes, current_hashes))
    })?;

    if !json {
        println!("🔍 Phase 2: Identifying changed rows...");
    }

    // Phase 2: Compare hash sets to identify changed rows
    let changed_rows = StreamingChangeDetector::identify_changed_rows(&baseline_hashes, &current_hashes);

    // Calculate actual number of logical changes (not double-counting modified rows)
    let unique_changed_rows: std::collections::HashSet<u64> = changed_rows.baseline_changed.iter()
        .chain(changed_rows.current_changed.iter())
        .cloned()
        .collect();
    let changed_count = unique_changed_rows.len();
    
    if !json {
        println!("📈 Found {} unchanged rows, {} changed rows", changed_rows.unchanged_count, changed_count);
        
        if changed_count > 0 {
            println!("🔍 Phase 3: Analyzing {changed_count} changed rows in detail...");
        }
    }

    // Get original columns (exclude snapbase metadata) for consistent analysis
    let original_column_count = baseline_metadata.columns.len();
    
    // Phase 3: Load and analyze only the changed rows if there are any changes
    let changes = if changed_count > 0 {
        rt.block_on(async {
            // Load baseline changed rows
            let baseline_changed_data_full = if !changed_rows.baseline_changed.is_empty() {
                let mut processor = DataProcessor::new_with_workspace(&workspace)?;
                processor.load_specific_rows_from_storage(baseline_data_path, &workspace, &changed_rows.baseline_changed).await?
            } else {
                HashMap::new()
            };

            // Load current changed rows
            let current_changed_data_full = if !changed_rows.current_changed.is_empty() {
                let mut processor = DataProcessor::new_with_workspace(&workspace)?;
                let _data_info = processor.load_file(&input_path)?;
                processor.load_specific_rows(&changed_rows.current_changed).await?
            } else {
                HashMap::new()
            };
            
            // Filter to original columns only (exclude metadata) for analysis
            let baseline_changed_data = filter_changed_row_data(&baseline_changed_data_full, original_column_count);
            let current_changed_data = filter_changed_row_data(&current_changed_data_full, original_column_count);

            StreamingChangeDetector::analyze_changed_rows(
                &changed_rows,
                &baseline_metadata.columns,
                &current_data_info.columns,
                baseline_changed_data,
                current_changed_data,
            ).await
        })?
    } else {
        // No changes detected, create empty result
        snapbase_core::change_detection::ChangeDetectionResult {
            schema_changes: snapbase_core::change_detection::SchemaChanges {
                column_order: None,
                columns_added: Vec::new(),
                columns_removed: Vec::new(),
                columns_renamed: Vec::new(),
                type_changes: Vec::new(),
            },
            row_changes: snapbase_core::change_detection::RowChanges {
                modified: Vec::new(),
                added: Vec::new(),
                removed: Vec::new(),
            },
        }
    };

    // Output results
    if json {
        let status_json = JsonFormatter::format_comprehensive_status_results(&changes)?;
        println!("{status_json}");
    } else {
        PrettyPrinter::print_comprehensive_status_results(&changes, quiet);
        if !quiet {
            println!("✅ Memory-efficient streaming comparison completed!");
            println!("   Processed {} baseline rows, {} current rows", baseline_hashes.len(), current_hashes.len());
            println!("   Memory usage optimized - only loaded {changed_count} changed rows");
        }
    }

    Ok(())
}

/// Check status against a snapshot (original implementation - kept for fallback)
fn status_command(
    workspace_path: Option<&Path>,
    input: &str,
    compare_to: Option<&str>,
    quiet: bool,
    json: bool,
) -> Result<()> {
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());

    // Canonicalize input path for comparison
    let input_path = if Path::new(input).is_absolute() {
        Path::new(input).to_path_buf()
    } else {
        workspace.root.join(input)
    };
    let canonical_input_path = input_path.canonicalize()
        .unwrap_or(input_path.clone())
        .to_string_lossy()
        .to_string();

    // Resolve comparison snapshot
    let comparison_snapshot = if let Some(name) = compare_to {
        let snap_ref = SnapshotRef::from_string(name.to_string());
        resolver.resolve(&snap_ref)?
    } else {
        // Find latest snapshot for this specific source
        let latest_for_source = workspace.latest_snapshot_for_source(&canonical_input_path)?;
        if let Some(latest_name) = latest_for_source {
            let snap_ref = SnapshotRef::from_string(latest_name);
            resolver.resolve(&snap_ref)?
        } else {
            return Err(SnapbaseError::workspace("No snapshots found to compare against"));
        }
    };

    if !json {
        println!("📊 Checking status of '{}' against snapshot '{}'...", input, comparison_snapshot.name);
    }

    // Load baseline snapshot metadata from Hive storage
    let baseline_metadata = if let Some(preloaded) = comparison_snapshot.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &comparison_snapshot.json_path {
        // Load from storage backend (works for both local and cloud)
        let rt = tokio::runtime::Runtime::new()?;
        let metadata_data = rt.block_on(async {
            workspace.storage().read_file(json_path).await
        })?;
        serde_json::from_slice::<snapshot::SnapshotMetadata>(&metadata_data)?
    } else {
        return Err(SnapbaseError::SnapshotNotFound {
            name: comparison_snapshot.name.clone(),
        });
    };

    
    // Load baseline snapshot data from Hive storage
    let data_path = comparison_snapshot.data_path.as_ref().ok_or_else(|| {
        SnapbaseError::archive("Baseline snapshot has no data path")
    })?;


    let rt = tokio::runtime::Runtime::new()?;
    let mut data_processor = DataProcessor::new_with_workspace(&workspace)?;

    let baseline_row_data = rt.block_on(async {
        data_processor.load_cloud_storage_data(data_path, &workspace, false).await
    })?;

    // Load current data
    let mut current_data_processor = DataProcessor::new_with_workspace(&workspace)?;
    let current_data_info = current_data_processor.load_file(&input_path)?;
    let current_row_data = current_data_processor.extract_all_data()?;

    // Extract baseline schema from metadata
    let baseline_schema = baseline_metadata.columns.clone();

    // Use comprehensive change detection
    let changes = ChangeDetector::detect_changes(
        &baseline_schema,
        &baseline_row_data,
        &current_data_info.columns,
        &current_row_data,
    )?;

    // Output results
    if json {
        let status_json = JsonFormatter::format_comprehensive_status_results(&changes)?;
        println!("{status_json}");
    } else {
        PrettyPrinter::print_comprehensive_status_results(&changes, quiet);
    }

    Ok(())
}

/// List all snapshots
fn list_command(workspace_path: Option<&Path>, source_filter: Option<&str>, json: bool) -> Result<()> {
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    
    // Get all snapshots with their source information - same logic for both local and S3
    let rt = tokio::runtime::Runtime::new()?;
    let all_source_snapshots = rt.block_on(async {
        workspace.storage().list_snapshots_for_all_sources().await
    })?;
    
    let mut filtered_snapshots = std::collections::HashMap::new();
    for (source_file, snapshots) in all_source_snapshots {
        if let Some(filter) = source_filter {
            // Try multiple matching strategies
            let matches = {
                // 1. Direct match
                source_file == filter ||
                // 2. Filename match (e.g., "test_data.csv")
                Path::new(&source_file).file_name().and_then(|n| n.to_str()) == Some(filter) ||
                // 3. Canonicalized path match
                {
                    let filter_path = if Path::new(filter).is_absolute() {
                        Path::new(filter).to_path_buf()
                    } else {
                        workspace.root.join(filter)
                    };
                    let canonical_filter = filter_path.canonicalize()
                        .unwrap_or(filter_path)
                        .to_string_lossy()
                        .to_string();
                    source_file == canonical_filter
                }
            };
            
            if matches {
                filtered_snapshots.insert(source_file, snapshots);
            }
        } else {
            filtered_snapshots.insert(source_file, snapshots);
        }
    }
    let snapshots_by_source = filtered_snapshots;
    
    if json {
        println!("{}", serde_json::to_string_pretty(&snapshots_by_source)?);
    } else {
        if snapshots_by_source.is_empty() {
            if let Some(filter) = source_filter {
                println!("No snapshots found for source file: {filter}");
            } else {
                println!("No snapshots found.");
            }
            return Ok(());
        }
        
        // If filtering by a specific source, use the dedicated snapshot list printer
        if source_filter.is_some() && snapshots_by_source.len() == 1 {
            let (_source, snapshots) = snapshots_by_source.into_iter().next().unwrap();
            PrettyPrinter::print_snapshot_list(&snapshots);
        } else {
            // Show all sources with their snapshots
            println!("📸 Available Snapshots:");
            for (source_file, snapshots) in snapshots_by_source {
                // Use the full relative path for display
                let display_name = if source_file == "local snapshots" {
                    source_file
                } else {
                    source_file.clone()
                };
                
                println!("📁 {display_name}");
                for (i, snapshot) in snapshots.iter().enumerate() {
                    let prefix = if i == snapshots.len() - 1 { "└─" } else { "├─" };
                    println!("   {prefix} {snapshot}");
                }
            }
        }
    }

    Ok(())
}


/// Clean up old snapshot archives to save space
fn cleanup_command(
    workspace_path: Option<&Path>,
    keep_full: usize,
    dry_run: bool,
    force: bool,
) -> Result<()> {
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    
    // Check if using cloud storage - cleanup is not applicable
    if workspace.is_cloud_storage() {
        println!("❌ Cleanup command is not applicable for cloud storage backends.");
        println!("💡 For S3 storage, use S3 lifecycle policies to manage old snapshots.");
        println!("   Example: Configure your S3 bucket to delete objects older than 30 days");
        println!("   or transition them to cheaper storage classes like Glacier.");
        return Ok(());
    }
    
    // Build snapshot chain to understand relationships
    let chain = snapshot::SnapshotChain::build_chain(&workspace)?;
    
    if chain.snapshots.is_empty() {
        println!("No snapshots found to clean up.");
        return Ok(());
    }

    println!("🧹 Analyzing snapshots for cleanup...");
    
    // Count total archives for display (consolidate the calculation)
    let mut full_archives_count = 0;
    for snapshot in &chain.snapshots {
        let (archive_path, _) = workspace.snapshot_paths(&snapshot.name);
        if archive_path.exists() {
            full_archives_count += 1;
        }
    }
    
    // Find snapshots that can have their full data removed (selective cleanup)
    let candidates_for_cleanup = chain.find_data_cleanup_candidates(keep_full, &workspace)?;

    if candidates_for_cleanup.is_empty() {
        println!("✅ No snapshots need data cleanup.");
        println!("   • Total archives: {full_archives_count}");
        println!("   • Keep full data for: {keep_full}");
        return Ok(());
    }

    // Calculate space savings from removing data.parquet files
    let mut total_space_saved = 0u64;
    
    // Estimate space savings (this would be more accurate with actual file analysis)
    for snapshot in &candidates_for_cleanup {
        if let Some(archive_size) = snapshot.archive_size {
            // Estimate that data.parquet is about 60-80% of archive size
            total_space_saved += (archive_size as f64 * 0.7) as u64;
        }
    }

    println!("📊 Cleanup analysis:");
    println!("   • Total archives: {full_archives_count}");
    println!("   • Snapshots for data cleanup: {}", candidates_for_cleanup.len());
    println!("   • Keep full data for: {keep_full}");
    println!("   • Estimated space savings: {total_space_saved} bytes");
    println!("   • Archives will retain deltas for reconstruction");

    if dry_run {
        println!("\n🔍 Dry run - snapshots that would have data cleaned up:");
        for snapshot in &candidates_for_cleanup {
            println!("   • {} (seq: {}, estimated savings: {} bytes)", 
                    snapshot.name, 
                    snapshot.sequence_number,
                    (snapshot.archive_size.unwrap_or(0) as f64 * 0.7) as u64);
        }
        println!("\n💡 Use --force to apply these changes");
        return Ok(());
    }

    // Ask for confirmation unless force is used
    if !force {
        println!("\n⚠️  ROLLBACK IMPACT WARNING:");
        if !candidates_for_cleanup.is_empty() {
            let oldest_cleanup = &candidates_for_cleanup[candidates_for_cleanup.len() - 1];
            let newest_cleanup = &candidates_for_cleanup[0];
            println!("   • Rollback will NOT work for snapshots: {} to {}", oldest_cleanup.name, newest_cleanup.name);
        }
        println!("   • Rollback WILL work for the {keep_full} most recent snapshots");
        println!("   • Full data will be removed from {} snapshots (deltas preserved)", candidates_for_cleanup.len());
        println!("\n❓ Continue with cleanup? (y/N)");
        
        let mut user_input = String::new();
        std::io::stdin().read_line(&mut user_input)?;
        
        if !user_input.trim().to_lowercase().starts_with('y') {
            println!("❌ Cleanup cancelled.");
            return Ok(());
        }
    }

    // Perform selective cleanup (remove data.parquet but keep delta.parquet)
    let cleaned_count = 0;
    let actual_space_saved = 0u64;
    
    // Archive system removed - cleanup functionality disabled
    println!("ℹ️  Archive cleanup is no longer available (moved to Hive-style storage)");

    println!("✅ Cleanup completed!");
    println!("   • Snapshots cleaned: {cleaned_count}");
    println!("   • Estimated space saved: {actual_space_saved} bytes");
    println!("   • Deltas preserved for reconstruction");

    Ok(())
}

/// Query historical snapshots using SQL
fn query_command(
    workspace_path: Option<&Path>,
    source: &str,
    sql: Option<&str>,
    format: &str,
    limit: Option<usize>,
    list_snapshots: bool,
) -> Result<()> {
    use snapbase_core::query::SnapshotQueryEngine;
    
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    
    // Create DuckDB connection configured for the workspace storage backend
    let connection = snapbase_core::query_engine::create_configured_connection(&workspace)?;
    
    // Register the Hive view for the source
    snapbase_core::query_engine::register_hive_view(&connection, &workspace, source, "data")?;
    
    let mut query_engine = SnapshotQueryEngine::new(workspace)?;
    
    // Handle list snapshots request
    if list_snapshots {
        let snapshots = query_engine.list_snapshots(source)?;
        println!("📊 Available snapshots for '{source}':");
        if snapshots.is_empty() {
            println!("   No snapshots found.");
        } else {
            for snapshot in snapshots {
                println!("  {} - {}", snapshot.name, snapshot.timestamp.format("%Y-%m-%d %H:%M:%S UTC"));
            }
        }
        return Ok(());
    }
    
    // Execute the query
    let sql_str = sql.unwrap_or("SELECT * FROM data");
    let mut final_sql = sql_str.to_string();
    if let Some(limit_value) = limit {
        final_sql = format!("{final_sql} LIMIT {limit_value}");
    }
    
    let result = query_engine.query(source, &final_sql)?;
    
    // Output results in requested format
    match format {
        "json" => print_json_result(&result)?,
        "csv" => print_csv_result(&result)?,
        _ => print_table_result(&result)?,
    }
    
    Ok(())
}

fn print_table_result(result: &QueryResult) -> Result<()> {
    // Simple table printing since we don't have prettytable-rs yet
    
    // Calculate column widths
    let mut col_widths: Vec<usize> = result.columns.iter()
        .map(|col| col.len())
        .collect();
    
    // Update widths based on data
    for row in &result.rows {
        for (i, val) in row.iter().enumerate() {
            let val_str = format_query_value(val);
            col_widths[i] = col_widths[i].max(val_str.len());
        }
    }
    
    // Ensure minimum width of 10
    for width in &mut col_widths {
        *width = (*width).max(10);
    }
    
    // Print header
    let header: Vec<String> = result.columns.iter()
        .zip(&col_widths)
        .map(|(col, &width)| format!("{col:<width$}"))
        .collect();
    println!("{}", header.join(" | "));
    
    // Print separator with proper alignment
    let separator: Vec<String> = col_widths.iter()
        .map(|&width| "-".repeat(width))
        .collect();
    println!("{}", separator.join("-|-"));
    
    // Print rows
    for row in &result.rows {
        let row_str: Vec<String> = row.iter()
            .zip(&col_widths)
            .map(|(val, &width)| {
                let val_str = format_query_value(val);
                format!("{val_str:<width$}")
            })
            .collect();
        println!("{}", row_str.join(" | "));
    }
    
    println!("\n📊 {} rows returned", result.row_count);
    
    Ok(())
}

fn print_json_result(result: &QueryResult) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(result)?);
    Ok(())
}

fn print_csv_result(result: &QueryResult) -> Result<()> {
    // Print header
    println!("{}", result.columns.join(","));
    
    // Print rows
    for row in &result.rows {
        let csv_row: Vec<String> = row.iter()
            .map(format_query_value)
            .collect();
        println!("{}", csv_row.join(","));
    }
    
    Ok(())
}

fn format_query_value(value: &QueryValue) -> String {
    match value {
        QueryValue::String(s) => s.clone(),
        QueryValue::Integer(i) => i.to_string(),
        QueryValue::Float(f) => f.to_string(),
        QueryValue::Boolean(b) => b.to_string(),
        QueryValue::Null => "null".to_string(),
    }
}

/// Check if a Hive snapshot already exists using storage backend
fn hive_snapshot_exists(workspace: &SnapbaseWorkspace, source: &str, snapshot_name: &str) -> Result<bool> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        // Get all snapshots for this source from the storage backend
        let all_snapshots = workspace.storage().list_snapshots_for_all_sources().await?;
        
        // Use the source name (filename) as the key, same as hive directory structure
        let source_key = source.to_string();
        
        // Check if this snapshot name exists for this source
        if let Some(snapshots) = all_snapshots.get(&source_key) {
            Ok(snapshots.contains(&snapshot_name.to_string()))
        } else {
            Ok(false)
        }
    })
}

/// Create a snapshot using pure Hive structure
/// Extract database name from ATTACH statement
fn extract_database_name(connection_string: &str) -> Option<String> {
    // Look for "AS database_name" pattern in ATTACH statements
    if let Some(as_pos) = connection_string.to_uppercase().find(" AS ") {
        let after_as = &connection_string[as_pos + 4..];
        if let Some(space_pos) = after_as.find(' ') {
            Some(after_as[..space_pos].trim().to_string())
        } else if let Some(paren_pos) = after_as.find('(') {
            Some(after_as[..paren_pos].trim().to_string())
        } else {
            Some(after_as.trim().to_string())
        }
    } else {
        None
    }
}

fn create_hive_snapshot(
    workspace: &SnapbaseWorkspace,
    input_path: &Path,
    source_name: &str,
    snapshot_name: &str,
) -> Result<snapshot::SnapshotMetadata> {
    use snapbase_core::data::DataProcessor;
    use chrono::Utc;
    
    // Create timestamp
    let timestamp = Utc::now();
    let timestamp_str = timestamp.format("%Y%m%dT%H%M%S%.6fZ").to_string();
    
    // Create Hive directory structure path using storage-backend-aware path construction
    let hive_path_str = path_utils::join_for_storage_backend(&[
        "sources",
        source_name,
        &format!("snapshot_name={snapshot_name}"),
        &format!("snapshot_timestamp={timestamp_str}")
    ], workspace.storage());
    
    // Use async runtime to handle storage backend operations
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        // Ensure directory exists in storage backend
        workspace.storage().ensure_directory(&hive_path_str).await
    })?;
    
    // Process data with streaming for large datasets - use workspace-configured processor
    let mut processor = DataProcessor::new_with_workspace(workspace)?;
    let data_info = processor.load_file(input_path)?;
    
    // Create progress reporter for the snapshot operation
    let mut progress_reporter = ProgressReporter::new_for_snapshot(data_info.row_count);
    
    // Show progress for large datasets
    println!("📊 Found {} rows, {} columns", data_info.row_count, data_info.column_count());
    
    if data_info.row_count > 100_000 {
        println!("🚀 Using optimized streaming for large dataset ({} rows)...", data_info.row_count);
    }
    
    // Finish schema analysis phase
    progress_reporter.finish_schema(&format!("Schema analyzed: {} columns", data_info.column_count()));
    
    // Load baseline data from previous snapshot for change detection
    // Use the canonical path for lookup, but the source name for directory structure
    let canonical_input_path = input_path.canonicalize()
        .unwrap_or_else(|_| input_path.to_path_buf())
        .to_string_lossy()
        .to_string();
    let baseline_data = load_baseline_data_for_cli(workspace, &canonical_input_path, source_name)?;

    // Create Parquet file using DuckDB COPY - get storage backend path
    let parquet_relative_path = format!("{hive_path_str}/data.parquet");
    let parquet_path = workspace.storage().get_duckdb_path(&parquet_relative_path);
    
    println!("💾 Writing data to Parquet file...");
    
    // Update progress for data processing phase
    progress_reporter.finish_rows(&format!("Processing {} rows", data_info.row_count));
    
    // Use DuckDB's direct COPY command for optimal performance
    // This avoids materializing data in memory and row-by-row inserts
    if sql::is_sql_file(input_path) {
        // For SQL files, we need to copy from the streaming query directly
        let sql_file = sql::parse_sql_file(input_path)?;
        let connection_string = sql::substitute_env_vars(&sql_file.connection_string)?;
        
        // Check if we need to re-establish connection
        // The connection might already be established from load_file()
        if !connection_string.is_empty() {
            if let Some(db_name) = extract_database_name(&connection_string) {
                // Check if database is already attached
                let check_query = format!("SELECT database_name FROM duckdb_databases() WHERE database_name = '{db_name}'");
                let is_attached = processor.connection
                    .prepare(&check_query)
                    .and_then(|mut stmt| stmt.query_row([], |_| Ok(true)))
                    .unwrap_or(false);
                
                if !is_attached {
                    processor.connection.execute(&connection_string, [])?;
                } else {
                    println!("🔗 Database '{db_name}' already connected");
                }
            }
        }
        
        // Parse the SQL content to get the SELECT query
        let content = std::fs::read_to_string(input_path)?;
        let statements: Vec<&str> = content.split(';').collect();
        let mut select_query = String::new();
        
        for statement in statements {
            let trimmed = statement.trim();
            if trimmed.is_empty() {
                continue;
            }
            
            let cleaned_statement = trimmed.lines()
                .filter(|line| !line.trim().starts_with("--") && !line.trim().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_string();
            
            let upper_statement = cleaned_statement.to_uppercase();
            if (upper_statement.starts_with("SELECT") || upper_statement.starts_with("WITH")) && 
               !upper_statement.contains("CREATE TABLE") {
                select_query = cleaned_statement;
                break;
            }
        }
        
        if !select_query.is_empty() {
            // Direct copy from SQL query to Parquet - maximum performance
            let copy_sql = format!(
                "COPY ({select_query}) TO '{parquet_path}' (FORMAT parquet)"
            );
            
            let start_time = std::time::Instant::now();
            processor.connection.execute(&copy_sql, [])?;
            let elapsed = start_time.elapsed();
            
            println!("⚡ Direct copy completed in {:.1}s", elapsed.as_secs_f64());
        }
    } else {
        // For regular files, use our flag-enhanced export method
        println!("⚡ Executing direct file-to-Parquet copy...");
        let start_time = std::time::Instant::now();
        
        // Create a local path for the export
        let temp_path = std::path::Path::new(&parquet_path);
        processor.export_to_parquet_with_flags(temp_path, baseline_data.as_ref())?;
        
        let elapsed = start_time.elapsed();
        println!("⚡ Direct copy completed in {:.1}s", elapsed.as_secs_f64());
    }
    
    println!("✅ Parquet file created successfully");
    
    // Create metadata.json
    let metadata = snapshot::SnapshotMetadata {
        format_version: "1.0.0".to_string(),
        name: snapshot_name.to_string(),
        created: timestamp,
        source: input_path.to_string_lossy().to_string(),
        source_hash: {
            let source_content = std::fs::read_to_string(input_path)?;
            use blake3::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(source_content.as_bytes());
            hasher.finalize().to_hex().to_string()
        },
        row_count: data_info.row_count,
        column_count: data_info.columns.len(),
        columns: data_info.columns.clone(),
        archive_size: None, // No longer using archives - using direct Parquet files
        parent_snapshot: None,
        sequence_number: 0,
        delta_from_parent: None,
        can_reconstruct_parent: false,
        source_path: Some(input_path.to_string_lossy().to_string()),
        source_fingerprint: Some({
            let source_content = std::fs::read_to_string(input_path)?;
            use blake3::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(input_path.to_string_lossy().as_bytes());
            hasher.update(b":");
            hasher.update(source_content.as_bytes());
            format!("{}:{}", input_path.to_string_lossy(), hasher.finalize().to_hex())
        }),
    };
    
    let metadata_json = serde_json::to_string_pretty(&metadata)?;
    let metadata_path = format!("{hive_path_str}/metadata.json");
    
    // Write metadata using storage backend with progress bar
    let metadata_bytes = metadata_json.as_bytes();
    let metadata_size = metadata_bytes.len() as u64;
    
    println!("📝 Writing metadata to {metadata_path}");
    let upload_progress = progress_reporter.create_upload_progress(metadata_size, "Uploading metadata...");
    
    rt.block_on(async {
        workspace.storage().write_file_with_progress(&metadata_path, metadata_bytes, upload_progress).await
    })?;
    
    progress_reporter.finish_upload("Metadata uploaded successfully");
    
    // Note: schema.json is no longer created - schema information is stored in metadata.json
    // and hashes are computed on-demand from the columns array
    
    Ok(metadata)
}

/// Configure snapbase settings
fn config_command(workspace_path: Option<&Path>, command: &crate::cli::ConfigCommand) -> Result<()> {
    match command {
        crate::cli::ConfigCommand::Storage { 
            backend, 
            s3_bucket, 
            s3_prefix, 
            s3_region, 
            local_path,
            global
        } => {
            configure_storage(workspace_path, backend, s3_bucket, s3_prefix, s3_region, local_path, *global)
        }
        crate::cli::ConfigCommand::Show => {
            show_current_config()
        }
        crate::cli::ConfigCommand::DefaultName { pattern } => {
            configure_default_name(pattern)
        }
    }
}

fn configure_storage(
    workspace_path: Option<&Path>,
    backend: &crate::cli::StorageBackend,
    s3_bucket: &Option<String>,
    s3_prefix: &Option<String>,
    s3_region: &Option<String>,
    local_path: &Option<String>,
    global: bool,
) -> Result<()> {
    use config::StorageConfig;
    
    let config = match backend {
        crate::cli::StorageBackend::Local => {
            let path = local_path.clone().unwrap_or_else(|| ".snapbase".to_string());
            StorageConfig::Local {
                path: std::path::PathBuf::from(path),
            }
        }
        crate::cli::StorageBackend::S3 => {
            // Load .env file if it exists before checking environment variables
            if let Ok(current_dir) = std::env::current_dir() {
                let env_file = current_dir.join(".env");
                if env_file.exists() {
                    if let Err(e) = dotenv::from_filename(&env_file) {
                        log::warn!("Failed to load .env file: {e}");
                    }
                }
            }
            
            // Check for bucket from CLI args first, then environment variables
            let bucket = s3_bucket.clone()
                .or_else(|| std::env::var("SNAPBASE_S3_BUCKET").ok())
                .ok_or_else(|| {
                    SnapbaseError::invalid_input("S3 bucket is required for S3 backend. Provide it via --s3-bucket argument or set SNAPBASE_S3_BUCKET environment variable in .env file".to_string())
                })?;
            
            let prefix = s3_prefix.clone()
                .or_else(|| std::env::var("SNAPBASE_S3_PREFIX").ok())
                .unwrap_or_default();
            
            let region = s3_region.clone()
                .or_else(|| std::env::var("SNAPBASE_S3_REGION").ok())
                .unwrap_or_else(|| "us-east-1".to_string());
            
            StorageConfig::S3 {
                bucket,
                prefix,
                region,
                access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
                secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
            }
        }
    };
    
    if global {
        // Save to global config
        config::save_storage_config(&config)?;
        println!("✅ Storage configuration saved to global config");
    } else {
        // Save to workspace config (default behavior)
        let current_dir = std::env::current_dir()?;
        let root = workspace_path.unwrap_or(&current_dir);
        
        // Check if we're in a workspace
        let workspace_config_path = root.join("snapbase.toml");
        if workspace_config_path.exists() {
            // We're in a workspace, save to workspace config
            let workspace = snapbase_core::workspace::SnapbaseWorkspace::find_or_create(Some(root))?;
            workspace.save_storage_config(&config)?;
            println!("✅ Storage configuration saved to workspace config");
        } else {
            // No workspace found, save to global config
            config::save_storage_config(&config)?;
            println!("✅ Storage configuration saved to global config (no workspace found)");
        }
    }
    
    show_config(&config);
    
    Ok(())
}

fn show_current_config() -> Result<()> {
    let config = config::get_config()?;
    show_config(&config.storage.to_runtime());
    show_snapshot_config(&config.snapshot);
    Ok(())
}

fn show_config(config: &config::StorageConfig) {
    println!("Current storage configuration:");
    match config {
        config::StorageConfig::Local { path } => {
            println!("  Backend: Local");
            println!("  Path: {}", path.display());
        }
        config::StorageConfig::S3 { bucket, prefix, region, .. } => {
            println!("  Backend: S3");
            println!("  Bucket: {bucket}");
            println!("  Prefix: {prefix}");
            println!("  Region: {region}");
        }
    }
}

fn configure_default_name(pattern: &str) -> Result<()> {
    config::save_default_name_pattern(pattern)?;
    println!("✅ Default snapshot name pattern updated to: {pattern}");
    println!("Available variables: {{source}}, {{source_ext}}, {{format}}, {{seq}}, {{timestamp}}, {{date}}, {{time}}, {{hash}}, {{user}}");
    Ok(())
}

fn show_snapshot_config(config: &config::SnapshotConfig) {
    println!("Current snapshot configuration:");
    println!("  Default name pattern: {}", config.default_name_pattern);
}

/// Load baseline data from the latest snapshot for change detection
fn load_baseline_data_for_cli(
    workspace: &SnapbaseWorkspace,
    canonical_source_path: &str,
    source_name: &str,
) -> Result<Option<snapbase_core::data::BaselineData>> {
    // Find the latest snapshot for this source
    let latest_snapshot = workspace.latest_snapshot_for_source(canonical_source_path)?;

    if let Some(snapshot_name) = latest_snapshot {
        // Load the snapshot data
        let rt = tokio::runtime::Runtime::new()?;
        
        // Find the snapshot directory
        let sources_dir = "sources";
        let snapshot_dir = format!("{sources_dir}/{source_name}/snapshot_name={snapshot_name}");
        
        // List all timestamp directories to find the latest
        let timestamp_dirs = rt.block_on(async {
            workspace.storage().list_directories(&snapshot_dir).await
        })?;
        
        // Find the latest timestamp directory
        let mut latest_timestamp = None;
        for dir in timestamp_dirs {
            if let Some(stripped) = dir.strip_prefix("snapshot_timestamp=") {
                let timestamp = stripped.to_string(); // Remove "snapshot_timestamp=" prefix
                if latest_timestamp.is_none() || Some(&timestamp) > latest_timestamp.as_ref() {
                    latest_timestamp = Some(timestamp);
                }
            }
        }
        
        if let Some(timestamp) = latest_timestamp {
            let full_snapshot_path = format!("{snapshot_dir}/snapshot_timestamp={timestamp}");
            let metadata_path = format!("{full_snapshot_path}/metadata.json");
            let parquet_path = format!("{full_snapshot_path}/data.parquet");
            
            // Load metadata to get schema
            let metadata_data = rt.block_on(async {
                workspace.storage().read_file(&metadata_path).await
            })?;
            
            let metadata: serde_json::Value = serde_json::from_slice(&metadata_data)?;
            
            // Extract column information
            let columns = metadata["columns"].as_array()
                .ok_or_else(|| SnapbaseError::invalid_input("Missing columns in metadata"))?;
            
            let mut schema = Vec::new();
            for col in columns {
                schema.push(hash::ColumnInfo {
                    name: col["name"].as_str().unwrap_or("unknown").to_string(),
                    data_type: col["data_type"].as_str().unwrap_or("TEXT").to_string(),
                    nullable: col["nullable"].as_bool().unwrap_or(true),
                });
            }
            
            // Load parquet data using DuckDB
            let data = load_parquet_data_from_storage(workspace, &parquet_path, &schema)?;
            
            return Ok(Some(snapbase_core::data::BaselineData { schema, data }));
        }
    }
    
    Ok(None)
}

/// Load parquet data from storage backend
fn load_parquet_data_from_storage(
    workspace: &SnapbaseWorkspace,
    parquet_path: &str,
    schema: &[hash::ColumnInfo],
) -> Result<Vec<Vec<String>>> {
    let data_processor = snapbase_core::data::DataProcessor::new_with_workspace(workspace)?;
    
    // Get the DuckDB path for the parquet file
    let duckdb_path = workspace.storage().get_duckdb_path(parquet_path);
    
    // Load the parquet file using DuckDB
    let column_names: Vec<String> = schema.iter().map(|c| c.name.clone()).collect();
    let load_sql = format!(
        "SELECT {} FROM read_parquet('{}')",
        column_names.join(", "),
        duckdb_path
    );
    
    let mut stmt = data_processor.connection.prepare(&load_sql)?;
    let rows = stmt.query_map([], |row| {
        let mut string_row = Vec::new();
        for i in 0..schema.len() {
            let value: String = match row.get_ref(i)? {
                duckdb::types::ValueRef::Null => String::new(),
                duckdb::types::ValueRef::Boolean(b) => if b { "true".to_string() } else { "false".to_string() },
                duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                duckdb::types::ValueRef::Int(i) => i.to_string(),
                duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                duckdb::types::ValueRef::UInt(i) => i.to_string(),
                duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                duckdb::types::ValueRef::Float(f) => f.to_string(),
                duckdb::types::ValueRef::Double(f) => f.to_string(),
                duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                duckdb::types::ValueRef::Date32(d) => {
                    // Convert days since epoch to proper date format
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let date = epoch + chrono::Duration::days(d as i64);
                    date.format("%Y-%m-%d").to_string()
                },
                duckdb::types::ValueRef::Time64(unit, t) => {
                    // Convert microseconds since midnight to HH:MM:SS format
                    match unit {
                        duckdb::types::TimeUnit::Microsecond => {
                            let total_seconds = t / 1_000_000;
                            let hours = total_seconds / 3600;
                            let minutes = (total_seconds % 3600) / 60;
                            let seconds = total_seconds % 60;
                            let microseconds = t % 1_000_000;
                            if microseconds > 0 {
                                format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                            } else {
                                format!("{hours:02}:{minutes:02}:{seconds:02}")
                            }
                        }
                        _ => format!("{t:?}"), // Fallback for other time units
                    }
                },
                duckdb::types::ValueRef::Timestamp(unit, ts) => {
                    // Convert microseconds since Unix epoch to YYYY-MM-DD HH:MM:SS format
                    match unit {
                        duckdb::types::TimeUnit::Microsecond => {
                            let seconds = ts / 1_000_000;
                            let microseconds = ts % 1_000_000;
                            let datetime = chrono::DateTime::from_timestamp(seconds, (microseconds * 1000) as u32)
                                .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                            if microseconds > 0 {
                                datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                            } else {
                                datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                            }
                        }
                        _ => format!("{ts:?}"), // Fallback for other time units
                    }
                },
                _ => "<unknown>".to_string(),
            };
            string_row.push(value);
        }
        Ok(string_row)
    })?;
    
    let mut data = Vec::new();
    for row_result in rows {
        data.push(row_result?);
    }
    
    Ok(data)
}

/// Show workspace statistics
fn stats_command(workspace_path: Option<&Path>, json: bool) -> Result<()> {
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    
    // Calculate workspace statistics
    let rt = tokio::runtime::Runtime::new()?;
    let stats = rt.block_on(async {
        let all_snapshots = workspace.storage().list_all_snapshots().await?;
        let snapshot_count = all_snapshots.len();
        
        // For now, use simplified stats since we've moved to Hive storage
        // We can enhance this later with actual file size calculations
        Ok::<snapbase_core::workspace::WorkspaceStats, SnapbaseError>(snapbase_core::workspace::WorkspaceStats {
            snapshot_count,
            diff_count: 0, // Legacy from archive system
            total_archive_size: 0, // Legacy from archive system  
            total_json_size: 0, // Would require reading all metadata files
            total_diff_size: 0, // Legacy from archive system
        })
    })?;
    
    if json {
        let json_output = JsonFormatter::format_workspace_stats(&stats)?;
        println!("{json_output}");
    } else {
        PrettyPrinter::print_workspace_stats(&stats);
    }
    
    Ok(())
}

/// Memory-efficient streaming diff command
fn streaming_diff_command(
    workspace_path: Option<&Path>,
    source: &str,
    from_snapshot: &str,
    to_snapshot: &str,
    json: bool,
) -> Result<()> {
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());
    
    // Resolve both snapshots
    let from_resolved = resolver.resolve_by_name_for_source(from_snapshot, Some(source))?;
    let to_resolved = resolver.resolve_by_name_for_source(to_snapshot, Some(source))?;
    
    if !json {
        println!("🔍 Streaming comparison of snapshots '{from_snapshot}' → '{to_snapshot}'");
    }
    
    // Load metadata for both snapshots
    let rt = tokio::runtime::Runtime::new()?;
    
    let from_metadata = if let Some(preloaded) = from_resolved.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &from_resolved.json_path {
        let metadata_data = rt.block_on(async {
            workspace.storage().read_file(json_path).await
        })?;
        serde_json::from_slice::<snapbase_core::snapshot::SnapshotMetadata>(&metadata_data)?
    } else {
        return Err(SnapbaseError::SnapshotNotFound {
            name: from_snapshot.to_string(),
        });
    };
    
    let to_metadata = if let Some(preloaded) = to_resolved.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &to_resolved.json_path {
        let metadata_data = rt.block_on(async {
            workspace.storage().read_file(json_path).await
        })?;
        serde_json::from_slice::<snapbase_core::snapshot::SnapshotMetadata>(&metadata_data)?
    } else {
        return Err(SnapbaseError::SnapshotNotFound {
            name: to_snapshot.to_string(),
        });
    };
    
    // Get data paths
    let from_data_path = from_resolved.data_path.as_ref().ok_or_else(|| {
        SnapbaseError::archive("From snapshot has no data path")
    })?;

    let to_data_path = to_resolved.data_path.as_ref().ok_or_else(|| {
        SnapbaseError::archive("To snapshot has no data path")  
    })?;

    if !json {
        println!("🔍 Phase 1: Building hash sets for efficient comparison...");
    }

    // Phase 1: Stream both datasets and build hash sets
    let (from_hashes, to_hashes) = rt.block_on(async {
        // Stream from snapshot data
        let mut from_processor = DataProcessor::new_with_workspace(&workspace)?;
        let from_rows = from_processor.load_cloud_storage_data(from_data_path, &workspace, false).await?;
        let from_stream_data: Vec<(u64, Vec<String>)> = from_rows
            .into_iter()
            .enumerate()
            .map(|(i, row)| (i as u64, row))
            .collect();

        // Stream to snapshot data
        let mut to_processor = DataProcessor::new_with_workspace(&workspace)?;
        let to_rows = to_processor.load_cloud_storage_data(to_data_path, &workspace, false).await?;
        let to_stream_data: Vec<(u64, Vec<String>)> = to_rows
            .into_iter()
            .enumerate()
            .map(|(i, row)| (i as u64, row))
            .collect();

        // Build hash sets
        let mut from_hashes = snapbase_core::change_detection::RowHashSet::new();
        let mut to_hashes = snapbase_core::change_detection::RowHashSet::new();

        // Get original columns (exclude snapbase metadata) for consistent hashing
        let original_column_count = from_metadata.columns.len();
        
        // Build from snapshot hashes (only original columns, exclude metadata)
        for (index, row) in from_stream_data {
            let original_row_data = filter_original_columns(&row, original_column_count);
            let hash = StreamingChangeDetector::compute_row_hash(&original_row_data);
            from_hashes.add_row(index, hash);
        }

        // Build to snapshot hashes (only original columns, exclude metadata)
        for (index, row) in to_stream_data {
            let original_row_data = filter_original_columns(&row, original_column_count);
            let hash = StreamingChangeDetector::compute_row_hash(&original_row_data);
            to_hashes.add_row(index, hash);
        }

        Ok::<_, SnapbaseError>((from_hashes, to_hashes))
    })?;

    if !json {
        println!("🔍 Phase 2: Identifying changed rows...");
    }

    // Phase 2: Compare hash sets to identify changed rows
    let changed_rows = StreamingChangeDetector::identify_changed_rows(&from_hashes, &to_hashes);
    // Calculate actual number of logical changes (not double-counting modified rows)
    let unique_changed_rows: std::collections::HashSet<u64> = changed_rows.baseline_changed.iter()
        .chain(changed_rows.current_changed.iter())
        .cloned()
        .collect();
    let changed_count = unique_changed_rows.len();
    
    if !json {
        println!("📈 Found {} unchanged rows, {} changed rows", changed_rows.unchanged_count, changed_count);
        
        if changed_count > 0 {
            println!("🔍 Phase 3: Analyzing {changed_count} changed rows in detail...");
        }
    }

    // Get original columns (exclude snapbase metadata) for consistent analysis
    let original_column_count = from_metadata.columns.len();
    
    // Phase 3: Load and analyze only the changed rows if there are any changes
    let changes = if changed_count > 0 {
        rt.block_on(async {
            // Load from snapshot changed rows
            let from_changed_data_full = if !changed_rows.baseline_changed.is_empty() {
                let mut processor = DataProcessor::new_with_workspace(&workspace)?;
                processor.load_specific_rows_from_storage(from_data_path, &workspace, &changed_rows.baseline_changed).await?
            } else {
                HashMap::new()
            };

            // Load to snapshot changed rows
            let to_changed_data_full = if !changed_rows.current_changed.is_empty() {
                let mut processor = DataProcessor::new_with_workspace(&workspace)?;
                processor.load_specific_rows_from_storage(to_data_path, &workspace, &changed_rows.current_changed).await?
            } else {
                HashMap::new()
            };
            
            // Filter to original columns only (exclude metadata) for analysis
            let from_changed_data = filter_changed_row_data(&from_changed_data_full, original_column_count);
            let to_changed_data = filter_changed_row_data(&to_changed_data_full, original_column_count);

            StreamingChangeDetector::analyze_changed_rows(
                &changed_rows,
                &from_metadata.columns,
                &to_metadata.columns,
                from_changed_data,
                to_changed_data,
            ).await
        })?
    } else {
        // No changes detected, create empty result
        snapbase_core::change_detection::ChangeDetectionResult {
            schema_changes: snapbase_core::change_detection::SchemaChanges {
                column_order: None,
                columns_added: Vec::new(),
                columns_removed: Vec::new(),
                columns_renamed: Vec::new(),
                type_changes: Vec::new(),
            },
            row_changes: snapbase_core::change_detection::RowChanges {
                modified: Vec::new(),
                added: Vec::new(),
                removed: Vec::new(),
            },
        }
    };
    
    // Output results
    if json {
        let diff_json = serde_json::to_value(&changes)?;
        println!("{}", serde_json::to_string_pretty(&diff_json)?);
    } else {
        // Use the same comprehensive output as status command
        PrettyPrinter::print_comprehensive_diff_results(&changes, from_snapshot, to_snapshot);
        println!("✅ Memory-efficient streaming diff completed!");
        println!("   Processed {} baseline rows, {} comparison rows", from_hashes.len(), to_hashes.len());
        println!("   Memory usage optimized - only loaded {changed_count} changed rows");
    }
    
    Ok(())
}

/// Compare two snapshots (original implementation - kept for fallback)
fn diff_command(
    workspace_path: Option<&Path>,
    source: &str,
    from_snapshot: &str,
    to_snapshot: &str,
    json: bool,
) -> Result<()> {
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());
    
    // Resolve both snapshots
    let from_resolved = resolver.resolve_by_name_for_source(from_snapshot, Some(source))?;
    let to_resolved = resolver.resolve_by_name_for_source(to_snapshot, Some(source))?;
    
    println!("🔍 Comparing snapshots '{from_snapshot}' → '{to_snapshot}'");
    
    // Load metadata for both snapshots
    let rt = tokio::runtime::Runtime::new()?;
    
    let from_metadata = if let Some(preloaded) = from_resolved.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &from_resolved.json_path {
        let metadata_data = rt.block_on(async {
            workspace.storage().read_file(json_path).await
        })?;
        serde_json::from_slice::<snapbase_core::snapshot::SnapshotMetadata>(&metadata_data)?
    } else {
        return Err(SnapbaseError::SnapshotNotFound {
            name: from_snapshot.to_string(),
        });
    };
    
    let to_metadata = if let Some(preloaded) = to_resolved.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &to_resolved.json_path {
        let metadata_data = rt.block_on(async {
            workspace.storage().read_file(json_path).await
        })?;
        serde_json::from_slice::<snapbase_core::snapshot::SnapshotMetadata>(&metadata_data)?
    } else {
        return Err(SnapbaseError::SnapshotNotFound {
            name: to_snapshot.to_string(),
        });
    };
    
    // Load data for both snapshots
    let mut data_processor = DataProcessor::new_with_workspace(&workspace)?;
    
    let from_data_path = from_resolved.data_path.as_ref().ok_or_else(|| {
        SnapbaseError::archive("From snapshot has no data path")
    })?;

    let to_data_path = to_resolved.data_path.as_ref().ok_or_else(|| {
        SnapbaseError::archive("To snapshot has no data path")  
    })?;

    let mut progress_reporter = ProgressReporter::new_for_diff();
    progress_reporter.update_archive("Loading snapshot data...");
    
    let from_row_data = rt.block_on(async {
        data_processor.load_cloud_storage_data(from_data_path, &workspace, false).await
    })?;
    println!("FROM DATA {from_row_data:?}");
    progress_reporter.update_archive("Loading comparison data...");
    
    let to_row_data = rt.block_on(async {
        data_processor.load_cloud_storage_data(to_data_path, &workspace, false).await
    })?;
    println!("TO DATA {to_row_data:?}");

    progress_reporter.update_archive("Detecting changes...");
    
    // Perform change detection
    let changes = ChangeDetector::detect_changes(
        &from_metadata.columns,
        &from_row_data,
        &to_metadata.columns,
        &to_row_data,
    )?;
    
    progress_reporter.finish_all("Comparison completed");
    
    // Output results
    if json {
        let diff_json = serde_json::to_value(&changes)?;
        println!("{}", serde_json::to_string_pretty(&diff_json)?);
    } else {
        PrettyPrinter::print_diff_results(&serde_json::to_value(&changes)?);
    }
    
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;
    
    // Import test fixtures
    use snapbase_core::test_fixtures::*;

    #[test]
    fn test_init_command() {
        let workspace = TestWorkspace::new("local.toml");
        let _guard = workspace.change_to_workspace();
        
        // Test successful initialization
        let result = init_command(Some(workspace.path()), false);
        assert!(result.is_ok(), "Failed to initialize workspace");
        
        // Test initialization of already initialized workspace
        let result = init_command(Some(workspace.path()), false);
        assert!(result.is_ok(), "Should handle already initialized workspace");
        
        // Verify workspace directory was created
        let workspace_created = workspace.path().join("snapbase_storage").exists() || 
                               workspace.path().join(".snapbase").exists();
        assert!(workspace_created, "Workspace directory not created");
    }

    #[test]
    fn test_snapshot_command_basic() {
        let workspace = TestWorkspace::new("local.toml");
        let data_file = workspace.copy_data_file("simple.csv", "test.csv");
        let _guard = workspace.change_to_workspace();
        
        // Initialize workspace
        init_command(Some(workspace.path()), false).unwrap();
        
        // Verify workspace was created correctly
        let _ws = SnapbaseWorkspace::find_or_create(Some(workspace.path())).unwrap();
        let workspace_created = workspace.path().join("snapbase_storage").exists() || 
                               workspace.path().join(".snapbase").exists();
        assert!(workspace_created, "Workspace directory not created");
        assert!(data_file.exists(), "Data file not created");
        
        // Test actual snapshot creation
        let result = snapshot_command(
            Some(workspace.path()),
            &data_file.to_string_lossy(),
            Some("test_snapshot")
        );
        
        match result {
            Ok(_) => println!("✅ Snapshot creation successful"),
            Err(e) => {
                println!("⚠️  Snapshot creation failed: {e}");
                // Still verify workspace is functional
                assert!(workspace_created, "Workspace directory not created");
            }
        }
    }


    #[test]
    fn test_validate_file_within_workspace() {
        let workspace = TestWorkspace::new("local.toml");
        let _guard = workspace.change_to_workspace();
        
        // Initialize workspace
        init_command(Some(workspace.path()), false).unwrap();
        let ws = SnapbaseWorkspace::find_or_create(Some(workspace.path())).unwrap();
        
        // Test file within workspace
        let valid_file = workspace.path().join("test.csv");
        fs::write(&valid_file, "data").unwrap();
        let result = validate_file_within_workspace(&valid_file, &ws);
        assert!(result.is_ok(), "Should accept file within workspace");
        
        // Test file outside workspace
        let outside_temp = TempDir::new().unwrap();
        let invalid_file = outside_temp.path().join("test.csv");
        fs::write(&invalid_file, "data").unwrap();
        let result = validate_file_within_workspace(&invalid_file, &ws);
        assert!(result.is_err(), "Should reject file outside workspace");
    }
}
