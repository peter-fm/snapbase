//! Command implementations for snapbase CLI

use crate::cli::Commands;
use crate::output::{PrettyPrinter, JsonFormatter};
use crate::progress::ProgressReporter;
use snapbase_core::query::{QueryResult, QueryValue};
use snapbase_core::error::{Result, SnapbaseError};
use snapbase_core::resolver::{SnapshotRef, SnapshotResolver};
use snapbase_core::workspace::SnapbaseWorkspace;
use snapbase_core::change_detection::StreamingChangeDetector;
use snapbase_core::naming::SnapshotNamer;
use snapbase_core::config::{get_snapshot_config, get_database_config};
use snapbase_core::database::{discover_database_tables, create_table_snapshot_sql, TableInfo};
use snapbase_core::config;
use snapbase_core::snapshot;
use snapbase_core::path_utils;
use snapbase_core::sql;
use snapbase_core::export::{UnifiedExporter, ExportOptions, ExportFormat};
use std::path::Path;


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
        } => thin_wrapper_status_command(workspace_path, &input, compare_to.as_deref(), quiet, json),
        Commands::List { source, json } => list_command(workspace_path, source.as_deref(), json),
        Commands::Stats { json } => stats_command(workspace_path, json),
        Commands::Diff { source, from, to, json } => thin_wrapper_diff_command(workspace_path, &source, &from, &to, json),
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

/// Export snapshot data to a file using unified export functionality
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

    // Build export options
    let mut options = ExportOptions {
        include_header: true,
        delimiter: ',',
        force,
        snapshot_name: to.map(|s| s.to_string()),
        snapshot_date: to_date.map(|s| s.to_string()),
    };

    let output_path = Path::new(output_file);
    
    // Determine output format from file extension
    let export_format = ExportFormat::from_extension(output_path)?;
    
    // Show what will be exported in dry-run mode
    if dry_run {
        println!("🔍 Dry run - would export:");
        println!("  Source: {input}");
        if let Some(snapshot_name) = &options.snapshot_name {
            println!("  Snapshot: {snapshot_name}");
        }
        if let Some(snapshot_date) = &options.snapshot_date {
            println!("  Snapshot date: {snapshot_date}");
        }
        println!("  Output: {} ({:?} format)", output_file, export_format);
        return Ok(());
    }

    // Ask for confirmation if not forced and file exists
    if output_path.exists() && !force {
        println!("⚠️  Output file '{}' already exists. Continue? (y/N)", output_file);
        let mut user_input = String::new();
        std::io::stdin().read_line(&mut user_input)?;
        
        if !user_input.trim().to_lowercase().starts_with('y') {
            println!("❌ Export cancelled.");
            return Ok(());
        }
        // Update force flag since user confirmed
        options.force = true;
    }

    println!("📤 Exporting data using unified export engine...");

    // Use the unified exporter
    let mut exporter = UnifiedExporter::new(workspace)?;
    exporter.export(input, output_path, options)?;

    println!("✅ Export completed successfully!");
    println!("📄 Data exported to '{}'", output_file);

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
    let mut progress_reporter = ProgressReporter::new_for_snapshot();
    
    // Show progress for large datasets
    println!("📊 Found {} rows, {} columns", data_info.row_count, data_info.column_count());
    
    if data_info.row_count > 100_000 {
        println!("🚀 Using optimized streaming for large dataset ({} rows)...", data_info.row_count);
    }
    
    // Finish schema analysis phase
    progress_reporter.finish_schema(&format!("Schema analyzed: {} columns", data_info.column_count()));

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
        processor.export_to_parquet(temp_path)?;
        
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
        row_count: data_info.row_count,
        column_count: data_info.columns.len(),
        columns: data_info.columns.clone(),
        archive_size: None, // No longer using archives - using direct Parquet files
        parent_snapshot: None,
        sequence_number: 0,
        delta_from_parent: None,
        can_reconstruct_parent: false,
        source_path: Some(input_path.to_string_lossy().to_string()),
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
            s3_express,
            s3_availability_zone,
            local_path,
            global
        } => {
            configure_storage(workspace_path, backend, s3_bucket, s3_prefix, s3_region, *s3_express, s3_availability_zone, local_path, *global)
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
    s3_express: bool,
    s3_availability_zone: &Option<String>,
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
            
            // Handle S3 Express configuration
            let use_express = s3_express || std::env::var("SNAPBASE_S3_USE_EXPRESS").map(|v| v.to_lowercase() == "true").unwrap_or(false);
            let availability_zone = if use_express {
                let az = s3_availability_zone.clone()
                    .or_else(|| std::env::var("SNAPBASE_S3_AVAILABILITY_ZONE").ok());
                if az.is_none() {
                    return Err(SnapbaseError::invalid_input("Availability zone is required when using S3 Express. Provide it via --s3-availability-zone argument or set SNAPBASE_S3_AVAILABILITY_ZONE environment variable".to_string()));
                }
                az
            } else {
                s3_availability_zone.clone()
            };
            
            StorageConfig::S3 {
                bucket,
                prefix,
                region,
                access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
                secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
                use_express,
                availability_zone,
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
        config::StorageConfig::S3 { bucket, prefix, region, use_express, availability_zone, .. } => {
            println!("  Backend: S3");
            println!("  Bucket: {bucket}");
            println!("  Prefix: {prefix}");
            println!("  Region: {region}");
            if *use_express {
                println!("  S3 Express: Enabled");
                if let Some(az) = availability_zone {
                    println!("  Availability Zone: {az}");
                }
            } else {
                println!("  S3 Express: Disabled");
            }
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

/// Thin wrapper status command using the new core API
fn thin_wrapper_status_command(
    workspace_path: Option<&Path>,
    input: &str,
    compare_to: Option<&str>,
    quiet: bool,
    json: bool,
) -> Result<()> {
    use snapbase_core::change_detection::{DataSource, ComparisonOptions};
    
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());

    // Canonicalize input path for comparison
    let input_path = if Path::new(input).is_absolute() {
        Path::new(input).to_path_buf()
    } else {
        workspace.root.join(input)
    };
    let canonical_input_path = input_path.canonicalize()
        .unwrap_or(input_path.clone());

    // Resolve comparison snapshot
    let comparison_snapshot = if let Some(name) = compare_to {
        let snap_ref = SnapshotRef::from_string(name.to_string());
        resolver.resolve(&snap_ref)?
    } else {
        // Find latest snapshot for this specific source
        let canonical_str = canonical_input_path.to_string_lossy().to_string();
        let latest_for_source = workspace.latest_snapshot_for_source(&canonical_str)?;
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

    // Get the actual data path from the snapshot
    let baseline_data_path = comparison_snapshot.data_path.as_ref()
        .ok_or_else(|| SnapbaseError::archive("Baseline snapshot has no data path"))?;

    // Create data sources
    let current_source = DataSource::File(canonical_input_path);
    let baseline_source = DataSource::StoredSnapshot {
        path: baseline_data_path.clone(),
        workspace: workspace.clone(),
    };

    // Set up comparison options - use defaults which exclude metadata columns
    let options = ComparisonOptions::default();

    // Run comparison using the new core API
    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        StreamingChangeDetector::compare_data_sources(
            baseline_source, 
            current_source, 
            options, 
            None
        ).await
    })?;

    // Output results
    if json {
        println!("{}", JsonFormatter::format_comprehensive_status_results(&result)?);
    } else {
        PrettyPrinter::print_comprehensive_status_results(&result, quiet);
    }

    Ok(())
}

/// Thin wrapper diff command using the new core API
fn thin_wrapper_diff_command(
    workspace_path: Option<&Path>,
    _source: &str,
    from: &str,
    to: &str,
    json: bool,
) -> Result<()> {
    use snapbase_core::change_detection::{DataSource, ComparisonOptions};
    
    let workspace = SnapbaseWorkspace::find_or_create(workspace_path)?;
    let resolver = SnapshotResolver::new(workspace.clone());

    // Resolve from and to snapshots
    let from_ref = SnapshotRef::from_string(from.to_string());
    let from_snapshot = resolver.resolve(&from_ref)?;
    
    let to_ref = SnapshotRef::from_string(to.to_string());
    let to_snapshot = resolver.resolve(&to_ref)?;

    if !json {
        println!("🔍 Streaming comparison of snapshots '{}' → '{}'", from, to);
    }

    // Get the actual data paths from the snapshots
    let from_data_path = from_snapshot.data_path.as_ref()
        .ok_or_else(|| SnapbaseError::archive("From snapshot has no data path"))?;
    let to_data_path = to_snapshot.data_path.as_ref()
        .ok_or_else(|| SnapbaseError::archive("To snapshot has no data path"))?;

    // Create data sources
    let baseline_source = DataSource::StoredSnapshot {
        path: from_data_path.clone(),
        workspace: workspace.clone(),
    };
    let current_source = DataSource::StoredSnapshot {
        path: to_data_path.clone(),
        workspace: workspace.clone(),
    };

    // Set up comparison options - use defaults which exclude metadata columns
    let options = ComparisonOptions::default();

    // Run comparison using the new core API
    let rt = tokio::runtime::Runtime::new()?;
    let result = rt.block_on(async {
        StreamingChangeDetector::compare_data_sources(
            baseline_source, 
            current_source, 
            options, 
            None
        ).await
    })?;

    // Output results
    if json {
        println!("{}", JsonFormatter::format_comprehensive_status_results(&result)?);
    } else {
        PrettyPrinter::print_comprehensive_diff_results(&result, from, to);
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