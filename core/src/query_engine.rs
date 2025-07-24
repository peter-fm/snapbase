use crate::config::StorageConfig;
use crate::workspace::SnapbaseWorkspace;
use anyhow::Result;
use duckdb::Connection;

/// Configure DuckDB for different storage backends
pub fn configure_duckdb_for_storage(
    connection: &Connection,
    config: &StorageConfig,
) -> Result<()> {
    match config {
        StorageConfig::S3 { region, use_express, availability_zone, .. } => {
            // Ensure .env file is loaded before accessing credentials
            if let Ok(current_dir) = std::env::current_dir() {
                let env_file = current_dir.join(".env");
                if env_file.exists() {
                    if let Err(e) = dotenv::from_filename(&env_file) {
                        log::warn!("Failed to load .env file: {e}");
                    }
                }
            }
            
            // Configure extension directory to use snapbase's config location for all extensions
            let home_dir = dirs::home_dir().unwrap_or_else(|| std::env::current_dir().unwrap());
            let extension_dir = home_dir.join(".snapbase").join("extensions");
            std::fs::create_dir_all(&extension_dir)?;
            
            connection.execute(&format!("SET extension_directory='{}'", extension_dir.display()), [])?;
            
            // Install and load S3 extension (cached in ~/.snapbase/extensions/)
            connection.execute("INSTALL httpfs", [])?;
            connection.execute("LOAD httpfs", [])?;
            
            // Configure S3 settings
            connection.execute(&format!("SET s3_region='{region}'"), [])?;
            
            // Configure credentials from environment or config
            let access_key = if let StorageConfig::S3 { access_key_id: Some(key), .. } = config {
                Some(key.clone())
            } else {
                std::env::var("AWS_ACCESS_KEY_ID").ok()
            };
            
            let secret_key = if let StorageConfig::S3 { secret_access_key: Some(key), .. } = config {
                Some(key.clone())
            } else {
                std::env::var("AWS_SECRET_ACCESS_KEY").ok()
            };
            
            if let Some(access_key) = access_key {
                connection.execute(&format!("SET s3_access_key_id='{access_key}'"), [])?;
            } else {
                log::error!("❌ No AWS_ACCESS_KEY_ID found in environment or config");
                return Err(anyhow::anyhow!("AWS_ACCESS_KEY_ID not found. Please set it in your .env file or environment."));
            }
            
            if let Some(secret_key) = secret_key {
                connection.execute(&format!("SET s3_secret_access_key='{secret_key}'"), [])?;
            } else {
                log::error!("❌ No AWS_SECRET_ACCESS_KEY found in environment or config");
                return Err(anyhow::anyhow!("AWS_SECRET_ACCESS_KEY not found. Please set it in your .env file or environment."));
            }
            
            // Optional: Configure session token for temporary credentials
            if let Ok(session_token) = std::env::var("AWS_SESSION_TOKEN") {
                connection.execute(&format!("SET s3_session_token='{session_token}'"), [])?;
            }
            
            // Configure endpoint for S3 Express or S3-compatible services
            if *use_express {
                if let Some(ref az) = availability_zone {
                    let endpoint = format!("s3express-{}.{}.amazonaws.com", az, region);
                    connection.execute(&format!("SET s3_endpoint='{endpoint}'"), [])?;
                } else {
                    log::warn!("⚠️  S3 Express enabled but no availability zone specified");
                }
            } else if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
                connection.execute(&format!("SET s3_endpoint='{endpoint}'"), [])?;
            }
            
            // Test S3 connection by trying to list the bucket
            let bucket_name = if *use_express {
                if let Some(ref az) = availability_zone {
                    if let StorageConfig::S3 { bucket, .. } = config {
                        format!("{}--{}--x-s3", bucket, az)
                    } else { "".to_string() }
                } else {
                    if let StorageConfig::S3 { bucket, .. } = config { bucket.clone() } else { "".to_string() }
                }
            } else {
                if let StorageConfig::S3 { bucket, .. } = config { bucket.clone() } else { "".to_string() }
            };
            let test_query = format!("SELECT * FROM read_parquet('s3://{}/non-existent-test-file.parquet') LIMIT 1", bucket_name);
            match connection.execute(&test_query, []) {
                Ok(_) => log::info!("✅ S3 connection test passed"),
                Err(e) => {
                    let err_msg = e.to_string();
                    if err_msg.contains("404") || err_msg.contains("does not exist") {
                    } else if err_msg.contains("403") {
                        log::error!("❌ S3 bucket access denied - check credentials and bucket permissions");
                    } else if err_msg.contains("400") {
                        log::error!("❌ S3 configuration error - check region and bucket name");
                    } else {
                        log::warn!("S3 connection test result: {err_msg}");
                    }
                }
            }
            
        }
        StorageConfig::Local { .. } => {
            // No special configuration needed for local storage
            log::debug!("Using local storage - no DuckDB configuration needed");
        }
    }
    
    Ok(())
}

/// Create a DuckDB connection configured for the workspace storage backend
pub fn create_configured_connection(workspace: &SnapbaseWorkspace) -> Result<Connection> {
    let connection = Connection::open_in_memory()?;
    
    // Configure DuckDB for optimal performance
    connection.execute("SET memory_limit='8GB'", [])?;
    // DuckDB auto-detects optimal thread count by default, no need to set explicitly
    
    // Configure storage backend
    configure_duckdb_for_storage(&connection, workspace.config())?;
    
    Ok(connection)
}

/// Register a Hive-partitioned view for querying snapshots
pub fn register_hive_view(
    connection: &Connection,
    workspace: &SnapbaseWorkspace,
    source: &str,
    view_name: &str,
) -> Result<()> {
    // Build query path for Hive partitioning using storage backend's get_duckdb_path method
    // This ensures proper S3 Express directory bucket naming
    let query_path = workspace.storage().get_duckdb_path(&format!("sources/{}/*/*/data.parquet", source));
    
    // Register Hive-partitioned view with union by name for schema evolution
    connection.execute(&format!(
        "CREATE OR REPLACE VIEW {view_name} AS SELECT * 
         FROM read_parquet('{query_path}', hive_partitioning=true, union_by_name=true)"
    ), [])?;
    
    log::debug!("Registered Hive view '{view_name}' for source '{source}' at path: {query_path}");
    
    Ok(())
}