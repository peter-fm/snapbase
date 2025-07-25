//! Workspace management for snapbase operations

use crate::config::StorageConfig;
use crate::error::Result;
use crate::storage::{LocalStorage, S3Storage, StorageBackend};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use walkdir::WalkDir;

/// Manages the .snapbase workspace directory
#[derive(Clone)]
pub struct SnapbaseWorkspace {
    /// Project root directory (where .snapbase/ lives)
    pub root: PathBuf,
    /// .snapbase/ directory path (for local storage compatibility)
    pub snapbase_dir: PathBuf,
    /// .snapbase/diffs/ directory path (for local storage compatibility)
    pub diffs_dir: PathBuf,
    /// Storage backend for cloud/local storage
    storage_backend: Arc<dyn StorageBackend>,
    /// Storage configuration
    config: StorageConfig,
}

impl std::fmt::Debug for SnapbaseWorkspace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapbaseWorkspace")
            .field("root", &self.root)
            .field("snapbase_dir", &self.snapbase_dir)
            .field("diffs_dir", &self.diffs_dir)
            .field("config", &self.config)
            .finish()
    }
}

impl SnapbaseWorkspace {
    /// Create workspace at exact path without directory traversal
    pub fn create_at_path(workspace_path: &Path) -> Result<Self> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let current_dir = std::env::current_dir()?;
            let resolved_path = if workspace_path.is_relative() {
                current_dir.join(workspace_path)
            } else {
                workspace_path.to_path_buf()
            };

            // Get storage configuration for the exact path
            let config = crate::config::get_storage_config_with_workspace(Some(&resolved_path))?;
            let storage_backend = create_storage_backend(&config).await?;

            // Create workspace at exact location without traversal
            Self::create_new(resolved_path, storage_backend, config).await
        })
    }

    /// Find existing workspace or create a new one
    pub fn find_or_create(start_dir: Option<&Path>) -> Result<Self> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let current_dir = std::env::current_dir()?;
            let start_path = if let Some(dir) = start_dir {
                if dir.as_os_str().is_empty() {
                    current_dir.clone()
                } else if dir.is_relative() {
                    current_dir.join(dir)
                } else {
                    dir.to_path_buf()
                }
            } else {
                current_dir.clone()
            };
            let start = start_path.as_path();

            // Get storage configuration (workspace-aware)
            let config = crate::config::get_storage_config_with_workspace(Some(start))?;
            let storage_backend = create_storage_backend(&config).await?;

            // First try to find existing .snapbase directory (for local storage compatibility)
            if let Some(workspace) =
                Self::find_existing(start, storage_backend.clone(), config.clone())?
            {
                return Ok(workspace);
            }

            // If not found, create in current directory or specified directory
            let root = start.to_path_buf();
            Self::create_new(root, storage_backend, config).await
        })
    }

    /// Get the storage backend
    pub fn storage(&self) -> &dyn StorageBackend {
        &*self.storage_backend
    }

    /// Get the storage configuration
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Find existing workspace - behavior depends on storage type
    fn find_existing(
        start_dir: &Path,
        storage_backend: Arc<dyn StorageBackend>,
        config: StorageConfig,
    ) -> Result<Option<Self>> {
        match config {
            StorageConfig::Local { .. } => {
                // For local storage, walk up directory tree looking for .snapbase directories
                let mut current = start_dir;

                loop {
                    let snapbase_dir = current.join(".snapbase");
                    if snapbase_dir.exists() && snapbase_dir.is_dir() {
                        return Ok(Some(Self::from_root(
                            current.to_path_buf(),
                            storage_backend,
                            config,
                        )?));
                    }

                    // Also check for .git directory as a hint for project root
                    let git_dir = current.join(".git");
                    if git_dir.exists() {
                        // Found git repo but no .snapbase, could create here
                        break;
                    }

                    match current.parent() {
                        Some(parent) => current = parent,
                        None => break, // Reached filesystem root
                    }
                }

                Ok(None)
            }
            StorageConfig::S3 { .. } => {
                // For cloud storage, only check current directory
                // Different S3 prefixes should create separate workspaces
                let snapbase_dir = start_dir.join(".snapbase");
                if snapbase_dir.exists() && snapbase_dir.is_dir() {
                    Ok(Some(Self::from_root(
                        start_dir.to_path_buf(),
                        storage_backend,
                        config,
                    )?))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Create a new workspace in the specified root directory
    pub async fn create_new(
        root: PathBuf,
        storage_backend: Arc<dyn StorageBackend>,
        config: StorageConfig,
    ) -> Result<Self> {
        let workspace = Self::from_root(root, storage_backend, config.clone())?;

        // Only create local directories if using local storage
        if matches!(config, StorageConfig::Local { .. }) {
            fs::create_dir_all(&workspace.snapbase_dir)?;
            // Note: No longer creating diffs_dir - we use SQL queries for diffs

            // Create initial config file
            workspace.create_config()?;
        }

        log::info!(
            "Created snapbase workspace at: {}",
            workspace.root.display()
        );

        Ok(workspace)
    }

    /// Create workspace from root directory path
    pub fn from_root(
        root: PathBuf,
        storage_backend: Arc<dyn StorageBackend>,
        config: StorageConfig,
    ) -> Result<Self> {
        let snapbase_dir = root.join(".snapbase");
        let diffs_dir = snapbase_dir.join("diffs");

        Ok(Self {
            root,
            snapbase_dir,
            diffs_dir,
            storage_backend,
            config,
        })
    }

    /// Get paths for a snapshot (archive and JSON)
    pub fn snapshot_paths(&self, name: &str) -> (PathBuf, PathBuf) {
        let archive_path = self.snapbase_dir.join(format!("{name}.snapbase"));
        let json_path = self.snapbase_dir.join(format!("{name}.json"));
        (archive_path, json_path)
    }

    /// Get path for a diff result
    pub fn diff_path(&self, name1: &str, name2: &str) -> PathBuf {
        self.diffs_dir.join(format!("{name1}-{name2}.json"))
    }

    /// List all available snapshots
    pub fn list_snapshots(&self) -> Result<Vec<String>> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            match self.storage_backend.list_all_snapshots().await {
                Ok(snapshots) => {
                    let mut names: Vec<String> = snapshots.into_iter().map(|s| s.name).collect();
                    names.sort();
                    Ok(names)
                }
                Err(_) => {
                    // Fallback to local filesystem for compatibility
                    let mut snapshots = Vec::new();

                    if self.snapbase_dir.exists() {
                        for entry in fs::read_dir(&self.snapbase_dir)? {
                            let entry = entry?;
                            let path = entry.path();

                            if let Some(extension) = path.extension() {
                                if extension == "json" {
                                    if let Some(stem) = path.file_stem() {
                                        if let Some(name) = stem.to_str() {
                                            // Filter out config file - only include user snapshots
                                            if name != "config" {
                                                snapshots.push(name.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    snapshots.sort();
                    Ok(snapshots)
                }
            }
        })
    }

    /// List snapshots for a specific source file
    pub fn list_snapshots_for_source(&self, source_path: &str) -> Result<Vec<String>> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            match self.storage_backend.list_all_snapshots().await {
                Ok(snapshots) => {
                    let mut source_snapshots = Vec::new();

                    for snapshot in snapshots {
                        // Check if this snapshot is from the same source
                        let is_same_source = if let Some(snapshot_source_path) =
                            &snapshot.source_path
                        {
                            // Use the stored canonical source path
                            snapshot_source_path == source_path
                        } else {
                            // Legacy snapshot without source_path, check original source field
                            let snapshot_canonical_path = std::path::Path::new(&snapshot.source)
                                .canonicalize()
                                .unwrap_or_else(|_| std::path::PathBuf::from(&snapshot.source))
                                .to_string_lossy()
                                .to_string();

                            snapshot_canonical_path == source_path
                        };

                        if is_same_source {
                            source_snapshots.push(snapshot.name);
                        }
                    }

                    source_snapshots.sort();
                    Ok(source_snapshots)
                }
                Err(_) => {
                    // Fallback to local filesystem
                    let all_snapshots = self.list_snapshots()?;
                    let mut source_snapshots = Vec::new();

                    for snapshot_name in all_snapshots {
                        let (_, json_path) = self.snapshot_paths(&snapshot_name);
                        if json_path.exists() {
                            // Load metadata to check source
                            let content = fs::read_to_string(&json_path)?;
                            let metadata: serde_json::Value = serde_json::from_str(&content)?;

                            // Check if this snapshot is from the same source
                            let is_same_source = if let Some(snapshot_source_path) =
                                metadata.get("source_path").and_then(|v| v.as_str())
                            {
                                // Use the stored canonical source path
                                snapshot_source_path == source_path
                            } else if let Some(snapshot_source) =
                                metadata.get("source").and_then(|v| v.as_str())
                            {
                                // Legacy snapshot without source_path, check original source field
                                let snapshot_canonical_path = std::path::Path::new(snapshot_source)
                                    .canonicalize()
                                    .unwrap_or_else(|_| std::path::PathBuf::from(snapshot_source))
                                    .to_string_lossy()
                                    .to_string();

                                snapshot_canonical_path == source_path
                            } else {
                                false
                            };

                            if is_same_source {
                                source_snapshots.push(snapshot_name);
                            }
                        }
                    }

                    source_snapshots.sort();
                    Ok(source_snapshots)
                }
            }
        })
    }

    /// Find the most recent snapshot for a specific source file
    pub fn latest_snapshot_for_source(&self, source_path: &str) -> Result<Option<String>> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            match self.storage_backend.list_all_snapshots().await {
                Ok(snapshots) => {
                    let mut latest_time = None;
                    let mut latest_name = None;

                    for snapshot in snapshots {
                        // Check if this snapshot is from the same source
                        let is_same_source = if let Some(snapshot_source_path) =
                            &snapshot.source_path
                        {
                            // Use the stored canonical source path
                            snapshot_source_path == source_path
                        } else {
                            // Legacy snapshot without source_path, check original source field
                            let snapshot_canonical_path = std::path::Path::new(&snapshot.source)
                                .canonicalize()
                                .unwrap_or_else(|_| std::path::PathBuf::from(&snapshot.source))
                                .to_string_lossy()
                                .to_string();

                            snapshot_canonical_path == source_path
                        };

                        if is_same_source
                            && (latest_time.is_none() || Some(snapshot.created) > latest_time)
                        {
                            latest_time = Some(snapshot.created);
                            latest_name = Some(snapshot.name);
                        }
                    }

                    Ok(latest_name)
                }
                Err(_) => {
                    // Fallback to local filesystem
                    let source_snapshots = self.list_snapshots_for_source(source_path)?;

                    if source_snapshots.is_empty() {
                        return Ok(None);
                    }

                    // Read creation times from JSON files and find latest
                    let mut latest_time = None;
                    let mut latest_name = None;

                    for name in source_snapshots {
                        let (_, json_path) = self.snapshot_paths(&name);
                        if json_path.exists() {
                            if let Ok(metadata) = fs::metadata(&json_path) {
                                if let Ok(created) = metadata.created() {
                                    if latest_time.is_none() || Some(created) > latest_time {
                                        latest_time = Some(created);
                                        latest_name = Some(name);
                                    }
                                }
                            }
                        }
                    }

                    Ok(latest_name)
                }
            }
        })
    }

    /// Find the most recent snapshot by creation time
    pub fn latest_snapshot(&self) -> Result<Option<String>> {
        let snapshots = self.list_snapshots()?;

        // Filter out config file - only consider user snapshots
        let user_snapshots: Vec<String> = snapshots
            .into_iter()
            .filter(|name| name != "config")
            .collect();

        if user_snapshots.is_empty() {
            return Ok(None);
        }

        // Read creation times from JSON files and find latest
        let mut latest_time = None;
        let mut latest_name = None;

        for name in user_snapshots {
            let (_, json_path) = self.snapshot_paths(&name);
            if json_path.exists() {
                if let Ok(metadata) = fs::metadata(&json_path) {
                    if let Ok(created) = metadata.created() {
                        if latest_time.is_none() || Some(created) > latest_time {
                            latest_time = Some(created);
                            latest_name = Some(name);
                        }
                    }
                }
            }
        }

        Ok(latest_name)
    }

    /// Check if a snapshot exists
    pub fn snapshot_exists(&self, name: &str) -> bool {
        let (_, json_path) = self.snapshot_paths(name);
        json_path.exists()
    }

    /// Create initial configuration file
    fn create_config(&self) -> Result<()> {
        self.create_config_with_force(false)
    }

    /// Create configuration file with optional force overwrite
    pub fn create_config_with_force(&self, force: bool) -> Result<()> {
        let config_path = self.root.join("snapbase.toml");

        if config_path.exists() && !force {
            return Ok(()); // Don't overwrite existing config unless forced
        }

        // Create user-friendly config with comments based on global config
        self.create_user_friendly_config(&config_path)?;
        Ok(())
    }

    /// Create a user-friendly configuration file with comments and examples
    fn create_user_friendly_config(&self, config_path: &std::path::Path) -> Result<()> {
        // Try to get global config, fall back to defaults if it doesn't exist
        let global_config = crate::config::get_config().unwrap_or_default();

        // Use the workspace's current storage config if available, otherwise use global
        let storage_config = crate::config::StorageConfigToml::from_runtime(&self.config);

        // Create the config content with helpful comments
        let storage_backend = match storage_config.backend {
            crate::config::StorageBackend::Local => "local",
            crate::config::StorageBackend::S3 => "s3",
        };

        let storage_section = self.generate_storage_section(&storage_config);
        let databases_section = self.generate_databases_section(&global_config.databases);

        let config_content = format!(
            r#"# snapbase.toml
# Local configuration file for snapbase workspace
# This file was automatically created based on your global configuration

[storage]
backend = "{storage_backend}"
{storage_section}

[snapshot]
default_name_pattern = "{}"
{databases_section}"#,
            global_config.snapshot.default_name_pattern,
        );

        // Write the config file
        fs::write(config_path, config_content)?;
        println!("📝 Created snapbase.toml configuration file");

        Ok(())
    }

    /// Generate the storage section based on the backend type
    fn generate_storage_section(&self, storage: &crate::config::StorageConfigToml) -> String {
        match storage.backend {
            crate::config::StorageBackend::Local => {
                let default_local = crate::config::LocalStorageConfig::default();
                let local_config = storage.local.as_ref().unwrap_or(&default_local);
                format!(
                    r#"
[storage.local]
path = "{}""#,
                    local_config.path.display()
                )
            }
            crate::config::StorageBackend::S3 => {
                let default_s3 = crate::config::S3StorageConfig::default();
                let s3_config = storage.s3.as_ref().unwrap_or(&default_s3);
                format!(
                    r#"
[storage.s3]
bucket = "{}"
prefix = "{}"
region = "{}"

# AWS credentials are read from environment variables:
# AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"#,
                    s3_config.bucket, s3_config.prefix, s3_config.region
                )
            }
        }
    }

    /// Generate the databases section if any databases are configured
    fn generate_databases_section(
        &self,
        databases: &std::collections::HashMap<String, crate::config::DatabaseConfig>,
    ) -> String {
        if databases.is_empty() {
            return "
# Database configurations for --database flag
# Uncomment and configure as needed:
#
# [databases.my-database]
# type = \"mysql\"
# host = \"localhost\"
# port = 3306
# database = \"myapp\"
# username = \"dbuser\"
# password_env = \"DB_PASSWORD\"
# tables = [\"*\"]
# exclude_tables = [\"temp_*\"]"
                .to_string();
        }

        let mut sections = Vec::new();
        for (name, db_config) in databases {
            let db_type = match db_config.db_type {
                crate::config::DatabaseType::Mysql => "mysql",
                crate::config::DatabaseType::Postgresql => "postgresql",
                crate::config::DatabaseType::Sqlite => "sqlite",
            };

            let mut section = format!("\n[databases.{name}]\ntype = \"{db_type}\"");

            if let Some(ref conn_str) = db_config.connection_string {
                section.push_str(&format!("\nconnection_string = \"{conn_str}\""));
            } else {
                if let Some(ref host) = db_config.host {
                    section.push_str(&format!("\nhost = \"{host}\""));
                }
                if let Some(port) = db_config.port {
                    section.push_str(&format!("\nport = {port}"));
                }
                if let Some(ref database) = db_config.database {
                    section.push_str(&format!("\ndatabase = \"{database}\""));
                }
                if let Some(ref username) = db_config.username {
                    section.push_str(&format!("\nusername = \"{username}\""));
                }
            }

            if let Some(ref password_env) = db_config.password_env {
                section.push_str(&format!("\npassword_env = \"{password_env}\""));
            }

            if !db_config.tables.is_empty() {
                section.push_str(&format!("\ntables = {:?}", db_config.tables));
            }

            if !db_config.exclude_tables.is_empty() {
                section.push_str(&format!(
                    "\nexclude_tables = {:?}",
                    db_config.exclude_tables
                ));
            }

            sections.push(section);
        }

        sections.join("\n")
    }

    /// Save storage configuration to workspace config
    pub fn save_storage_config(&self, storage_config: &crate::config::StorageConfig) -> Result<()> {
        // Create/update snapbase.toml file in the workspace root
        let toml_config_path = self.root.join("snapbase.toml");
        let toml_config = crate::config::Config {
            storage: crate::config::StorageConfigToml::from_runtime(storage_config),
            snapshot: crate::config::SnapshotConfig::default(),
            databases: std::collections::HashMap::new(),
        };
        let toml_content = toml::to_string_pretty(&toml_config)?;
        fs::write(toml_config_path, toml_content)?;

        Ok(())
    }

    /// Load storage configuration from workspace config
    pub fn load_storage_config(&self) -> Result<Option<crate::config::StorageConfig>> {
        // Load from snapbase.toml in workspace root
        let toml_config_path = self.root.join("snapbase.toml");
        if toml_config_path.exists() {
            let content = fs::read_to_string(&toml_config_path)?;
            if let Ok(config) = toml::from_str::<crate::config::Config>(&content) {
                return Ok(Some(config.storage.to_runtime()));
            }
        }

        Ok(None)
    }

    /// Get workspace statistics
    pub fn stats(&self) -> Result<WorkspaceStats> {
        let snapshots = self.list_snapshots()?;
        let mut total_archive_size = 0u64;
        let mut total_json_size = 0u64;

        for name in &snapshots {
            let (archive_path, json_path) = self.snapshot_paths(name);

            if archive_path.exists() {
                if let Ok(metadata) = fs::metadata(&archive_path) {
                    total_archive_size += metadata.len();
                }
            }

            if json_path.exists() {
                if let Ok(metadata) = fs::metadata(&json_path) {
                    total_json_size += metadata.len();
                }
            }
        }

        // Count diff files
        let mut diff_count = 0;
        let mut total_diff_size = 0u64;

        if self.diffs_dir.exists() {
            for entry in WalkDir::new(&self.diffs_dir) {
                let entry = entry?;
                if entry.file_type().is_file() {
                    diff_count += 1;
                    total_diff_size += entry.metadata()?.len();
                }
            }
        }

        Ok(WorkspaceStats {
            snapshot_count: snapshots.len(),
            diff_count,
            total_archive_size,
            total_json_size,
            total_diff_size,
        })
    }

    /// Clean up old or unused files
    pub fn cleanup(&self, keep_latest: usize) -> Result<CleanupStats> {
        let snapshots = self.list_snapshots()?;

        if snapshots.len() <= keep_latest {
            return Ok(CleanupStats::default());
        }

        // Sort by creation time and remove oldest
        let mut snapshots_with_time = Vec::new();

        for name in snapshots {
            let (_, json_path) = self.snapshot_paths(&name);
            if let Ok(metadata) = fs::metadata(&json_path) {
                if let Ok(created) = metadata.created() {
                    snapshots_with_time.push((name, created));
                }
            }
        }

        snapshots_with_time.sort_by_key(|(_, time)| *time);

        let mut stats = CleanupStats::default();

        // Remove oldest snapshots beyond keep_latest
        for (name, _) in snapshots_with_time
            .iter()
            .take(snapshots_with_time.len().saturating_sub(keep_latest))
        {
            let (archive_path, _json_path) = self.snapshot_paths(name);

            if archive_path.exists() {
                if let Ok(metadata) = fs::metadata(&archive_path) {
                    stats.archives_removed += 1;
                    stats.bytes_freed += metadata.len();
                }
                fs::remove_file(archive_path)?;
            }

            // Note: We keep JSON files for Git history
            log::info!("Removed old snapshot archive: {name}");
        }

        Ok(stats)
    }

    /// Check if using cloud storage backend
    pub fn is_cloud_storage(&self) -> bool {
        matches!(self.config, StorageConfig::S3 { .. })
    }

    /// Check if a snapshot with the given name exists for a specific source
    pub fn snapshot_exists_for_source(
        &self,
        source_name: &str,
        snapshot_name: &str,
    ) -> Result<bool> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            // Get all snapshots for this source from the storage backend
            let all_snapshots = self.storage().list_snapshots_for_all_sources().await?;

            // Use the source name (filename) as the key, same as hive directory structure
            let source_key = source_name.to_string();

            // Check if this snapshot name exists for this source
            if let Some(snapshots) = all_snapshots.get(&source_key) {
                Ok(snapshots.contains(&snapshot_name.to_string()))
            } else {
                Ok(false)
            }
        })
    }
}

/// Statistics about the workspace
#[derive(Debug, Default)]
pub struct WorkspaceStats {
    pub snapshot_count: usize,
    pub diff_count: usize,
    pub total_archive_size: u64,
    pub total_json_size: u64,
    pub total_diff_size: u64,
}

/// Statistics about cleanup operations
#[derive(Debug, Default)]
pub struct CleanupStats {
    pub archives_removed: usize,
    pub bytes_freed: u64,
}

/// Create storage backend based on configuration
pub async fn create_storage_backend(config: &StorageConfig) -> Result<Arc<dyn StorageBackend>> {
    match config {
        StorageConfig::Local { path } => Ok(Arc::new(LocalStorage::new(path.clone()))),
        StorageConfig::S3 {
            bucket,
            prefix,
            region,
            use_express,
            availability_zone,
            ..
        } => {
            let s3_storage = S3Storage::new(
                bucket.clone(),
                prefix.clone(),
                region.clone(),
                *use_express,
                availability_zone.clone(),
            )
            .await?;
            Ok(Arc::new(s3_storage))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_workspace_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let storage_backend = create_storage_backend(&config).await.unwrap();
        let workspace =
            SnapbaseWorkspace::create_new(temp_dir.path().to_path_buf(), storage_backend, config)
                .await
                .unwrap();

        assert!(workspace.snapbase_dir.exists());
    }

    #[tokio::test]
    async fn test_snapshot_paths() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let storage_backend = create_storage_backend(&config).await.unwrap();
        let workspace =
            SnapbaseWorkspace::from_root(temp_dir.path().to_path_buf(), storage_backend, config)
                .unwrap();

        let (archive, json) = workspace.snapshot_paths("test");
        assert_eq!(archive.file_name().unwrap(), "test.snapbase");
        assert_eq!(json.file_name().unwrap(), "test.json");
    }
}
