use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    pub storage: StorageConfigToml,
    pub snapshot: SnapshotConfig,
    #[serde(default)]
    pub databases: HashMap<String, DatabaseConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConfig {
    pub default_name_pattern: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    Mysql,
    Postgresql,
    Sqlite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    #[serde(rename = "type")]
    pub db_type: DatabaseType,

    /// Direct connection string (alternative to individual fields)
    pub connection_string: Option<String>,

    /// Individual connection fields
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub username: Option<String>,

    /// Environment variable containing the password
    pub password_env: Option<String>,

    /// Tables to include (supports patterns like "users" or "*" for all)
    #[serde(default)]
    pub tables: Vec<String>,

    /// Tables to exclude (supports patterns like "temp_*")
    #[serde(default)]
    pub exclude_tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    Local,
    S3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStorageConfig {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3StorageConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    /// Enable S3 Express One Zone (Directory Buckets) support
    #[serde(default)]
    pub use_express: bool,
    /// Availability zone for S3 Express (e.g., "use1-az5")
    pub availability_zone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfigToml {
    pub backend: StorageBackend,
    #[serde(default)]
    pub local: Option<LocalStorageConfig>,
    #[serde(default)]
    pub s3: Option<S3StorageConfig>,
}

// Runtime config that includes credentials for existing code
#[derive(Debug, Clone)]
pub enum StorageConfig {
    Local {
        path: PathBuf,
    },
    S3 {
        bucket: String,
        prefix: String,
        region: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        use_express: bool,
        availability_zone: Option<String>,
    },
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfigToml::default().to_runtime()
    }
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            default_name_pattern: "{source}_{format}_{seq}".to_string(),
        }
    }
}

impl Default for StorageBackend {
    fn default() -> Self {
        Self::Local
    }
}

impl Default for LocalStorageConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from(".snapbase"),
        }
    }
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        Self {
            bucket: "my-snapbase-bucket".to_string(),
            prefix: "project-name/".to_string(),
            region: "us-west-2".to_string(),
            use_express: false,
            availability_zone: None,
        }
    }
}

impl Default for StorageConfigToml {
    fn default() -> Self {
        Self {
            backend: StorageBackend::default(),
            local: Some(LocalStorageConfig::default()),
            s3: None,
        }
    }
}

impl StorageConfigToml {
    pub fn to_runtime(&self) -> StorageConfig {
        match self.backend {
            StorageBackend::Local => {
                let default_local = LocalStorageConfig::default();
                let local_config = self.local.as_ref().unwrap_or(&default_local);
                StorageConfig::Local {
                    path: local_config.path.clone(),
                }
            }
            StorageBackend::S3 => {
                let default_s3 = S3StorageConfig::default();
                let s3_config = self.s3.as_ref().unwrap_or(&default_s3);
                StorageConfig::S3 {
                    bucket: s3_config.bucket.clone(),
                    prefix: s3_config.prefix.clone(),
                    region: s3_config.region.clone(),
                    access_key_id: env::var("AWS_ACCESS_KEY_ID").ok(),
                    secret_access_key: env::var("AWS_SECRET_ACCESS_KEY").ok(),
                    use_express: s3_config.use_express,
                    availability_zone: s3_config.availability_zone.clone(),
                }
            }
        }
    }

    pub fn from_runtime(runtime_config: &StorageConfig) -> Self {
        match runtime_config {
            StorageConfig::Local { path } => Self {
                backend: StorageBackend::Local,
                local: Some(LocalStorageConfig { path: path.clone() }),
                s3: None,
            },
            StorageConfig::S3 {
                bucket,
                prefix,
                region,
                use_express,
                availability_zone,
                ..
            } => Self {
                backend: StorageBackend::S3,
                local: None,
                s3: Some(S3StorageConfig {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    region: region.clone(),
                    use_express: *use_express,
                    availability_zone: availability_zone.clone(),
                }),
            },
        }
    }
}

pub fn get_config() -> Result<Config> {
    // Priority order (highest to lowest):
    // 1. Explicit config file via SNAPBASE_CONFIG env var
    // 2. Local config file (snapbase.toml)
    // 3. Saved global config file (~/.snapbase/global.toml)
    // 4. Default configuration

    let mut config = Config::default();

    // 1. Check config file via environment variable (highest priority)
    if let Ok(config_path) = env::var("SNAPBASE_CONFIG") {
        if let Ok(config_content) = fs::read_to_string(config_path) {
            if let Ok(loaded_config) = toml::from_str::<Config>(&config_content) {
                return Ok(loaded_config);
            }
        }
    }

    // 2. Check saved global config file location (fallback)
    if let Some(home_dir) = dirs::home_dir() {
        let config_path = home_dir.join(".snapbase").join("global.toml");
        if config_path.exists() {
            if let Ok(config_content) = fs::read_to_string(config_path) {
                if let Ok(loaded_config) = toml::from_str::<Config>(&config_content) {
                    config = loaded_config;
                }
            }
        }
    }

    // 3. Load local config file (snapbase.toml) if it exists - takes precedence over global
    if let Ok(current_dir) = env::current_dir() {
        let local_config_path = current_dir.join("snapbase.toml");
        if local_config_path.exists() {
            if let Ok(local_content) = fs::read_to_string(local_config_path) {
                if let Ok(local_config) = toml::from_str::<Config>(&local_content) {
                    // Merge local config with global config (local takes precedence)
                    config.storage = local_config.storage;
                    config.snapshot = local_config.snapshot;
                    config.databases = local_config.databases;
                }
            }
        }
    }

    // Override snapshot config with environment variables for backward compatibility
    if let Ok(pattern) = env::var("SNAPBASE_DEFAULT_NAME_PATTERN") {
        config.snapshot.default_name_pattern = pattern;
    }

    Ok(config)
}

pub fn get_storage_config() -> Result<StorageConfig> {
    Ok(get_config()?.storage.to_runtime())
}

pub fn get_storage_config_from_env_or_default() -> Result<StorageConfig> {
    // Skip saved config file and only use environment variables or default

    // Check environment variables (for backward compatibility)
    if let Ok(bucket) = std::env::var("SNAPBASE_S3_BUCKET") {
        return Ok(StorageConfig::S3 {
            bucket,
            prefix: std::env::var("SNAPBASE_S3_PREFIX").unwrap_or_default(),
            region: std::env::var("SNAPBASE_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
            use_express: std::env::var("SNAPBASE_S3_USE_EXPRESS")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false),
            availability_zone: std::env::var("SNAPBASE_S3_AVAILABILITY_ZONE").ok(),
        });
    }

    // Default to local storage
    Ok(StorageConfigToml::default().to_runtime())
}

pub fn get_storage_config_project_first() -> Result<StorageConfig> {
    // Project-first priority: snapbase.toml → defaults
    // This is used for regular init to prioritize local project settings

    // Load local config file (snapbase.toml) if it exists
    if let Ok(current_dir) = std::env::current_dir() {
        let local_config_path = current_dir.join("snapbase.toml");
        if local_config_path.exists() {
            if let Ok(local_content) = fs::read_to_string(local_config_path) {
                if let Ok(local_config) = toml::from_str::<Config>(&local_content) {
                    return Ok(local_config.storage.to_runtime());
                }
            }
        }
    }

    // Check environment variables (for backward compatibility)
    if let Ok(bucket) = std::env::var("SNAPBASE_S3_BUCKET") {
        return Ok(StorageConfig::S3 {
            bucket,
            prefix: std::env::var("SNAPBASE_S3_PREFIX").unwrap_or_default(),
            region: std::env::var("SNAPBASE_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID").ok(),
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
            use_express: std::env::var("SNAPBASE_S3_USE_EXPRESS")
                .map(|v| v.to_lowercase() == "true")
                .unwrap_or(false),
            availability_zone: std::env::var("SNAPBASE_S3_AVAILABILITY_ZONE").ok(),
        });
    }

    // Default to local storage
    Ok(StorageConfigToml::default().to_runtime())
}

pub fn save_config(config: &Config) -> Result<()> {
    let config_dir = if let Some(home_dir) = dirs::home_dir() {
        home_dir.join(".snapbase")
    } else {
        PathBuf::from(".snapbase")
    };

    fs::create_dir_all(&config_dir)?;

    let config_path = config_dir.join("global.toml");
    let config_toml = toml::to_string_pretty(config)?;
    fs::write(config_path, config_toml)?;

    Ok(())
}

pub fn save_storage_config(config: &StorageConfig) -> Result<()> {
    let mut full_config = get_config()?;
    // Convert runtime config back to toml format
    full_config.storage = StorageConfigToml::from_runtime(config);
    save_config(&full_config)
}

pub fn get_snapshot_config() -> Result<SnapshotConfig> {
    Ok(get_config()?.snapshot)
}

/// Get snapshot configuration with workspace context
pub fn get_snapshot_config_with_workspace(
    workspace_path: Option<&std::path::Path>,
) -> Result<SnapshotConfig> {
    // Priority order: workspace config → global config → env vars → defaults

    // 1. Check workspace config first (if workspace path provided)
    if let Some(workspace_path) = workspace_path {
        let workspace_toml_path = workspace_path.join("snapbase.toml");
        if workspace_toml_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&workspace_toml_path) {
                if let Ok(config) = toml::from_str::<Config>(&content) {
                    return Ok(config.snapshot);
                }
            }
        }
    }

    // 2. Fall back to global config → env vars → defaults
    get_snapshot_config()
}

pub fn save_default_name_pattern(pattern: &str) -> Result<()> {
    let mut config = get_config()?;
    config.snapshot.default_name_pattern = pattern.to_string();
    save_config(&config)
}

pub fn save_global_config_if_missing(storage_config: &StorageConfig) -> Result<()> {
    // Check if global config file exists
    let config_dir = if let Some(home_dir) = dirs::home_dir() {
        home_dir.join(".snapbase")
    } else {
        PathBuf::from(".snapbase")
    };
    let config_path = config_dir.join("global.toml");

    // Only save if the file doesn't exist
    if !config_path.exists() {
        let config = Config {
            storage: StorageConfigToml::from_runtime(storage_config),
            ..Default::default()
        };
        save_config(&config)?;
    }

    Ok(())
}

pub fn get_storage_config_with_workspace(
    workspace_path: Option<&std::path::Path>,
) -> Result<StorageConfig> {
    // Priority order: workspace config → global config → env vars → defaults

    // 1. Check workspace config first (if workspace path provided)
    if let Some(workspace_path) = workspace_path {
        let root = workspace_path.to_path_buf();

        // Check for snapbase.toml in workspace directory
        let workspace_toml_path = root.join("snapbase.toml");
        if workspace_toml_path.exists() {
            if let Ok(content) = std::fs::read_to_string(&workspace_toml_path) {
                if let Ok(config) = toml::from_str::<Config>(&content) {
                    return Ok(config.storage.to_runtime());
                }
            }
        }
    }

    // 2. Fall back to global config → env vars → defaults
    let mut config = get_storage_config()?;

    // If we have a workspace path and the config uses local storage with relative path,
    // resolve it relative to the workspace, not current directory
    if let Some(workspace_path) = workspace_path {
        if let StorageConfig::Local { ref mut path } = config {
            if path.is_relative() {
                let relative_path = path.clone();
                *path = workspace_path.join(relative_path);
            } else if !path.starts_with(workspace_path) {
                // Additional safeguard: if the path is absolute but doesn't start with workspace path,
                // it might be a stale absolute path. For workspace isolation, force it to be within workspace.
                *path = workspace_path.join(".snapbase");
            }
        }
    }

    Ok(config)
}

impl DatabaseConfig {
    /// Build a connection string from the configuration
    pub fn build_connection_string(&self) -> Result<String> {
        if let Some(ref conn_str) = self.connection_string {
            // Use provided connection string, substitute password if needed
            let mut result = conn_str.clone();
            if let Some(ref password_env) = self.password_env {
                if let Ok(password) = env::var(password_env) {
                    result = result.replace("{password}", &password);
                }
            }
            return Ok(result);
        }

        // Build connection string from individual fields
        let host = self
            .host
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Database host is required"))?;
        let database = self
            .database
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Database name is required"))?;
        let username = self
            .username
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Database username is required"))?;

        let password = if let Some(ref password_env) = self.password_env {
            env::var(password_env).map_err(|_| {
                anyhow::anyhow!("Password environment variable '{}' not found", password_env)
            })?
        } else {
            return Err(anyhow::anyhow!(
                "Database password or password_env is required"
            ));
        };

        let port = self.port.unwrap_or(match self.db_type {
            DatabaseType::Mysql => 3306,
            DatabaseType::Postgresql => 5432,
            DatabaseType::Sqlite => 0, // Not used for SQLite
        });

        let connection_string = match self.db_type {
            DatabaseType::Mysql => {
                format!("mysql://{username}:{password}@{host}:{port}/{database}")
            }
            DatabaseType::Postgresql => {
                format!("postgresql://{username}:{password}@{host}:{port}/{database}")
            }
            DatabaseType::Sqlite => {
                // For SQLite, database field is the file path
                format!("sqlite://{database}")
            }
        };

        Ok(connection_string)
    }

    /// Get the tables to include, handling patterns
    pub fn get_included_tables(&self) -> Vec<String> {
        if self.tables.is_empty() || self.tables.contains(&"*".to_string()) {
            vec!["*".to_string()]
        } else {
            self.tables.clone()
        }
    }

    /// Get the tables to exclude, handling patterns
    pub fn get_excluded_tables(&self) -> Vec<String> {
        self.exclude_tables.clone()
    }
}

/// Get database configuration by name
pub fn get_database_config(name: &str) -> Result<DatabaseConfig> {
    let config = get_config()?;
    config
        .databases
        .get(name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Database '{}' not found in configuration", name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_config_connection_string() {
        env::set_var("TEST_DB_PASSWORD", "testpass");

        // Test with individual fields
        let config = DatabaseConfig {
            db_type: DatabaseType::Mysql,
            connection_string: None,
            host: Some("localhost".to_string()),
            port: Some(3306),
            database: Some("testdb".to_string()),
            username: Some("testuser".to_string()),
            password_env: Some("TEST_DB_PASSWORD".to_string()),
            tables: vec![],
            exclude_tables: vec![],
        };

        let conn_str = config.build_connection_string().unwrap();
        assert_eq!(conn_str, "mysql://testuser:testpass@localhost:3306/testdb");

        // Test with connection string template
        let config = DatabaseConfig {
            db_type: DatabaseType::Postgresql,
            connection_string: Some("postgresql://user:{password}@host:5432/db".to_string()),
            host: None,
            port: None,
            database: None,
            username: None,
            password_env: Some("TEST_DB_PASSWORD".to_string()),
            tables: vec![],
            exclude_tables: vec![],
        };

        let conn_str = config.build_connection_string().unwrap();
        assert_eq!(conn_str, "postgresql://user:testpass@host:5432/db");

        env::remove_var("TEST_DB_PASSWORD");
    }

    #[test]
    fn test_database_config_table_filtering() {
        let config = DatabaseConfig {
            db_type: DatabaseType::Mysql,
            connection_string: None,
            host: None,
            port: None,
            database: None,
            username: None,
            password_env: None,
            tables: vec!["users".to_string(), "orders".to_string()],
            exclude_tables: vec!["temp_*".to_string()],
        };

        let included = config.get_included_tables();
        assert_eq!(included, vec!["users", "orders"]);

        let excluded = config.get_excluded_tables();
        assert_eq!(excluded, vec!["temp_*"]);

        // Test wildcard
        let config = DatabaseConfig {
            db_type: DatabaseType::Mysql,
            connection_string: None,
            host: None,
            port: None,
            database: None,
            username: None,
            password_env: None,
            tables: vec!["*".to_string()],
            exclude_tables: vec![],
        };

        let included = config.get_included_tables();
        assert_eq!(included, vec!["*"]);
    }

    #[test]
    fn test_s3_express_config() {
        // Test S3 Express configuration
        let toml_content = r#"
[storage]
backend = "s3"

[storage.s3]
bucket = "my-express-bucket"
prefix = "data/"
region = "us-east-1"
use_express = true
availability_zone = "use1-az5"

[snapshot]
default_name_pattern = "{source}_{seq}"
"#;

        let config: Config = toml::from_str(toml_content).unwrap();
        assert_eq!(config.storage.backend, StorageBackend::S3);

        let s3_config = config.storage.s3.as_ref().unwrap();
        assert_eq!(s3_config.bucket, "my-express-bucket");
        assert_eq!(s3_config.prefix, "data/");
        assert_eq!(s3_config.region, "us-east-1");
        assert!(s3_config.use_express);
        assert_eq!(s3_config.availability_zone, Some("use1-az5".to_string()));

        // Test runtime conversion
        let runtime_config = config.storage.to_runtime();
        match runtime_config {
            StorageConfig::S3 {
                bucket,
                prefix,
                region,
                use_express,
                availability_zone,
                ..
            } => {
                assert_eq!(bucket, "my-express-bucket");
                assert_eq!(prefix, "data/");
                assert_eq!(region, "us-east-1");
                assert!(use_express);
                assert_eq!(availability_zone, Some("use1-az5".to_string()));
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_config_with_databases() {
        let mut config = Config::default();

        let db_config = DatabaseConfig {
            db_type: DatabaseType::Mysql,
            connection_string: None,
            host: Some("localhost".to_string()),
            port: Some(3306),
            database: Some("testdb".to_string()),
            username: Some("testuser".to_string()),
            password_env: Some("TEST_PASSWORD".to_string()),
            tables: vec!["users".to_string()],
            exclude_tables: vec![],
        };

        config.databases.insert("test-db".to_string(), db_config);

        assert!(config.databases.contains_key("test-db"));
        assert_eq!(
            config.databases["test-db"].host,
            Some("localhost".to_string())
        );
        assert_eq!(config.databases["test-db"].tables, vec!["users"]);
    }

    #[test]
    fn test_config_s3_only_no_local_section() {
        // Test that S3 config works without requiring local section
        let toml_content = r#"
[storage]
backend = "s3"

[storage.s3]
bucket = "my-bucket"
prefix = "my-prefix"
region = "us-west-2"

[snapshot]
default_name_pattern = "{source}_{seq}"
"#;

        let config: Config = toml::from_str(toml_content).unwrap();
        assert_eq!(config.storage.backend, StorageBackend::S3);
        assert!(config.storage.local.is_none());
        assert!(config.storage.s3.is_some());

        let s3_config = config.storage.s3.as_ref().unwrap();
        assert_eq!(s3_config.bucket, "my-bucket");
        assert_eq!(s3_config.prefix, "my-prefix");
        assert_eq!(s3_config.region, "us-west-2");

        // Test that to_runtime() works correctly
        let runtime_config = config.storage.to_runtime();
        match runtime_config {
            StorageConfig::S3 {
                bucket,
                prefix,
                region,
                use_express,
                availability_zone,
                ..
            } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(prefix, "my-prefix");
                assert_eq!(region, "us-west-2");
                assert!(!use_express);
                assert_eq!(availability_zone, None);
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_s3_express_environment_variables() {
        // Test S3 Express configuration from environment variables
        std::env::set_var("SNAPBASE_S3_BUCKET", "my-express-bucket");
        std::env::set_var("SNAPBASE_S3_PREFIX", "test-prefix");
        std::env::set_var("SNAPBASE_S3_REGION", "us-east-1");
        std::env::set_var("SNAPBASE_S3_USE_EXPRESS", "true");
        std::env::set_var("SNAPBASE_S3_AVAILABILITY_ZONE", "use1-az4");

        let config = get_storage_config_from_env_or_default().unwrap();

        match config {
            StorageConfig::S3 {
                bucket,
                prefix,
                region,
                use_express,
                availability_zone,
                ..
            } => {
                assert_eq!(bucket, "my-express-bucket");
                assert_eq!(prefix, "test-prefix");
                assert_eq!(region, "us-east-1");
                assert!(use_express);
                assert_eq!(availability_zone, Some("use1-az4".to_string()));
            }
            _ => panic!("Expected S3 config"),
        }

        // Cleanup
        std::env::remove_var("SNAPBASE_S3_BUCKET");
        std::env::remove_var("SNAPBASE_S3_PREFIX");
        std::env::remove_var("SNAPBASE_S3_REGION");
        std::env::remove_var("SNAPBASE_S3_USE_EXPRESS");
        std::env::remove_var("SNAPBASE_S3_AVAILABILITY_ZONE");
    }

    #[test]
    fn test_config_local_only_no_s3_section() {
        // Test that local config works without requiring S3 section
        let toml_content = r#"
[storage]
backend = "local"

[storage.local]
path = "/tmp/snapbase"

[snapshot]
default_name_pattern = "{source}_{seq}"
"#;

        let config: Config = toml::from_str(toml_content).unwrap();
        assert_eq!(config.storage.backend, StorageBackend::Local);
        assert!(config.storage.local.is_some());
        assert!(config.storage.s3.is_none());

        let local_config = config.storage.local.as_ref().unwrap();
        assert_eq!(local_config.path, PathBuf::from("/tmp/snapbase"));

        // Test that to_runtime() works correctly
        let runtime_config = config.storage.to_runtime();
        match runtime_config {
            StorageConfig::Local { path } => {
                assert_eq!(path, PathBuf::from("/tmp/snapbase"));
            }
            _ => panic!("Expected Local config"),
        }
    }
}

/// Configuration resolution information for debugging
#[derive(Debug, Clone)]
pub struct ConfigResolutionInfo {
    pub config_source: String,
    pub config_path: Option<String>,
    pub workspace_path: Option<String>,
    pub resolution_order: Vec<String>,
}

/// Get detailed information about configuration resolution
pub fn get_config_resolution_info(
    workspace_path: Option<&std::path::Path>,
) -> Result<ConfigResolutionInfo> {
    let mut resolution_order = Vec::new();
    let mut config_source = "default".to_string();
    let mut config_path = None;

    // Check resolution order
    if let Ok(env_config) = std::env::var("SNAPBASE_CONFIG") {
        resolution_order.push(format!(
            "SNAPBASE_CONFIG environment variable: {}",
            env_config
        ));
        if std::path::Path::new(&env_config).exists() {
            config_source = "environment_variable".to_string();
            config_path = Some(env_config);
        }
    }

    // Check workspace config
    if let Some(workspace_path) = workspace_path {
        let workspace_toml_path = workspace_path.join("snapbase.toml");
        resolution_order.push(format!(
            "Workspace config: {}",
            workspace_toml_path.display()
        ));
        if workspace_toml_path.exists() {
            config_source = "workspace".to_string();
            config_path = Some(workspace_toml_path.to_string_lossy().to_string());
        }
    }

    // Check current directory config (only if no workspace was specified or workspace config not found)
    let current_dir_toml = std::env::current_dir()?.join("snapbase.toml");
    if workspace_path.is_none() || config_source == "default" {
        resolution_order.push(format!(
            "Current directory config: {}",
            current_dir_toml.display()
        ));
        if config_source == "default" && current_dir_toml.exists() {
            config_source = "current_directory".to_string();
            config_path = Some(current_dir_toml.to_string_lossy().to_string());
        }
    }

    // Check global config
    let config_dir = if let Some(home_dir) = dirs::home_dir() {
        home_dir.join(".snapbase")
    } else {
        PathBuf::from(".snapbase")
    };
    let global_toml_path = config_dir.join("global.toml");
    resolution_order.push(format!("Global config: {}", global_toml_path.display()));
    if config_source == "default" && global_toml_path.exists() {
        config_source = "global".to_string();
        config_path = Some(global_toml_path.to_string_lossy().to_string());
    }

    resolution_order.push("Built-in defaults".to_string());

    Ok(ConfigResolutionInfo {
        config_source,
        config_path,
        workspace_path: workspace_path.map(|p| p.to_string_lossy().to_string()),
        resolution_order,
    })
}

/// Get storage configuration with resolution info
pub fn get_storage_config_with_resolution_info(
    workspace_path: Option<&std::path::Path>,
) -> Result<(StorageConfig, ConfigResolutionInfo)> {
    let resolution_info = get_config_resolution_info(workspace_path)?;
    let storage_config = get_storage_config_with_workspace(workspace_path)?;
    Ok((storage_config, resolution_info))
}
