use anyhow::Result;
use async_trait::async_trait;
use crate::snapshot::SnapshotMetadata;
use indicatif::ProgressBar;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Get the base path for this storage backend
    fn get_base_path(&self) -> String;
    
    /// Ensure directory exists (no-op for S3)
    async fn ensure_directory(&self, path: &str) -> Result<()>;
    
    /// Write file to storage
    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()>;
    
    /// Write file to storage with progress tracking
    async fn write_file_with_progress(&self, path: &str, data: &[u8], progress_bar: Option<&ProgressBar>) -> Result<()>;
    
    /// Read file from storage
    async fn read_file(&self, path: &str) -> Result<Vec<u8>>;
    
    /// List directories at path
    async fn list_directories(&self, path: &str) -> Result<Vec<String>>;
    
    /// Delete file from storage
    async fn delete_file(&self, path: &str) -> Result<()>;
    
    /// Check if backend supports DuckDB direct access
    fn supports_duckdb_direct_access(&self) -> bool;
    
    /// Get DuckDB-compatible path for querying
    fn get_duckdb_path(&self, path: &str) -> String;
    
    /// List all snapshots for a source
    async fn list_snapshots(&self, source: &str) -> Result<Vec<SnapshotMetadata>>;
    
    /// List all snapshots across all sources
    async fn list_all_snapshots(&self) -> Result<Vec<SnapshotMetadata>>;
    
    /// List snapshots grouped by source file (returns source path -> snapshot names)
    async fn list_snapshots_for_all_sources(&self) -> Result<std::collections::HashMap<String, Vec<String>>>;
    
    /// Check if a file exists
    async fn file_exists(&self, path: &str) -> Result<bool>;
}

pub mod local;
pub mod s3;

pub use local::LocalStorage;
pub use s3::S3Storage;