//! Centralized path resolution for workspace operations
//!
//! This module provides a single source of truth for all path resolution in snapbase.
//! It eliminates the confusion between workspace roots, storage paths, and current working
//! directories by providing a clean abstraction that always works with absolute paths
//! and ensures proper workspace isolation.

use crate::error::SnapbaseError;
use std::path::{Path, PathBuf};

/// PathResolver provides centralized path resolution for all workspace operations.
///
/// This struct ensures that:
/// - All paths are resolved as absolute paths
/// - Workspace operations are isolated to the workspace directory
/// - Storage operations use the correct base paths
/// - There's a single source of truth for path resolution
#[derive(Debug, Clone)]
pub struct PathResolver {
    /// The absolute path to the workspace root directory
    workspace_root: PathBuf,
    /// The absolute path to the storage base directory (usually workspace_root/.snapbase)
    storage_base: PathBuf,
    /// The current working directory at the time of creation (for reference only)
    current_working_dir: PathBuf,
}

impl PathResolver {
    /// Create a new PathResolver for the given workspace root
    ///
    /// # Arguments
    /// * `workspace_root` - The root directory of the workspace (will be made absolute)
    ///
    /// # Returns
    /// A new PathResolver with absolute paths set up correctly
    pub fn new(workspace_root: PathBuf) -> Result<Self, SnapbaseError> {
        let current_working_dir = std::env::current_dir().map_err(|e| {
            SnapbaseError::invalid_input(&format!("Cannot get current directory: {}", e))
        })?;

        // Always ensure workspace_root is absolute
        let workspace_root = if workspace_root.is_absolute() {
            workspace_root
        } else {
            current_working_dir.join(workspace_root)
        };

        // Storage base is always workspace_root/.snapbase
        let storage_base = workspace_root.join(".snapbase");

        Ok(Self {
            workspace_root,
            storage_base,
            current_working_dir,
        })
    }

    /// Get the absolute workspace root path
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    /// Get the absolute storage base path (where .snapbase lives)
    pub fn storage_base(&self) -> &Path {
        &self.storage_base
    }

    /// Resolve a path relative to the workspace root
    ///
    /// # Arguments
    /// * `relative_path` - A path relative to the workspace root
    ///
    /// # Returns
    /// Absolute path resolved from workspace root
    pub fn resolve_workspace_path<P: AsRef<Path>>(&self, relative_path: P) -> PathBuf {
        let path = relative_path.as_ref();
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.workspace_root.join(path)
        }
    }

    /// Resolve a path relative to the storage base
    ///
    /// # Arguments  
    /// * `relative_path` - A path relative to the storage base (.snapbase directory)
    ///
    /// # Returns
    /// Absolute path resolved from storage base
    pub fn resolve_storage_path<P: AsRef<Path>>(&self, relative_path: P) -> PathBuf {
        let path = relative_path.as_ref();
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.storage_base.join(path)
        }
    }

    /// Generate a Hive-style path for a snapshot
    ///
    /// # Arguments
    /// * `source_name` - The name of the source file
    /// * `snapshot_name` - The name of the snapshot
    /// * `timestamp` - The timestamp for the snapshot
    ///
    /// # Returns
    /// Relative path string for use with storage backend (e.g., "sources/file.csv/snapshot_name=v1/snapshot_timestamp=20240101T120000Z")
    pub fn get_hive_path(&self, source_name: &str, snapshot_name: &str, timestamp: &str) -> String {
        format!(
            "sources/{}/snapshot_name={}/snapshot_timestamp={}",
            source_name, snapshot_name, timestamp
        )
    }

    /// Get the absolute path to the workspace config file
    pub fn workspace_config_path(&self) -> PathBuf {
        self.workspace_root.join("snapbase.toml")
    }

    /// Get the absolute path to a file within the workspace
    ///
    /// This method ensures that files are resolved relative to the workspace,
    /// not the current working directory, providing proper workspace isolation.
    pub fn resolve_workspace_file<P: AsRef<Path>>(&self, file_path: P) -> PathBuf {
        let path = file_path.as_ref();
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.workspace_root.join(path)
        }
    }

    /// Check if a path is within the workspace boundaries
    ///
    /// This is useful for security and isolation - ensuring that operations
    /// only affect files within the intended workspace.
    pub fn is_within_workspace<P: AsRef<Path>>(&self, path: P) -> bool {
        let path = path.as_ref();
        let absolute_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.workspace_root.join(path)
        };

        absolute_path.starts_with(&self.workspace_root)
    }

    /// Convert an absolute path to a path relative to the workspace root
    ///
    /// This is useful for storing relative paths in configs or displaying
    /// paths to users in a workspace-relative context.
    pub fn make_relative_to_workspace<P: AsRef<Path>>(&self, absolute_path: P) -> Option<PathBuf> {
        let path = absolute_path.as_ref();
        path.strip_prefix(&self.workspace_root)
            .ok()
            .map(|p| p.to_path_buf())
    }

    /// Get debug information about the path resolver
    pub fn debug_info(&self) -> String {
        format!(
            "PathResolver {{\n  workspace_root: {},\n  storage_base: {},\n  current_working_dir: {}\n}}",
            self.workspace_root.display(),
            self.storage_base.display(),
            self.current_working_dir.display()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_path_resolver_creation() {
        let temp_dir = TempDir::new().unwrap();
        let workspace_root = temp_dir.path().to_path_buf();

        let resolver = PathResolver::new(workspace_root.clone()).unwrap();

        assert_eq!(resolver.workspace_root(), workspace_root);
        assert_eq!(resolver.storage_base(), workspace_root.join(".snapbase"));
    }

    #[test]
    fn test_relative_workspace_root() {
        let current_dir = std::env::current_dir().unwrap();

        // Create a relative path that exists within current directory
        let relative_path = PathBuf::from("test_workspace");

        let resolver = PathResolver::new(relative_path.clone()).unwrap();

        // Should be converted to absolute
        assert!(resolver.workspace_root().is_absolute());
        assert_eq!(
            resolver.workspace_root(),
            current_dir.join("test_workspace")
        );
    }

    #[test]
    fn test_resolve_workspace_path() {
        let temp_dir = TempDir::new().unwrap();
        let resolver = PathResolver::new(temp_dir.path().to_path_buf()).unwrap();

        let relative_path = "test/file.csv";
        let resolved = resolver.resolve_workspace_path(relative_path);

        assert_eq!(resolved, temp_dir.path().join("test/file.csv"));
        assert!(resolved.is_absolute());
    }

    #[test]
    fn test_resolve_storage_path() {
        let temp_dir = TempDir::new().unwrap();
        let resolver = PathResolver::new(temp_dir.path().to_path_buf()).unwrap();

        let relative_path = "sources/test.csv";
        let resolved = resolver.resolve_storage_path(relative_path);

        assert_eq!(resolved, temp_dir.path().join(".snapbase/sources/test.csv"));
        assert!(resolved.is_absolute());
    }

    #[test]
    fn test_get_hive_path() {
        let temp_dir = TempDir::new().unwrap();
        let resolver = PathResolver::new(temp_dir.path().to_path_buf()).unwrap();

        let hive_path = resolver.get_hive_path("test.csv", "v1", "20240101T120000Z");

        assert_eq!(
            hive_path,
            "sources/test.csv/snapshot_name=v1/snapshot_timestamp=20240101T120000Z"
        );
    }

    #[test]
    fn test_workspace_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let resolver = PathResolver::new(temp_dir.path().to_path_buf()).unwrap();

        // File within workspace
        let internal_file = "data/test.csv";
        assert!(resolver.is_within_workspace(internal_file));

        // Absolute file within workspace
        let internal_absolute = temp_dir.path().join("data/test.csv");
        assert!(resolver.is_within_workspace(&internal_absolute));

        // File outside workspace
        let external_file = "/tmp/external.csv";
        assert!(!resolver.is_within_workspace(external_file));
    }

    #[test]
    fn test_make_relative_to_workspace() {
        let temp_dir = TempDir::new().unwrap();
        let resolver = PathResolver::new(temp_dir.path().to_path_buf()).unwrap();

        let absolute_path = temp_dir.path().join("data/test.csv");
        let relative = resolver.make_relative_to_workspace(&absolute_path).unwrap();

        assert_eq!(relative, PathBuf::from("data/test.csv"));
    }

    #[test]
    fn test_workspace_config_path() {
        let temp_dir = TempDir::new().unwrap();
        let resolver = PathResolver::new(temp_dir.path().to_path_buf()).unwrap();

        let config_path = resolver.workspace_config_path();

        assert_eq!(config_path, temp_dir.path().join("snapbase.toml"));
        assert!(config_path.is_absolute());
    }
}
