//! Common test utilities and fixtures

use std::path::{Path, PathBuf};
use std::fs;
use tempfile::TempDir;

/// Test fixture paths and utilities
pub struct TestFixtures {
    pub fixtures_dir: PathBuf,
    pub data_dir: PathBuf,
    pub configs_dir: PathBuf,
}

impl TestFixtures {
    /// Get the test fixtures directory
    pub fn new() -> Self {
        let fixtures_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures");
        
        Self {
            data_dir: fixtures_dir.join("data"),
            configs_dir: fixtures_dir.join("configs"),
            fixtures_dir,
        }
    }
    
    /// Get path to a test data file
    pub fn data_file(&self, name: &str) -> PathBuf {
        self.data_dir.join(name)
    }
    
    /// Get path to a test config file
    pub fn config_file(&self, name: &str) -> PathBuf {
        self.configs_dir.join(name)
    }
}

/// Create a temporary test workspace with a config file
pub struct TestWorkspace {
    pub temp_dir: TempDir,
    pub path: PathBuf,
    pub config_path: PathBuf,
}

impl TestWorkspace {
    /// Create a new test workspace with the specified config
    pub fn new(config_name: &str) -> Self {
        let fixtures = TestFixtures::new();
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let path = temp_dir.path().to_path_buf();
        
        // Copy the config file to the temp workspace
        let source_config = fixtures.config_file(config_name);
        let config_path = path.join("snapbase.toml");
        
        // Read the original config
        let config_content = fs::read_to_string(&source_config)
            .expect("Failed to read config file");
        
        // Create storage directory within temp workspace
        let storage_dir = path.join("snapbase_storage");
        fs::create_dir_all(&storage_dir)
            .expect("Failed to create storage directory");
        
        // Update config to use the temp storage directory
        let updated_config = config_content.replace(
            "path = \"snapbase_storage\"",
            &format!("path = \"{}\"", storage_dir.to_string_lossy())
        );
        
        // Write updated config
        fs::write(&config_path, updated_config)
            .expect("Failed to write config file");
        
        Self {
            temp_dir,
            path,
            config_path,
        }
    }
    
    /// Copy a test data file to the workspace
    pub fn copy_data_file(&self, data_file_name: &str, target_name: &str) -> PathBuf {
        let fixtures = TestFixtures::new();
        let source = fixtures.data_file(data_file_name);
        let target = self.path.join(target_name);
        
        fs::copy(&source, &target)
            .expect("Failed to copy data file");
        
        target
    }
    
    /// Get the workspace path
    pub fn path(&self) -> &Path {
        &self.path
    }
    
    /// Change to the workspace directory (for tests that need current dir)
    pub fn change_to_workspace(&self) -> WorkspaceGuard {
        let original_dir = std::env::current_dir()
            .expect("Failed to get current directory");
        
        std::env::set_current_dir(&self.path)
            .expect("Failed to change to workspace directory");
        
        WorkspaceGuard { original_dir }
    }
}

/// RAII guard to restore original directory
pub struct WorkspaceGuard {
    original_dir: PathBuf,
}

impl Drop for WorkspaceGuard {
    fn drop(&mut self) {
        // Only restore if the original directory still exists
        if self.original_dir.exists() {
            let _ = std::env::set_current_dir(&self.original_dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_fixtures_exist() {
        let fixtures = TestFixtures::new();
        assert!(fixtures.fixtures_dir.exists());
        assert!(fixtures.data_dir.exists());
        assert!(fixtures.configs_dir.exists());
        
        // Check some expected files
        assert!(fixtures.data_file("simple.csv").exists());
        assert!(fixtures.config_file("local.toml").exists());
    }
    
    #[test]
    fn test_workspace_creation() {
        let workspace = TestWorkspace::new("local.toml");
        assert!(workspace.path().exists());
        assert!(workspace.config_path.exists());
        
        // Test copying data file
        let data_file = workspace.copy_data_file("simple.csv", "test.csv");
        assert!(data_file.exists());
    }
}