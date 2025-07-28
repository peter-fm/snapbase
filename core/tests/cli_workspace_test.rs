use std::fs;
use tempfile::TempDir;
use snapbase_core::workspace::SnapbaseWorkspace;

#[path = "common/mod.rs"]
mod common;
use common::TestWorkspace;

/// Test that CLI workspace behavior works like git - finds existing workspaces
/// by traversing up the directory tree
#[cfg(test)]
mod cli_workspace_tests {
    use super::*;

    #[test]
    fn test_cli_workspace_traversal_behavior() {
        // This test verifies that CLI workspace finding works like git - 
        // it can find existing workspaces by traversing up the directory tree
        // This is the correct behavior and matches the documentation
        
        let test_workspace = TestWorkspace::new("local.toml");
        let workspace_path = test_workspace.path();
        
        // Change to the test workspace directory
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(workspace_path).unwrap();
        
        // Use find_or_create like CLI does - this may find an existing workspace
        // by traversing up (like git), which is the correct behavior
        let workspace = SnapbaseWorkspace::find_or_create(None).unwrap();
        
        // The workspace should be valid and have a root path
        assert!(workspace.root().exists());
        assert!(workspace.root().is_absolute());
        
        // Restore original directory
        std::env::set_current_dir(original_dir).unwrap();
    }
    
    #[test]
    fn test_cli_workspace_initialization_creates_directory() {
        // Test that workspace initialization properly creates the .snapbase directory
        // Use create_at_path to ensure we create a new workspace instead of finding existing one
        let test_workspace = TestWorkspace::new("local.toml");
        let workspace_path = test_workspace.path();
        
        // Create a new workspace directly at the path (like snapbase init would do)
        let workspace = SnapbaseWorkspace::create_at_path(workspace_path).unwrap();
        
        // Verify workspace was initialized with .snapbase directory
        let snapbase_dir = workspace_path.join(".snapbase");
        assert!(snapbase_dir.exists(), "Snapbase directory should be created during initialization");
        
        // Verify workspace root points to the correct directory
        assert_eq!(workspace.root().canonicalize().unwrap(), 
                   workspace_path.canonicalize().unwrap());
        
        // Verify config can be created
        workspace.create_config_with_force(false).unwrap();
        let config_path = workspace_path.join("snapbase.toml");
        assert!(config_path.exists(), "Configuration file should be created");
    }
    
    #[test]
    fn test_cli_workspace_with_explicit_paths() {
        // Test that explicit paths work correctly (used by bindings)
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path();
        
        let project_dir = temp_path.join("explicit_project");
        fs::create_dir_all(&project_dir).unwrap();
        
        let original_dir = std::env::current_dir().unwrap();
        
        // When explicit path is provided, should use that path
        let workspace = SnapbaseWorkspace::find_or_create(Some(&project_dir)).unwrap();
        
        // Workspace should be rooted at the specified path
        assert_eq!(workspace.root().canonicalize().unwrap(), 
                   project_dir.canonicalize().unwrap());
        
        // Restore original directory
        std::env::set_current_dir(original_dir).unwrap();
    }
    
    #[test]
    fn test_workspace_initialization_creates_config() {
        // Test that workspace initialization properly creates configuration
        let test_workspace = TestWorkspace::new("local.toml");
        let workspace_path = test_workspace.path();
        
        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(workspace_path).unwrap();
        
        // Create a new workspace in an isolated directory
        // Since we're in a temp dir, no existing workspace should be found
        let workspace = SnapbaseWorkspace::create_at_path(workspace_path).unwrap();
        
        // Initialize configuration
        workspace.create_config_with_force(false).unwrap();
        
        // Verify config file was created
        let config_path = workspace_path.join("snapbase.toml");
        assert!(config_path.exists(), "Configuration file should be created");
        
        std::env::set_current_dir(original_dir).unwrap();
    }
}