//! DuckDB configuration and library discovery

use crate::error::{Result, SnapbaseError};
use std::env;
use std::path::{Path, PathBuf};

/// DuckDB configuration manager
pub struct DuckDbConfig {
    pub library_path: Option<PathBuf>,
    pub prefer_bundled: bool,
}

impl DuckDbConfig {
    /// Create a new DuckDB configuration with automatic discovery
    pub fn new() -> Self {
        let library_path = Self::discover_library_path();
        let prefer_bundled = env::var("DUCKDB_DISABLE_BUNDLED").is_err();
        
        Self {
            library_path,
            prefer_bundled,
        }
    }

    /// Discover DuckDB library path using various methods
    fn discover_library_path() -> Option<PathBuf> {
        // 1. Check environment variable override
        if let Ok(path) = env::var("DUCKDB_LIB_PATH") {
            let path_buf = PathBuf::from(path);
            if path_buf.exists() {
                return Some(path_buf);
            }
        }

        // 2. Check standard system paths
        for path in Self::get_standard_paths() {
            if Self::check_duckdb_library(&path) {
                return Some(path);
            }
        }

        // 3. Try pkg-config (Linux/macOS)
        if let Some(path) = Self::try_pkg_config() {
            return Some(path);
        }

        None
    }

    /// Get standard installation paths for each platform
    fn get_standard_paths() -> Vec<PathBuf> {
        let mut paths = Vec::new();

        if cfg!(target_os = "macos") {
            // macOS standard paths
            paths.extend([
                PathBuf::from("/opt/homebrew/lib"),
                PathBuf::from("/usr/local/lib"),
                PathBuf::from("/opt/local/lib"), // MacPorts
            ]);
        } else if cfg!(target_os = "linux") {
            // Linux standard paths
            paths.extend([
                PathBuf::from("/usr/lib"),
                PathBuf::from("/usr/local/lib"),
                PathBuf::from("/lib"),
                PathBuf::from("/usr/lib/x86_64-linux-gnu"),
                PathBuf::from("/usr/lib64"),
            ]);
        } else if cfg!(target_os = "windows") {
            // Windows standard paths
            paths.extend([
                PathBuf::from("C:\\Program Files\\DuckDB\\lib"),
                PathBuf::from("C:\\duckdb\\lib"),
                PathBuf::from("C:\\tools\\duckdb\\lib"),
            ]);
        }

        paths
    }

    /// Check if DuckDB library exists in the given path
    fn check_duckdb_library(path: &Path) -> bool {
        if !path.exists() {
            return false;
        }

        let library_names = if cfg!(target_os = "windows") {
            vec!["duckdb.dll", "libduckdb.dll"]
        } else if cfg!(target_os = "macos") {
            vec!["libduckdb.dylib", "libduckdb.so"]
        } else {
            vec!["libduckdb.so", "libduckdb.so.1"]
        };

        for lib_name in library_names {
            if path.join(lib_name).exists() {
                return true;
            }
        }

        false
    }

    /// Try to find DuckDB using pkg-config
    fn try_pkg_config() -> Option<PathBuf> {
        if cfg!(target_os = "windows") {
            return None; // pkg-config not typically available on Windows
        }

        // Try to run pkg-config
        if let Ok(output) = std::process::Command::new("pkg-config")
            .args(["--libs-only-L", "duckdb"])
            .output()
        {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    if let Some(path_str) = line.strip_prefix("-L") {
                        let path = PathBuf::from(path_str.trim());
                        if Self::check_duckdb_library(&path) {
                            return Some(path);
                        }
                    }
                }
            }
        }

        None
    }

    /// Validate the current configuration
    pub fn validate(&self) -> Result<()> {
        // With bundled feature, we don't need to validate system libraries
        if cfg!(feature = "bundled") && self.prefer_bundled {
            return Ok(());
        }

        // Check if we found a system library
        if let Some(ref path) = self.library_path {
            if Self::check_duckdb_library(path) {
                return Ok(());
            }
        }

        // If we get here, no valid DuckDB installation was found
        Err(SnapbaseError::config(self.create_helpful_error_message()))
    }

    /// Create a helpful error message for missing DuckDB
    fn create_helpful_error_message(&self) -> String {
        let mut message = String::from("❌ DuckDB library not found!\n\n");
        
        message.push_str("Possible solutions:\n");
        
        if cfg!(target_os = "macos") {
            message.push_str("1. Install DuckDB: brew install duckdb\n");
        } else if cfg!(target_os = "linux") {
            message.push_str("1. Install DuckDB: sudo apt install libduckdb-dev (Ubuntu/Debian)\n");
            message.push_str("   or: sudo yum install duckdb-devel (RHEL/CentOS)\n");
        } else if cfg!(target_os = "windows") {
            message.push_str("1. Download DuckDB from: https://duckdb.org/docs/installation/\n");
            message.push_str("   Extract to C:\\Program Files\\DuckDB\\\n");
        }
        
        message.push_str("2. Set custom path: export DUCKDB_LIB_PATH=/path/to/duckdb/lib\n");
        
        if cfg!(feature = "bundled") {
            message.push_str("3. Use bundled version: This should work automatically!\n");
            message.push_str("   If you see this error, please report it as a bug.\n");
        } else {
            message.push_str("3. Rebuild with bundled DuckDB: cargo build --features bundled\n");
        }
        
        message.push_str("\nSearched paths:\n");
        for path in Self::get_standard_paths() {
            let status = if Self::check_duckdb_library(&path) {
                "✅ Found"
            } else if path.exists() {
                "❌ No DuckDB library"
            } else {
                "❌ Path not found"
            };
            message.push_str(&format!("  {} {}\n", status, path.display()));
        }
        
        if let Some(ref custom_path) = self.library_path {
            message.push_str(&format!("\nCustom path: {}\n", custom_path.display()));
        }
        
        message.push_str("\nFor more help: https://github.com/your-repo/snapbase#installation");
        
        message
    }

    /// Get the library path for linking (if needed)
    pub fn get_library_path(&self) -> Option<&Path> {
        self.library_path.as_deref()
    }

    /// Check if bundled DuckDB should be used
    pub fn use_bundled(&self) -> bool {
        cfg!(feature = "bundled") && self.prefer_bundled
    }
}

impl Default for DuckDbConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Initialize DuckDB configuration and validate it
pub fn init_duckdb() -> Result<DuckDbConfig> {
    let config = DuckDbConfig::new();
    config.validate()?;
    
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let _config = DuckDbConfig::new();
        // Should not panic
        assert!(true);
    }

    #[test]
    fn test_standard_paths_not_empty() {
        let paths = DuckDbConfig::get_standard_paths();
        assert!(!paths.is_empty(), "Should have at least one standard path for this platform");
    }

    #[test]
    fn test_bundled_feature_detection() {
        let config = DuckDbConfig::new();
        // This will be true if compiled with bundled feature
        let uses_bundled = config.use_bundled();
        println!("Uses bundled DuckDB: {uses_bundled}");
    }

    #[test]
    fn test_error_message_generation() {
        let config = DuckDbConfig {
            library_path: None,
            prefer_bundled: false,
        };
        let message = config.create_helpful_error_message();
        assert!(message.contains("DuckDB library not found"));
        assert!(message.contains("Possible solutions"));
    }
}
