//! Build script for snapbase - handles DuckDB library detection and linking

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    // Skip DuckDB library detection if bundled feature is enabled
    if cfg!(feature = "bundled") {
        // println!("cargo:warning=Using bundled DuckDB - skipping system library detection");
        return;
    }

    // Try to find DuckDB library for linking
    if let Some(lib_path) = find_duckdb_library() {
        println!("cargo:rustc-link-search=native={}", lib_path.display());
        println!("cargo:rustc-link-lib=duckdb");
        println!(
            "cargo:warning=Found DuckDB library at: {}",
            lib_path.display()
        );
    } else {
        // Print helpful error message
        eprintln!("❌ DuckDB library not found!");
        eprintln!();
        eprintln!("Please install DuckDB:");

        if cfg!(target_os = "macos") {
            eprintln!("  brew install duckdb");
        } else if cfg!(target_os = "linux") {
            eprintln!("  sudo apt install libduckdb-dev  # Ubuntu/Debian");
            eprintln!("  sudo yum install duckdb-devel   # RHEL/CentOS");
        } else if cfg!(target_os = "windows") {
            eprintln!("  Download from: https://duckdb.org/docs/installation/");
        }

        eprintln!();
        eprintln!("Or use bundled DuckDB:");
        eprintln!("  cargo build --features bundled");
        eprintln!();
        eprintln!("Or set custom path:");
        eprintln!("  export DUCKDB_LIB_PATH=/path/to/duckdb/lib");

        panic!("DuckDB library not found");
    }
}

fn find_duckdb_library() -> Option<PathBuf> {
    // 1. Check environment variable override
    if let Ok(path) = env::var("DUCKDB_LIB_PATH") {
        let path_buf = PathBuf::from(path);
        if check_duckdb_library(&path_buf) {
            return Some(path_buf);
        }
    }

    // 2. Try pkg-config first (most reliable)
    if let Some(path) = try_pkg_config() {
        return Some(path);
    }

    // 3. Check standard system paths
    get_standard_paths()
        .into_iter()
        .find(|path| check_duckdb_library(path))
}

fn try_pkg_config() -> Option<PathBuf> {
    if cfg!(target_os = "windows") {
        return None; // pkg-config not typically available on Windows
    }

    // Try to run pkg-config
    if let Ok(output) = Command::new("pkg-config")
        .args(["--libs-only-L", "duckdb"])
        .output()
    {
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            for line in stdout.lines() {
                if let Some(path_str) = line.strip_prefix("-L") {
                    let path = PathBuf::from(path_str.trim());
                    if check_duckdb_library(&path) {
                        return Some(path);
                    }
                }
            }
        }
    }

    None
}

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

fn check_duckdb_library(path: &Path) -> bool {
    if !path.exists() {
        return false;
    }

    let library_names = if cfg!(target_os = "windows") {
        vec!["duckdb.dll", "libduckdb.dll", "duckdb.lib"]
    } else if cfg!(target_os = "macos") {
        vec!["libduckdb.dylib", "libduckdb.so", "libduckdb.a"]
    } else {
        vec!["libduckdb.so", "libduckdb.so.1", "libduckdb.a"]
    };

    for lib_name in library_names {
        if path.join(lib_name).exists() {
            return true;
        }
    }

    false
}
