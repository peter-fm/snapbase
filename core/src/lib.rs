//! # snapbase-core
//!
//! Core library for snapbase - A snapshot-based structured data diff tool for detecting
//! schema, column-level, and row-level changes between versions of structured datasets.
//!
//! This crate provides the core functionality that can be used by different interfaces
//! (CLI, Python bindings, web APIs, etc.).

pub mod change_detection;
pub mod config;
pub mod data;
pub mod database;
pub mod duckdb_config;
pub mod error;
pub mod export;
pub mod hash;
pub mod naming;
pub mod path_utils;
pub mod query;
pub mod query_engine;
pub mod resolver;
pub mod snapshot;
pub mod sql;
pub mod storage;
pub mod workspace;

#[cfg(any(test, feature = "test-fixtures"))]
pub mod test_fixtures;

// Re-export the most commonly used types for convenience
pub use change_detection::{ChangeDetectionResult, StreamingChangeDetector};
pub use config::Config;
pub use error::{Result, SnapbaseError};
pub use export::{ExportFormat, ExportOptions, UnifiedExporter};
pub use resolver::SnapshotResolver;
pub use snapshot::{SnapshotCreator, SnapshotMetadata};
pub use storage::StorageBackend;
pub use workspace::SnapbaseWorkspace;

/// Current format version for snapbase files
pub const FORMAT_VERSION: &str = "1.0.0";

/// Core snapbase operations
pub struct SnapbaseCore {
    workspace: SnapbaseWorkspace,
}

impl SnapbaseCore {
    /// Create a new SnapbaseCore instance with the given workspace
    pub fn new(workspace: SnapbaseWorkspace) -> Self {
        Self { workspace }
    }

    /// Get a reference to the workspace
    pub fn workspace(&self) -> &SnapbaseWorkspace {
        &self.workspace
    }

    /// Get a mutable reference to the workspace
    pub fn workspace_mut(&mut self) -> &mut SnapbaseWorkspace {
        &mut self.workspace
    }
}
