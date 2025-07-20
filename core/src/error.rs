//! Error types for snapbase operations

use std::path::PathBuf;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, SnapbaseError>;

#[derive(Error, Debug)]
pub enum SnapbaseError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("TOML serialization error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    #[error("TOML deserialization error: {0}")]
    TomlDe(#[from] toml::de::Error),

    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Archive error: {0}")]
    Archive(String),

    #[error("Workspace error: {0}")]
    Workspace(String),

    #[error("Snapshot not found: {name}")]
    SnapshotNotFound { name: String },

    #[error("Invalid snapshot format: {path}")]
    InvalidSnapshot { path: PathBuf },

    #[error("Schema mismatch: {message}")]
    SchemaMismatch { message: String },

    #[error("Invalid sampling strategy: {strategy}")]
    InvalidSampling { strategy: String },

    #[error("Configuration error: {message}")]
    Config { message: String },

    #[error("Data processing error: {message}")]
    DataProcessing { message: String },

    #[error("Hash computation error: {message}")]
    Hash { message: String },

    #[error("Git operation error: {message}")]
    Git { message: String },

    #[error("Invalid input: {message}")]
    InvalidInput { message: String },

    #[error("Operation cancelled by user")]
    Cancelled,

    #[error("Walkdir error: {0}")]
    WalkDir(#[from] walkdir::Error),

    #[error("String conversion error: {0}")]
    StringConversion(#[from] std::string::FromUtf8Error),

    #[error("Generic error: {0}")]
    Generic(#[from] anyhow::Error),
}

impl SnapbaseError {
    pub fn workspace(msg: impl Into<String>) -> Self {
        Self::Workspace(msg.into())
    }

    pub fn archive(msg: impl Into<String>) -> Self {
        Self::Archive(msg.into())
    }

    pub fn schema_mismatch(msg: impl Into<String>) -> Self {
        Self::SchemaMismatch {
            message: msg.into(),
        }
    }

    pub fn invalid_sampling(strategy: impl Into<String>) -> Self {
        Self::InvalidSampling {
            strategy: strategy.into(),
        }
    }

    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config {
            message: msg.into(),
        }
    }

    pub fn data_processing(msg: impl Into<String>) -> Self {
        Self::DataProcessing {
            message: msg.into(),
        }
    }

    pub fn hash(msg: impl Into<String>) -> Self {
        Self::Hash {
            message: msg.into(),
        }
    }

    pub fn git(msg: impl Into<String>) -> Self {
        Self::Git {
            message: msg.into(),
        }
    }

    pub fn invalid_input(msg: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: msg.into(),
        }
    }
}
