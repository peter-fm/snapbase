//! Command-line interface for snapbase

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "snapbase")]
#[command(about = "A snapshot-based structured data diff tool")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
    
    /// Override workspace location
    #[arg(long, global = true)]
    pub workspace: Option<PathBuf>,
    
    /// Enable verbose logging
    #[arg(short, long, global = true)]
    pub verbose: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize snapbase workspace
    Init {
        /// Use global config instead of local .env file
        #[arg(long)]
        from_global: bool,
    },
    
    /// Create a snapshot of structured data
    Snapshot {
        /// Input file or directory path (required unless --database is used)
        #[arg(required_unless_present = "database")]
        input: Option<String>,
        
        /// Database configuration name from snapbase.toml
        #[arg(long, required_unless_present = "input")]
        database: Option<String>,
        
        /// Specific tables to snapshot (overrides config)
        #[arg(long, requires = "database")]
        tables: Option<Vec<String>>,
        
        /// Tables to exclude from snapshot (overrides config)
        #[arg(long, requires = "database")]
        exclude_tables: Option<Vec<String>>,
        
        /// Name for the snapshot (optional - uses configured default pattern if not provided)
        #[arg(long)]
        name: Option<String>,
        
    },
    
    
    /// Show snapshot information
    Show {
        /// Source file path
        source: String,
        
        /// Snapshot name to display
        snapshot: String,
        
        /// Show detailed information from archive
        #[arg(long)]
        detailed: bool,
        
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    
    /// Check current data against a snapshot
    Status {
        /// Input file or directory path
        input: String,
        
        /// Snapshot to compare against (defaults to latest)
        #[arg(long)]
        compare_to: Option<String>,
        
        /// Quiet output (machine-readable)
        #[arg(long)]
        quiet: bool,
        
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    
    /// List all snapshots
    List {
        /// Filter snapshots for a specific source file
        source: Option<String>,
        
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    
    /// Show workspace statistics
    Stats {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    
    /// Compare two snapshots
    Diff {
        /// Source file path
        source: String,
        
        /// First snapshot to compare
        from: String,
        
        /// Second snapshot to compare
        to: String,
        
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    
    /// Export snapshot data to a file
    Export {
        /// Input file to export snapshot from
        input: String,
        
        /// Output file path (format determined by extension: .csv or .parquet)
        #[arg(long)]
        file: String,
        
        /// Snapshot to export
        #[arg(long, conflicts_with = "to_date")]
        to: Option<String>,
        
        /// Date/time to export (e.g., '2025-01-01' or '2025-01-01 15:00:00')
        /// Finds the latest snapshot before this time
        #[arg(long, conflicts_with = "to")]
        to_date: Option<String>,
        
        /// Show what would be exported without creating file (dry run)
        #[arg(long)]
        dry_run: bool,
        
        /// Skip confirmation prompts
        #[arg(long)]
        force: bool,
    },
    
    
    /// Clean up old snapshot archives to save space
    Cleanup {
        /// Number of recent snapshots to keep full data for rollback capability (default: 5)
        /// Recent snapshots with full data can be rolled back, older ones become delta-only
        #[arg(long, default_value = "5")]
        keep_full: usize,
        
        /// Show what would be cleaned without applying (dry run)
        #[arg(long)]
        dry_run: bool,
        
        /// Skip confirmation prompts
        #[arg(long)]
        force: bool,
    },
    
    /// Query historical snapshots using SQL
    Query {
        /// Source file to query
        source: String,
        
        /// SQL query to execute
        #[arg(required_unless_present = "list_snapshots")]
        query: Option<String>,
        
        /// Output format (table, json, csv)
        #[arg(long, default_value = "table")]
        format: String,
        
        /// Limit number of results
        #[arg(long)]
        limit: Option<usize>,
        
        /// Show available snapshots for this source
        #[arg(long)]
        list_snapshots: bool,
    },
    
    /// Configure snapbase settings
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
}

#[derive(Subcommand)]
pub enum ConfigCommand {
    /// Configure storage backend
    Storage {
        /// Storage backend type (local or s3)
        #[arg(long, value_enum)]
        backend: StorageBackend,
        
        /// S3 bucket name (required for s3 backend)
        #[arg(long)]
        s3_bucket: Option<String>,
        
        /// S3 prefix (optional)
        #[arg(long)]
        s3_prefix: Option<String>,
        
        /// S3 region (optional, defaults to us-east-1)
        #[arg(long)]
        s3_region: Option<String>,
        
        /// Local storage path (optional, defaults to .snapbase)
        #[arg(long)]
        local_path: Option<String>,
        
        /// Save to global config instead of workspace config
        #[arg(long)]
        global: bool,
    },
    
    /// Show current configuration
    Show,
    
    /// Set default snapshot naming pattern
    DefaultName {
        /// Pattern for default snapshot names (e.g., "{source}_{format}_{seq}")
        /// Available variables: {source}, {format}, {seq}, {timestamp}, {date}, {time}, {hash}, {user}
        pattern: String,
    },
}

#[derive(clap::ValueEnum, Clone)]
pub enum StorageBackend {
    Local,
    S3,
}



#[cfg(test)]
mod tests {
    // Tests would go here
}
