//! Snapshot creation and management

use crate::data::{DataInfo, DataProcessor};
use crate::error::{Result, SnapbaseError};
use crate::hash::{ColumnHash, ColumnInfo, HashComputer, RowHash, SchemaHash};
use indicatif::ProgressBar;
use crate::change_detection::ChangeDetectionResult;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Snapshot metadata stored in JSON format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub format_version: String,
    pub name: String,
    pub created: DateTime<Utc>,
    pub source: String,
    pub source_hash: String,
    pub row_count: u64,
    pub column_count: usize,
    pub columns: Vec<ColumnInfo>,
    pub archive_size: Option<u64>,
    // Enhanced snapshot chain fields (with defaults for backward compatibility)
    #[serde(default)]
    pub parent_snapshot: Option<String>,
    #[serde(default)]
    pub sequence_number: u64,
    #[serde(default)]
    pub delta_from_parent: Option<DeltaInfo>,
    #[serde(default)]
    pub can_reconstruct_parent: bool,
    // Source-aware fields for proper chain isolation
    #[serde(default)]
    pub source_path: Option<String>,
    #[serde(default)]
    pub source_fingerprint: Option<String>,
}

impl SnapshotMetadata {
    /// Compute schema hash on-demand from columns
    pub fn compute_schema_hash(&self) -> Result<String> {
        use crate::hash::HashComputer;
        let hash_computer = HashComputer::new();
        let schema_hash = hash_computer.hash_schema(&self.columns)?;
        Ok(schema_hash.hash)
    }

    /// Compute column hashes on-demand from columns
    pub fn compute_column_hashes(&self) -> Result<Vec<String>> {
        use crate::data::DataProcessor;
        let column_hashes = DataProcessor::compute_column_metadata_hashes(&self.columns)?;
        Ok(column_hashes.into_iter().map(|h| h.hash).collect())
    }
}

/// Information about delta changes from parent snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaInfo {
    pub parent_name: String,
    pub changes: ChangeDetectionResult,
    pub compressed_size: u64,
}


/// Snapshot creator
pub struct SnapshotCreator {
    hash_computer: HashComputer,
    progress: Option<ProgressBar>,
}

impl SnapshotCreator {
    pub fn new(show_progress: bool) -> Self {
        let hash_computer = HashComputer::new();
        let progress = if show_progress { Some(ProgressBar::new(0)) } else { None };

        Self {
            hash_computer,
            progress,
        }
    }

    /// Create a snapshot from input file with enhanced chain management
    pub fn create_snapshot(
        &mut self,
        input_path: &Path,
        name: &str,
        _archive_path: &Path,
        json_path: &Path,
        full_data: bool,
    ) -> Result<SnapshotMetadata> {
        self.create_snapshot_with_workspace(input_path, name, _archive_path, json_path, full_data, None)
    }

    /// Create a snapshot with workspace context for chain management
    pub fn create_snapshot_with_workspace(
        &mut self,
        input_path: &Path,
        name: &str,
        _archive_path: &Path,
        json_path: &Path,
        full_data: bool,
        workspace: Option<&crate::workspace::SnapbaseWorkspace>,
    ) -> Result<SnapshotMetadata> {
        // Load data - use workspace-configured processor if available
        let mut data_processor = if let Some(ws) = workspace {
            DataProcessor::new_with_workspace(ws)?
        } else {
            DataProcessor::new()?
        };
        
        // Only check format for files, not directories (which can contain supported files)
        if input_path.is_file() && !DataProcessor::is_supported_format(input_path) {
            return Err(SnapbaseError::invalid_input(format!(
                "Unsupported file format: {}",
                input_path.display()
            )));
        }

        // Phase 1: Load and analyze data
        if let Some(pb) = &self.progress {
            pb.set_message("📊 Loading and analyzing data...");
        }
        let data_info = data_processor.load_file(input_path)?;
        
        // Update progress with actual row count
        if let Some(pb) = &self.progress {
            pb.set_length(data_info.row_count);
        }

        // Phase 2: Compute schema hash
        let schema_hash = self.hash_computer.hash_schema(&data_info.columns)?;

        // Phase 3: Compute row hashes with progress reporting
        let row_hashes = self.hash_computer.hash_rows_with_processor_and_progress(
            &mut data_processor,
            None // No callback needed - data.rs handles progress display directly
        )?;
        if let Some(pb) = &self.progress {
            let message = format!("✅ Hashed {} rows", row_hashes.len());
            pb.set_message(message);
        }

        // Find parent snapshot and compute delta if workspace is provided (using computed hashes)
        let (parent_snapshot, sequence_number, delta_from_parent) = if let Some(ws) = workspace {
            self.find_parent_and_compute_delta(ws, &data_info, &row_hashes)?
        } else {
            (None, 0, None)
        };

        // Phase 4: Compute column hashes
        let column_hashes = self.hash_computer.hash_columns_with_processor(&mut data_processor)?;
        if let Some(pb) = &self.progress {
            let message = format!("✅ Hashed {} columns", column_hashes.len());
            pb.set_message(message);
        }

        // Phase 5: Create Hive snapshot structure (replaces archive creation)
        if let Some(pb) = &self.progress {
            pb.set_message("📦 Creating snapshot...");
        }
        
        // Determine workspace base path
        let workspace_base = if let Some(ws) = workspace {
            ws.snapbase_dir.clone()
        } else {
            // Default to current directory's .snapbase if no workspace context
            std::env::current_dir()?.join(".snapbase")
        };
        
        self.create_hive_snapshot(
            &data_info,
            &schema_hash,
            &row_hashes,
            &column_hashes,
            name,
            full_data,
            &delta_from_parent,
            &mut data_processor,
            &workspace_base,
        )?;
        
        // Archive creation removed - using Hive-style storage only
        let archive_size = 0; // No archive file created

        // Create canonical source path and fingerprint for source tracking
        let canonical_source_path = input_path.canonicalize()
            .unwrap_or_else(|_| input_path.to_path_buf())
            .to_string_lossy()
            .to_string();
        
        let source_fingerprint = format!("{}:{}", 
            canonical_source_path,
            self.hash_computer.hash_value(&format!("{}:{}", 
                canonical_source_path, 
                data_info.row_count
            ))
        );

        // Create metadata
        let mut metadata = SnapshotMetadata {
            format_version: crate::FORMAT_VERSION.to_string(),
            name: name.to_string(),
            created: Utc::now(),
            source: input_path.to_string_lossy().to_string(),
            source_hash: self.hash_computer.hash_value(&std::fs::read_to_string(input_path).unwrap_or_default()),
            row_count: data_info.row_count,
            column_count: data_info.column_count(),
            columns: data_info.columns.clone(),
            archive_size: Some(archive_size),
            parent_snapshot,
            sequence_number,
            delta_from_parent,
            can_reconstruct_parent: false,
            source_path: Some(canonical_source_path),
            source_fingerprint: Some(source_fingerprint),
        };

        // Set can_reconstruct_parent flag if this snapshot has a delta
        self.update_current_reconstruct_flag(&mut metadata);

        // Save JSON metadata
        let json_content = serde_json::to_string_pretty(&metadata)?;
        std::fs::write(json_path, json_content)?;

        // Update parent's can_reconstruct_parent flag if we have a delta
        if let (Some(ws), Some(parent_name)) = (workspace, &metadata.parent_snapshot) {
            if metadata.delta_from_parent.is_some() {
                self.update_parent_reconstruct_flag(ws, parent_name)?;
            }
        }

        if let Some(pb) = &self.progress {
            pb.finish_with_message("🎉 Snapshot created successfully");
        }

        Ok(metadata)
    }



    // NOTE: Removed create_delta_parquet() - delta files are redundant
    // All operations use full data.parquet files, not deltas

    /// Create snapshot in Hive directory structure (replaces archive creation)
    fn create_hive_snapshot(
        &mut self,
        data_info: &DataInfo,
        _schema_hash: &SchemaHash,
        _row_hashes: &[RowHash],
        _column_hashes: &[ColumnHash],
        name: &str,
        full_data: bool,
        _delta_from_parent: &Option<DeltaInfo>,
        data_processor: &mut DataProcessor,
        workspace_base: &Path,
    ) -> Result<()> {
        // Generate timestamp for Hive partitioning
        let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        
        // Extract source name from path
        let source_name = data_info.source.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        
        // Create Hive directory structure using OS-native paths for local operations
        // Note: This is for local filesystem operations, not storage backend operations
        let hive_dir = workspace_base
            .join("sources")
            .join(source_name)
            .join(format!("snapshot_name={name}"))
            .join(format!("snapshot_timestamp={timestamp}"));
        
        std::fs::create_dir_all(&hive_dir)?;
        
        // Create metadata.json
        let source_content = std::fs::read_to_string(&data_info.source)?;
        let source_hash = {
            use blake3::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(source_content.as_bytes());
            hasher.finalize().to_hex().to_string()
        };
        let source_fingerprint = {
            use blake3::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(data_info.source.to_string_lossy().as_bytes());
            hasher.update(b":");
            hasher.update(source_content.as_bytes());
            format!("{}:{}", data_info.source.to_string_lossy(), hasher.finalize().to_hex())
        };
        
        let metadata = serde_json::json!({
            "format_version": "2.0",
            "name": name,
            "created": chrono::Utc::now(),
            "source": data_info.source.to_string_lossy(),
            "source_hash": source_hash,
            "row_count": data_info.row_count,
            "column_count": data_info.column_count(),
            "columns": data_info.columns,
            "parent_snapshot": _delta_from_parent.as_ref().map(|d| d.parent_name.clone()),
            "sequence_number": 1, // Will be calculated properly
            "can_reconstruct_parent": false, // No deltas
            "source_path": data_info.source.to_string_lossy(),
            "source_fingerprint": source_fingerprint,
        });
        
        let metadata_path = hive_dir.join("metadata.json");
        std::fs::write(&metadata_path, serde_json::to_string_pretty(&metadata)?)?;
        
        // Note: schema.json is no longer created - schema information is stored in metadata.json
        // and hashes are computed on-demand from the columns array
        
        // Create data.parquet using DuckDB COPY (only if full_data is true)
        if full_data {
            let parquet_path = hive_dir.join("data.parquet");
            
            // Load baseline data for change detection if parent snapshot exists
            // For first snapshot, baseline_data will be None, resulting in all rows marked as "added"
            let baseline_data = if let Some(parent_info) = _delta_from_parent {
                self.load_baseline_data_for_comparison(workspace_base, &parent_info.parent_name)?
            } else {
                None
            };
            
            // Use DuckDB's COPY command to export directly to Parquet with change flags
            data_processor.export_to_parquet_with_flags(&parquet_path, baseline_data.as_ref())?;
        }
        
        Ok(())
    }

    /// Load baseline data from parent snapshot for change detection
    fn load_baseline_data_for_comparison(
        &self,
        workspace_base: &Path,
        parent_name: &str,
    ) -> Result<Option<crate::data::BaselineData>> {
        // Find the parent snapshot in the Hive structure
        let sources_dir = workspace_base.join("sources");
        
        // Search through all sources to find the parent snapshot
        for source_entry in std::fs::read_dir(&sources_dir)? {
            let source_entry = source_entry?;
            if !source_entry.file_type()?.is_dir() {
                continue;
            }
            
            let source_path = source_entry.path();
            let snapshots_dir = source_path.join(format!("snapshot_name={parent_name}"));
            
            if snapshots_dir.exists() {
                // Find the latest timestamp for this snapshot name
                let mut latest_timestamp = None;
                let mut latest_path = None;
                
                for timestamp_entry in std::fs::read_dir(&snapshots_dir)? {
                    let timestamp_entry = timestamp_entry?;
                    if !timestamp_entry.file_type()?.is_dir() {
                        continue;
                    }
                    
                    let timestamp_path = timestamp_entry.path();
                    if let Some(dir_name) = timestamp_path.file_name().and_then(|n| n.to_str()) {
                        if let Some(stripped) = dir_name.strip_prefix("snapshot_timestamp=") {
                            let timestamp = stripped.to_string(); // Remove "snapshot_timestamp=" prefix
                            if latest_timestamp.is_none() || Some(&timestamp) > latest_timestamp.as_ref() {
                                latest_timestamp = Some(timestamp);
                                latest_path = Some(timestamp_path.clone());
                            }
                        }
                    }
                }
                
                if let Some(snapshot_path) = latest_path {
                    let metadata_path = snapshot_path.join("metadata.json");
                    let parquet_path = snapshot_path.join("data.parquet");
                    
                    // Load metadata to get schema information
                    if metadata_path.exists() {
                        let metadata_content = std::fs::read_to_string(&metadata_path)?;
                        let metadata: serde_json::Value = serde_json::from_str(&metadata_content)?;
                        
                        // Extract column information
                        let columns = metadata["columns"].as_array()
                            .ok_or_else(|| crate::error::SnapbaseError::invalid_input("Missing columns in metadata"))?;
                        
                        let mut schema = Vec::new();
                        for col in columns {
                            schema.push(crate::hash::ColumnInfo {
                                name: col["name"].as_str().unwrap_or("unknown").to_string(),
                                data_type: col["data_type"].as_str().unwrap_or("TEXT").to_string(),
                                nullable: col["nullable"].as_bool().unwrap_or(true),
                            });
                        }
                        
                        // Load data from parquet if it exists
                        let data = if parquet_path.exists() {
                            self.load_parquet_data(&parquet_path, &schema)?
                        } else {
                            Vec::new()
                        };
                        
                        return Ok(Some(crate::data::BaselineData { schema, data }));
                    }
                }
            }
        }
        
        Ok(None)
    }

    /// Load data from parquet file using DuckDB
    fn load_parquet_data(
        &self,
        parquet_path: &Path,
        schema: &[crate::hash::ColumnInfo],
    ) -> Result<Vec<Vec<String>>> {
        let data_processor = crate::data::DataProcessor::new()?;
        
        // Load the parquet file using DuckDB
        let column_names: Vec<String> = schema.iter().map(|c| c.name.clone()).collect();
        let load_sql = format!(
            "SELECT {} FROM read_parquet('{}')",
            column_names.join(", "),
            parquet_path.to_string_lossy()
        );
        
        let mut stmt = data_processor.connection.prepare(&load_sql)?;
        let rows = stmt.query_map([], |row| {
            let mut string_row = Vec::new();
            for i in 0..schema.len() {
                let value: String = match row.get_ref(i)? {
                    duckdb::types::ValueRef::Null => String::new(),
                    duckdb::types::ValueRef::Boolean(b) => if b { "true".to_string() } else { "false".to_string() },
                    duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Int(i) => i.to_string(),
                    duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Float(f) => f.to_string(),
                    duckdb::types::ValueRef::Double(f) => f.to_string(),
                    duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                    duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                    duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                    duckdb::types::ValueRef::Date32(d) => format!("{d:?}"),
                    duckdb::types::ValueRef::Time64(t, _) => format!("{t:?}"),
                    duckdb::types::ValueRef::Timestamp(ts, _) => format!("{ts:?}"),
                    _ => "<unknown>".to_string(),
                };
                string_row.push(value);
            }
            Ok(string_row)
        })?;
        
        let mut data = Vec::new();
        for row_result in rows {
            data.push(row_result?);
        }
        
        Ok(data)
    }


    /// Find parent snapshot and compute delta using cached hashes (FIXED ARCHITECTURE)
    fn find_parent_and_compute_delta(
        &self,
        workspace: &crate::workspace::SnapbaseWorkspace,
        current_data_info: &DataInfo,
        _current_row_hashes: &[crate::hash::RowHash],
    ) -> Result<(Option<String>, u64, Option<DeltaInfo>)> {
        // Create canonical source path for current file
        let current_canonical_path = current_data_info.source.canonicalize()
            .unwrap_or_else(|_| current_data_info.source.clone())
            .to_string_lossy()
            .to_string();

        // Build source-aware snapshot chain for the current file only
        let chain = SnapshotChain::build_chain_for_source(workspace, &current_canonical_path)?;
        
        if let Some(head_name) = &chain.head {
            // Load parent snapshot metadata
            let (_parent_archive_path, parent_json_path) = workspace.snapshot_paths(head_name);
            
            if parent_json_path.exists() {
                let parent_metadata = SnapshotLoader::load_metadata(&parent_json_path)?;
                
                // Double-check that parent is from the same source
                if let Some(parent_source_path) = &parent_metadata.source_path {
                    if parent_source_path != &current_canonical_path {
                        // Parent is from different source, treat as first snapshot
                        return Ok((None, 0, None));
                    }
                } else {
                    // Legacy snapshot without source_path, check original source field
                    let parent_canonical_path = std::path::Path::new(&parent_metadata.source)
                        .canonicalize()
                        .unwrap_or_else(|_| std::path::PathBuf::from(&parent_metadata.source))
                        .to_string_lossy()
                        .to_string();
                    
                    if parent_canonical_path != current_canonical_path {
                        // Parent is from different source, treat as first snapshot
                        return Ok((None, 0, None));
                    }
                }
                
                // Archive system removed - no delta computation from old snapshots
                // Parent exists but we don't compute deltas from archive files anymore
                let sequence_number = parent_metadata.sequence_number + 1;
                return Ok((Some(head_name.clone()), sequence_number, None));
            }
        }
        
        // No parent found - this is the first snapshot for this source
        Ok((None, 0, None))
    }


    /// Update parent's can_reconstruct_parent flag
    fn update_parent_reconstruct_flag(
        &self,
        _workspace: &crate::workspace::SnapbaseWorkspace,
        _parent_name: &str,
    ) -> Result<()> {
        // This method name is misleading - we're actually updating the CURRENT snapshot
        // to indicate it can reconstruct its parent, not updating the parent itself
        // The parent doesn't need to know it can be reconstructed - the child does
        Ok(())
    }

    /// Update current snapshot to indicate it can reconstruct its parent
    fn update_current_reconstruct_flag(
        &self,
        current_metadata: &mut SnapshotMetadata,
    ) {
        // If this snapshot has a delta from parent, it can reconstruct the parent
        if current_metadata.delta_from_parent.is_some() {
            current_metadata.can_reconstruct_parent = true;
        }
    }






}

/// Snapshot loader for reading existing snapshots
pub struct SnapshotLoader;

impl SnapshotLoader {
    /// Load snapshot metadata from JSON file
    pub fn load_metadata<P: AsRef<Path>>(json_path: P) -> Result<SnapshotMetadata> {
        let content = std::fs::read_to_string(json_path)?;
        let metadata: SnapshotMetadata = serde_json::from_str(&content)?;
        Ok(metadata)
    }


    /// Check if snapshot has full archive data
    pub fn has_archive<P: AsRef<Path>>(archive_path: P) -> bool {
        archive_path.as_ref().exists()
    }
}

/// Full snapshot data loaded from archive
#[derive(Debug)]
pub struct FullSnapshotData {
    pub metadata: SnapshotMetadata,
    pub schema_data: serde_json::Value,
    pub row_data: serde_json::Value,
    pub delta_data: Option<serde_json::Value>,
}

/// Snapshot chain management for tracking relationships between snapshots
#[derive(Debug, Clone)]
pub struct SnapshotChain {
    pub snapshots: Vec<SnapshotMetadata>,
    pub head: Option<String>,
}

impl SnapshotChain {
    /// Build snapshot chain from workspace
    pub fn build_chain(workspace: &crate::workspace::SnapbaseWorkspace) -> Result<Self> {
        let snapshot_names = workspace.list_snapshots()?;
        let mut snapshots = Vec::new();
        
        for name in snapshot_names {
            let (_, json_path) = workspace.snapshot_paths(&name);
            if json_path.exists() {
                let metadata = SnapshotLoader::load_metadata(&json_path)?;
                snapshots.push(metadata);
            }
        }
        
        // Sort by sequence number and creation time
        snapshots.sort_by(|a, b| {
            a.sequence_number.cmp(&b.sequence_number)
                .then_with(|| a.created.cmp(&b.created))
        });
        
        // Find head (latest snapshot)
        let head = snapshots.last().map(|s| s.name.clone());
        
        Ok(Self { snapshots, head })
    }

    /// Build snapshot chain for a specific source file
    pub fn build_chain_for_source(workspace: &crate::workspace::SnapbaseWorkspace, source_path: &str) -> Result<Self> {
        let snapshot_names = workspace.list_snapshots()?;
        let mut snapshots = Vec::new();
        
        for name in snapshot_names {
            let (_, json_path) = workspace.snapshot_paths(&name);
            if json_path.exists() {
                let metadata = SnapshotLoader::load_metadata(&json_path)?;
                
                // Check if this snapshot is from the same source
                let is_same_source = if let Some(snapshot_source_path) = &metadata.source_path {
                    // Use the stored canonical source path
                    snapshot_source_path == source_path
                } else {
                    // Legacy snapshot without source_path, check original source field
                    let snapshot_canonical_path = std::path::Path::new(&metadata.source)
                        .canonicalize()
                        .unwrap_or_else(|_| std::path::PathBuf::from(&metadata.source))
                        .to_string_lossy()
                        .to_string();
                    
                    snapshot_canonical_path == source_path
                };
                
                if is_same_source {
                    snapshots.push(metadata);
                }
            }
        }
        
        // Sort by sequence number and creation time
        snapshots.sort_by(|a, b| {
            a.sequence_number.cmp(&b.sequence_number)
                .then_with(|| a.created.cmp(&b.created))
        });
        
        // Find head (latest snapshot for this source)
        let head = snapshots.last().map(|s| s.name.clone());
        
        Ok(Self { snapshots, head })
    }
    
    /// Find path from one snapshot to another
    pub fn find_path_to_snapshot(&self, target: &str) -> Option<Vec<String>> {
        // Find target snapshot
        let target_snapshot = self.snapshots.iter().find(|s| s.name == target)?;
        
        // Build path by following parent chain backwards
        let mut path = vec![target.to_string()];
        let mut current = target_snapshot;
        
        while let Some(parent_name) = &current.parent_snapshot {
            path.push(parent_name.clone());
            current = self.snapshots.iter().find(|s| s.name == *parent_name)?;
        }
        
        path.reverse();
        Some(path)
    }
    
    /// Check if a snapshot can be safely deleted (has child that can reconstruct it)
    pub fn can_safely_delete(&self, snapshot: &str) -> bool {
        // Find children of this snapshot
        for child in &self.snapshots {
            if let Some(parent) = &child.parent_snapshot {
                if parent == snapshot && child.can_reconstruct_parent {
                    return true;
                }
            }
        }
        false
    }
    
    /// Get children of a snapshot
    pub fn get_children(&self, snapshot: &str) -> Vec<&SnapshotMetadata> {
        self.snapshots
            .iter()
            .filter(|s| s.parent_snapshot.as_ref() == Some(&snapshot.to_string()))
            .collect()
    }
    
    /// Get parent of a snapshot
    pub fn get_parent(&self, snapshot: &str) -> Option<&SnapshotMetadata> {
        let snapshot_meta = self.snapshots.iter().find(|s| s.name == snapshot)?;
        let parent_name = snapshot_meta.parent_snapshot.as_ref()?;
        self.snapshots.iter().find(|s| s.name == *parent_name)
    }
    
    /// Validate chain integrity
    pub fn validate(&self) -> Result<Vec<String>> {
        let mut issues = Vec::new();
        
        for snapshot in &self.snapshots {
            // Check parent exists if specified
            if let Some(parent_name) = &snapshot.parent_snapshot {
                if !self.snapshots.iter().any(|s| s.name == *parent_name) {
                    issues.push(format!("Snapshot '{}' references missing parent '{}'", 
                                      snapshot.name, parent_name));
                }
            }
            
            // Check sequence number consistency
            if let Some(parent) = self.get_parent(&snapshot.name) {
                if snapshot.sequence_number <= parent.sequence_number {
                    issues.push(format!("Snapshot '{}' has invalid sequence number", 
                                      snapshot.name));
                }
            }
        }
        
        Ok(issues)
    }
    
    /// Find snapshots that can be safely deleted using smart chain-aware logic
    pub fn find_safe_deletion_candidates(
        &self,
        keep_full: usize,
        workspace: &crate::workspace::SnapbaseWorkspace,
    ) -> Result<Vec<&SnapshotMetadata>> {
        let mut candidates = Vec::new();
        
        // Count total archives
        let mut archives_with_files = Vec::new();
        for snapshot in &self.snapshots {
            let (archive_path, _) = workspace.snapshot_paths(&snapshot.name);
            if archive_path.exists() {
                archives_with_files.push(snapshot);
            }
        }
        
        // If we don't have more archives than the minimum, nothing to delete
        if archives_with_files.len() <= keep_full {
            return Ok(candidates);
        }
        
        // Smart deletion strategy:
        // 1. Always keep the head (latest snapshot)
        // 2. Keep snapshots that are needed for reconstruction chains
        // 3. Delete from oldest to newest, but only if safe
        
        let head_name = self.head.as_ref();
        let mut essential_snapshots = std::collections::HashSet::new();
        
        // Mark head as essential
        if let Some(head) = head_name {
            essential_snapshots.insert(head.clone());
        }
        
        // Mark snapshots needed for reconstruction chains as essential
        for snapshot in &self.snapshots {
            if self.is_needed_for_reconstruction(&snapshot.name) {
                essential_snapshots.insert(snapshot.name.clone());
            }
        }
        
        // Find candidates for deletion (oldest first)
        let mut sorted_archives = archives_with_files.clone();
        sorted_archives.sort_by_key(|s| s.sequence_number);
        
        let mut archives_to_keep = archives_with_files.len();
        
        for snapshot in sorted_archives {
            // Don't delete if it's essential
            if essential_snapshots.contains(&snapshot.name) {
                continue;
            }
            
            // Don't delete if it would leave us with too few archives
            if archives_to_keep <= keep_full {
                break;
            }
            
            // Check if this snapshot can be safely deleted
            if self.can_safely_delete(&snapshot.name) {
                candidates.push(snapshot);
                archives_to_keep -= 1;
            }
        }
        
        Ok(candidates)
    }

    /// Find snapshots that can have their full data cleaned up (selective cleanup)
    pub fn find_data_cleanup_candidates(
        &self,
        keep_full: usize,
        workspace: &crate::workspace::SnapbaseWorkspace,
    ) -> Result<Vec<&SnapshotMetadata>> {
        let mut candidates = Vec::new();
        
        // Count total archives
        let mut archives_with_files = Vec::new();
        for snapshot in &self.snapshots {
            let (archive_path, _) = workspace.snapshot_paths(&snapshot.name);
            if archive_path.exists() {
                archives_with_files.push(snapshot);
            }
        }
        
        // Data cleanup strategy:
        // 1. Always keep full data for the most recent N snapshots (head + keep_full-1)
        // 2. Remove full data from ALL other snapshots but preserve deltas
        // 3. Can reconstruct any snapshot through delta chains from head
        
        // Sort archives by sequence number (newest first, so head is first)
        let mut sorted_archives = archives_with_files.clone();
        sorted_archives.sort_by_key(|s| std::cmp::Reverse(s.sequence_number));
        
        // Keep full data for the most recent keep_full snapshots
        for (index, snapshot) in sorted_archives.iter().enumerate() {
            // Clean up all snapshots except the most recent keep_full
            if index >= keep_full {
                // Check if this snapshot can be reconstructed from the chain
                if self.can_be_reconstructed(&snapshot.name) {
                    candidates.push(*snapshot);
                }
            }
        }
        
        Ok(candidates)
    }
    
    /// Check if a snapshot is needed for reconstruction of other snapshots
    fn is_needed_for_reconstruction(&self, snapshot_name: &str) -> bool {
        // A snapshot is needed if:
        // 1. It's the head (latest)
        // 2. It has children that depend on it for reconstruction
        // 3. It's part of a critical reconstruction path
        
        if let Some(head) = &self.head {
            if head == snapshot_name {
                return true;
            }
        }
        
        // Check if any children need this snapshot for reconstruction
        for child in &self.snapshots {
            if let Some(parent) = &child.parent_snapshot {
                if parent == snapshot_name {
                    // This snapshot has children - check if they can reconstruct it
                    if !child.can_reconstruct_parent {
                        // Child cannot reconstruct this parent, so parent is essential
                        return true;
                    }
                }
            }
        }
        
        false
    }

    /// Check if a snapshot can be reconstructed from the chain
    fn can_be_reconstructed(&self, snapshot_name: &str) -> bool {
        // A snapshot can be reconstructed if:
        // 1. There's a path from the head to this snapshot through deltas
        // 2. OR it has a child that can reconstruct it
        
        // Check if any child can reconstruct this snapshot
        for child in &self.snapshots {
            if let Some(parent) = &child.parent_snapshot {
                if parent == snapshot_name && child.can_reconstruct_parent {
                    return true;
                }
            }
        }
        
        // Check if we can trace a path from head to this snapshot
        if let Some(head) = &self.head {
            if let Some(path) = self.find_path_to_snapshot(snapshot_name) {
                // If there's a path and it's not just the snapshot itself, it can be reconstructed
                return path.len() > 1 || head == snapshot_name;
            }
        }
        
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs;

    #[test]
    fn test_snapshot_metadata_serialization() {
        let metadata = SnapshotMetadata {
            format_version: "1.0.0".to_string(),
            name: "test".to_string(),
            created: Utc::now(),
            source: "test.csv".to_string(),
            source_hash: "abc123".to_string(),
            row_count: 100,
            column_count: 3,
            columns: Vec::new(),
            archive_size: Some(1024),
            parent_snapshot: None,
            sequence_number: 0,
            delta_from_parent: None,
            can_reconstruct_parent: false,
            source_path: Some("/path/to/test.csv".to_string()),
            source_fingerprint: Some("test_fingerprint".to_string()),
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: SnapshotMetadata = serde_json::from_str(&json).unwrap();
        
        assert_eq!(metadata.name, deserialized.name);
        assert_eq!(metadata.row_count, deserialized.row_count);
        assert_eq!(metadata.sequence_number, deserialized.sequence_number);
    }

    #[test]
    fn test_snapshot_loader() {
        let temp_dir = TempDir::new().unwrap();
        let json_path = temp_dir.path().join("test.json");
        
        let metadata = SnapshotMetadata {
            format_version: "1.0.0".to_string(),
            name: "test".to_string(),
            created: Utc::now(),
            source: "test.csv".to_string(),
            source_hash: "abc123".to_string(),
            row_count: 100,
            column_count: 3,
            columns: Vec::new(),
            archive_size: Some(1024),
            parent_snapshot: None,
            sequence_number: 0,
            delta_from_parent: None,
            can_reconstruct_parent: false,
            source_path: Some("/path/to/test.csv".to_string()),
            source_fingerprint: Some("test_fingerprint".to_string()),
        };

        let json_content = serde_json::to_string_pretty(&metadata).unwrap();
        fs::write(&json_path, json_content).unwrap();

        let loaded = SnapshotLoader::load_metadata(&json_path).unwrap();
        assert_eq!(loaded.name, "test");
        assert_eq!(loaded.row_count, 100);
    }
}
