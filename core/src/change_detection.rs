//! Memory-efficient streaming change detection system for snapbase

use crate::error::Result;
use crate::hash::ColumnInfo;
use crate::workspace::SnapbaseWorkspace;
use crate::data::DataProcessor;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

// Type alias for complex return types
type ChangeResult = (Vec<(u64, u64)>, Vec<u64>, Vec<u64>);

/// Memory-efficient streaming change detector
pub struct StreamingChangeDetector;

/// Lightweight row hash with index for memory efficiency
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowHashEntry {
    pub row_index: u64,
    pub hash: String,
}

/// Set of row hashes for efficient comparison
#[derive(Debug)]
pub struct RowHashSet {
    pub hashes: HashMap<String, Vec<u64>>, // hash -> list of row_indices (handles duplicates)
    pub indices: HashSet<u64>,             // set of row indices for fast lookup
}

/// Identifies which rows have changed between datasets
#[derive(Debug)]
pub struct ChangedRowsResult {
    pub baseline_changed: Vec<u64>,   // row indices in baseline that are modified/removed
    pub current_changed: Vec<u64>,    // row indices in current that are modified/added  
    pub unchanged_count: usize,       // number of rows that didn't change
}

/// Data source for streaming comparison
#[derive(Debug, Clone)]
pub enum DataSource {
    /// Compare against a file (CSV, JSON, Parquet)
    File(PathBuf),
    /// Compare against a stored snapshot  
    StoredSnapshot {
        path: String,
        workspace: SnapbaseWorkspace,
    },
    /// Compare against a database query result
    DatabaseQuery {
        connection_string: String,
        query: String,
    },
}

/// Configuration options for streaming comparison
#[derive(Debug, Clone)]
pub struct ComparisonOptions {
    /// Exclude snapbase metadata columns from comparison
    pub exclude_metadata_columns: bool,
    /// Enable progress reporting during comparison
    pub progress_reporting: bool,
    /// Memory limit for streaming operations (None = unlimited)
    pub memory_limit: Option<usize>,
    /// Sample size for progress reporting
    pub progress_sample_size: u64,
}

impl Default for ComparisonOptions {
    fn default() -> Self {
        Self {
            exclude_metadata_columns: true,
            progress_reporting: true,
            memory_limit: None,
            progress_sample_size: 10000,
        }
    }
}

/// Progress information during streaming comparison
#[derive(Debug, Clone)]
pub struct StreamingProgress {
    pub phase: StreamingPhase,
    pub processed_rows: u64,
    pub total_rows: u64,
    pub message: String,
}

/// Phases of streaming comparison
#[derive(Debug, Clone, PartialEq)]
pub enum StreamingPhase {
    BuildingHashSets,
    IdentifyingChanges,
    AnalyzingDetails,
    Complete,
}

impl Default for RowHashSet {
    fn default() -> Self {
        Self::new()
    }
}

impl RowHashSet {
    pub fn new() -> Self {
        Self {
            hashes: HashMap::new(),
            indices: HashSet::new(),
        }
    }
    
    pub fn add_row(&mut self, row_index: u64, hash: String) {
        self.hashes.entry(hash).or_insert_with(Vec::new).push(row_index);
        self.indices.insert(row_index);
    }
    
    pub fn contains_hash(&self, hash: &str) -> bool {
        self.hashes.contains_key(hash)
    }
    
    pub fn get_rows_by_hash(&self, hash: &str) -> Option<&Vec<u64>> {
        self.hashes.get(hash)
    }
    
    pub fn get_first_row_by_hash(&self, hash: &str) -> Option<u64> {
        self.hashes.get(hash).and_then(|rows| rows.first().copied())
    }
    
    pub fn len(&self) -> usize {
        self.hashes.len()
    }
}

impl StreamingChangeDetector {
    /// Memory-efficient streaming change detection - Phase 1: Build hash sets
    /// This streams through both datasets once to build lightweight hash sets
    pub async fn build_row_hash_sets<F1, F2, Fut1, Fut2>(
        mut baseline_stream: F1,
        mut current_stream: F2,
        progress_callback: Option<&dyn Fn(u64, u64, &str)>,
    ) -> Result<(RowHashSet, RowHashSet)>
    where
        F1: FnMut() -> Fut1,
        F2: FnMut() -> Fut2,
        Fut1: std::future::Future<Output = Result<Option<(u64, Vec<String>)>>>,
        Fut2: std::future::Future<Output = Result<Option<(u64, Vec<String>)>>>,
    {
        let mut baseline_hashes = RowHashSet::new();
        let mut current_hashes = RowHashSet::new();
        
        let mut processed_baseline = 0u64;
        let mut processed_current = 0u64;
        
        // Phase 1a: Stream baseline dataset and build hash set
        if let Some(callback) = progress_callback {
            callback(0, 0, "Building baseline hash set...");
        }
        
        loop {
            match baseline_stream().await? {
                Some((row_index, row_data)) => {
                    let hash = Self::compute_row_hash(&row_data);
                    baseline_hashes.add_row(row_index, hash);
                    processed_baseline += 1;
                    
                    if processed_baseline % 50000 == 0 {
                        if let Some(callback) = progress_callback {
                            callback(processed_baseline, 0, "Processing baseline rows...");
                        }
                    }
                }
                None => break,
            }
        }
        
        // Phase 1b: Stream current dataset and build hash set
        if let Some(callback) = progress_callback {
            callback(processed_baseline, 0, "Building current hash set...");
        }
        
        loop {
            match current_stream().await? {
                Some((row_index, row_data)) => {
                    let hash = Self::compute_row_hash(&row_data);
                    current_hashes.add_row(row_index, hash);
                    processed_current += 1;
                    
                    if processed_current % 50000 == 0 {
                        if let Some(callback) = progress_callback {
                            callback(processed_baseline + processed_current, 0, "Processing current rows...");
                        }
                    }
                }
                None => break,
            }
        }
        
        if let Some(callback) = progress_callback {
            callback(
                processed_baseline + processed_current, 
                processed_baseline + processed_current, 
                "Hash sets built successfully"
            );
        }
        
        Ok((baseline_hashes, current_hashes))
    }
    
    /// Phase 2: Compare hash sets to identify changed rows (memory efficient)
    pub fn identify_changed_rows(
        baseline_hashes: &RowHashSet,
        current_hashes: &RowHashSet,
    ) -> ChangedRowsResult {
        let mut baseline_changed = Vec::new();
        let mut current_changed = Vec::new();
        let mut unchanged_count = 0;
        
        // Find rows in baseline that don't exist in current (removed or modified)
        for (hash, row_indices) in &baseline_hashes.hashes {
            if !current_hashes.contains_hash(hash) {
                // All instances of this hash are removed/modified
                baseline_changed.extend(row_indices);
            } else {
                // This hash exists in both datasets - count as unchanged
                unchanged_count += row_indices.len();
            }
        }
        
        // Find rows in current that don't exist in baseline (added or modified)
        for (hash, row_indices) in &current_hashes.hashes {
            if !baseline_hashes.contains_hash(hash) {
                // All instances of this hash are added/modified
                current_changed.extend(row_indices);
            }
        }
        
        ChangedRowsResult {
            baseline_changed,
            current_changed,
            unchanged_count,
        }
    }
    
    /// Phase 3: Load only changed rows and perform detailed analysis
    /// This is where we load the actual row data for changed rows only
    pub async fn analyze_changed_rows(
        changed_rows: &ChangedRowsResult,
        baseline_schema: &[ColumnInfo],
        current_schema: &[ColumnInfo],
        baseline_changed_data: HashMap<u64, Vec<String>>,
        current_changed_data: HashMap<u64, Vec<String>>,
    ) -> Result<ChangeDetectionResult>
    {
        // Perform detailed analysis only on changed rows
        let schema_changes = Self::detect_schema_changes(baseline_schema, current_schema)?;
        let row_changes = Self::classify_changed_rows_detailed(
            changed_rows,
            baseline_schema,
            current_schema,
            &baseline_changed_data,
            &current_changed_data,
        )?;
        
        Ok(ChangeDetectionResult {
            schema_changes,
            row_changes,
        })
    }
    
    /// Classify changed rows into additions, modifications, and removals
    /// This operates only on the subset of changed rows, not the entire dataset
    fn classify_changed_rows_detailed(
        changed_rows: &ChangedRowsResult,
        baseline_schema: &[ColumnInfo],
        current_schema: &[ColumnInfo],
        baseline_data: &HashMap<u64, Vec<String>>,
        current_data: &HashMap<u64, Vec<String>>,
    ) -> Result<RowChanges> {
        // Create column mapping for schema-aware comparison
        let common_columns = Self::find_common_columns(baseline_schema, current_schema);
        
        let mut modifications = Vec::new();
        let mut genuine_additions = Vec::new();
        let mut genuine_removals = Vec::new();
        
        // Strategy: Use content-based matching to identify modifications vs genuine adds/removes
        // Since we already know these rows changed (different hashes), we need to determine
        // if they're modifications of existing rows or genuine additions/removals
        
        let mut unmatched_baseline: Vec<u64> = changed_rows.baseline_changed.clone();
        let mut unmatched_current: Vec<u64> = changed_rows.current_changed.clone();
        
        // Try to match changed baseline rows with changed current rows based on similarity
        if !unmatched_baseline.is_empty() && !unmatched_current.is_empty() && !common_columns.is_empty() {
            let matches = Self::find_content_matches_from_maps(
                &unmatched_baseline,
                &unmatched_current,
                baseline_data,
                current_data,
                &common_columns,
                baseline_schema,
                current_schema,
            )?;
            
            #[cfg(debug_assertions)]
            {
                eprintln!("DEBUG Content matching:");
                eprintln!("  unmatched_baseline: {:?}", unmatched_baseline);
                eprintln!("  unmatched_current: {:?}", unmatched_current);
                eprintln!("  common_columns: {:?}", common_columns);
                eprintln!("  matches found: {:?}", matches);
            }
            
            // Record matched pairs as modifications
            for (baseline_idx, current_idx) in matches {
                // Perform detailed cell-level analysis
                if let (Some(baseline_row), Some(current_row)) = 
                    (baseline_data.get(&baseline_idx), current_data.get(&current_idx)) {
                    
                    let changes = Self::compare_rows_schema_aware_from_schemas(
                        baseline_row,
                        current_row,
                        baseline_schema,
                        current_schema,
                    );
                    
                    #[cfg(debug_assertions)]
                    {
                        eprintln!("DEBUG Row comparison baseline_idx={} current_idx={}:", baseline_idx, current_idx);
                        eprintln!("  baseline_row: {:?}", baseline_row);
                        eprintln!("  current_row: {:?}", current_row);
                        eprintln!("  changes found: {:?}", changes);
                    }
                    
                    if !changes.is_empty() {
                        modifications.push(RowModification {
                            row_index: current_idx,
                            changes,
                        });
                        
                        #[cfg(debug_assertions)]
                        {
                            eprintln!("  -> Added to modifications");
                        }
                    } else {
                        #[cfg(debug_assertions)]
                        {
                            eprintln!("  -> No changes found, not added to modifications");
                        }
                    }
                }
                
                // Remove from unmatched lists
                unmatched_baseline.retain(|&x| x != baseline_idx);
                unmatched_current.retain(|&x| x != current_idx);
            }
        }
        
        // Remaining unmatched baseline rows are genuine removals
        for &baseline_idx in &unmatched_baseline {
            if let Some(baseline_row) = baseline_data.get(&baseline_idx) {
                let mut data = HashMap::new();
                for (col_idx, col) in baseline_schema.iter().enumerate() {
                    if let Some(value) = baseline_row.get(col_idx) {
                        data.insert(col.name.clone(), value.clone());
                    }
                }
                genuine_removals.push(RowRemoval {
                    row_index: baseline_idx,
                    data,
                });
            }
        }
        
        // Remaining unmatched current rows are genuine additions
        for &current_idx in &unmatched_current {
            if let Some(current_row) = current_data.get(&current_idx) {
                let mut data = HashMap::new();
                for (col_idx, col) in current_schema.iter().enumerate() {
                    if let Some(value) = current_row.get(col_idx) {
                        data.insert(col.name.clone(), value.clone());
                    }
                }
                genuine_additions.push(RowAddition {
                    row_index: current_idx,
                    data,
                });
            }
        }
        
        Ok(RowChanges {
            modified: modifications,
            added: genuine_additions,
            removed: genuine_removals,
        })
    }
    
    /// Find content matches between changed baseline and current rows using similarity scoring
    fn find_content_matches_from_maps(
        baseline_indices: &[u64],
        current_indices: &[u64],
        baseline_data: &HashMap<u64, Vec<String>>,
        current_data: &HashMap<u64, Vec<String>>,
        common_columns: &[String],
        baseline_schema: &[ColumnInfo],
        current_schema: &[ColumnInfo],
    ) -> Result<Vec<(u64, u64)>> {
        // Create column index mappings
        let baseline_col_map: HashMap<String, usize> = baseline_schema
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
            
        let current_col_map: HashMap<String, usize> = current_schema
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
        
        let mut matches = Vec::new();
        let mut used_current_indices = HashSet::new();
        
        // For each baseline row, find the best matching current row
        for &baseline_idx in baseline_indices {
            if let Some(baseline_row) = baseline_data.get(&baseline_idx) {
                let mut best_match = None;
                let mut best_similarity = 0.5; // Minimum threshold
                
                for &current_idx in current_indices {
                    if used_current_indices.contains(&current_idx) {
                        continue; // Already matched
                    }
                    
                    if let Some(current_row) = current_data.get(&current_idx) {
                        let similarity = Self::calculate_row_similarity_with_maps(
                            baseline_row,
                            current_row,
                            common_columns,
                            &baseline_col_map,
                            &current_col_map,
                        );
                        
                        #[cfg(debug_assertions)]
                        {
                            eprintln!("    Similarity baseline_idx={} current_idx={}: {:.2}", baseline_idx, current_idx, similarity);
                        }
                        
                        if similarity > best_similarity {
                            best_similarity = similarity;
                            best_match = Some(current_idx);
                        }
                    }
                }
                
                // Record the best match if found
                if let Some(matched_current_idx) = best_match {
                    matches.push((baseline_idx, matched_current_idx));
                    used_current_indices.insert(matched_current_idx);
                }
            }
        }
        
        Ok(matches)
    }
    
    /// Calculate similarity between two rows using column mappings
    fn calculate_row_similarity_with_maps(
        baseline_row: &[String],
        current_row: &[String],
        common_columns: &[String],
        baseline_col_map: &HashMap<String, usize>,
        current_col_map: &HashMap<String, usize>,
    ) -> f64 {
        let mut matches = 0;
        let mut total = 0;
        
        #[cfg(debug_assertions)]
        {
            eprintln!("      Comparing rows:");
            eprintln!("        baseline_row: {:?}", baseline_row);
            eprintln!("        current_row: {:?}", current_row);
            eprintln!("        common_columns: {:?}", common_columns);
            eprintln!("        baseline_col_map: {:?}", baseline_col_map);
            eprintln!("        current_col_map: {:?}", current_col_map);
        }
        
        for col_name in common_columns {
            if let (Some(&baseline_idx), Some(&current_idx)) = 
                (baseline_col_map.get(col_name), current_col_map.get(col_name)) {
                
                if let (Some(baseline_val), Some(current_val)) = 
                    (baseline_row.get(baseline_idx), current_row.get(current_idx)) {
                    
                    total += 1;
                    let is_match = baseline_val == current_val;
                    if is_match {
                        matches += 1;
                    }
                    
                    #[cfg(debug_assertions)]
                    {
                        eprintln!("        {}: '{}' vs '{}' -> {}", col_name, baseline_val, current_val, if is_match { "MATCH" } else { "DIFF" });
                    }
                } else {
                    #[cfg(debug_assertions)]
                    {
                        eprintln!("        {}: missing values (baseline_idx={:?}, current_idx={:?})", col_name, 
                            baseline_row.get(baseline_idx), current_row.get(current_idx));
                    }
                }
            } else {
                #[cfg(debug_assertions)]
                {
                    eprintln!("        {}: column mapping failed", col_name);
                }
            }
        }
        
        let similarity = if total > 0 {
            matches as f64 / total as f64
        } else {
            0.0
        };
        
        #[cfg(debug_assertions)]
        {
            eprintln!("        -> similarity: {}/{} = {:.2}", matches, total, similarity);
        }
        
        similarity
    }
    
    /// Compare rows with schema awareness using schema arrays
    fn compare_rows_schema_aware_from_schemas(
        baseline_row: &[String],
        current_row: &[String],
        baseline_schema: &[ColumnInfo],
        current_schema: &[ColumnInfo],
    ) -> HashMap<String, CellChange> {
        let mut changes = HashMap::new();
        
        // Create column mappings
        let current_col_map: HashMap<String, usize> = current_schema
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
        
        // Compare common columns only
        for (baseline_idx, baseline_col) in baseline_schema.iter().enumerate() {
            if let Some(&current_idx) = current_col_map.get(&baseline_col.name) {
                let baseline_value = baseline_row.get(baseline_idx).map(|s| s.as_str()).unwrap_or("");
                let current_value = current_row.get(current_idx).map(|s| s.as_str()).unwrap_or("");
                
                if baseline_value != current_value {
                    changes.insert(baseline_col.name.clone(), CellChange {
                        before: baseline_value.to_string(),
                        after: current_value.to_string(),
                    });
                }
            }
        }
        
        changes
    }
    
    /// Find common columns between schemas
    fn find_common_columns(baseline_schema: &[ColumnInfo], current_schema: &[ColumnInfo]) -> Vec<String> {
        let current_names: HashSet<_> = current_schema.iter().map(|c| &c.name).collect();
        baseline_schema
            .iter()
            .filter_map(|col| {
                if current_names.contains(&col.name) {
                    Some(col.name.clone())
                } else {
                    None
                }
            })
            .collect()
    }
    
    /// Schema change detection (unchanged from original)
    fn detect_schema_changes(
        baseline: &[ColumnInfo],
        current: &[ColumnInfo],
    ) -> Result<SchemaChanges> {
        let baseline_names: Vec<String> = baseline.iter().map(|c| c.name.clone()).collect();
        let current_names: Vec<String> = current.iter().map(|c| c.name.clone()).collect();

        // Detect column order changes (if column names are reordered)
        let column_order = if baseline_names != current_names && baseline.len() == current.len() {
            // Check if it's just a reordering (same columns, different order)
            let mut baseline_sorted = baseline_names.clone();
            let mut current_sorted = current_names.clone();
            baseline_sorted.sort();
            current_sorted.sort();
            
            if baseline_sorted == current_sorted {
                Some(ColumnOrderChange {
                    before: baseline_names.clone(),
                    after: current_names.clone(),
                })
            } else {
                None // Not just reordering, there are additions/removals/renames
            }
        } else {
            None
        };

        let mut columns_added = Vec::new();
        let mut columns_removed = Vec::new();
        let mut columns_renamed = Vec::new();
        let mut type_changes = Vec::new();

        // Handle different column counts (additions/removals)
        if baseline.len() != current.len() {
            if current.len() > baseline.len() {
                // Columns were added at the end
                for (pos, col) in current.iter().enumerate().skip(baseline.len()) {
                    columns_added.push(ColumnAddition {
                        name: col.name.clone(),
                        data_type: col.data_type.clone(),
                        position: pos,
                        nullable: col.nullable,
                        default_value: None,
                    });
                }
            } else {
                // Columns were removed from the end
                for (pos, col) in baseline.iter().enumerate().skip(current.len()) {
                    columns_removed.push(ColumnRemoval {
                        name: col.name.clone(),
                        data_type: col.data_type.clone(),
                        position: pos,
                        nullable: col.nullable,
                    });
                }
            }
        }

        // Compare columns position by position (for common length)
        let min_len = baseline.len().min(current.len());
        for pos in 0..min_len {
            let baseline_col = &baseline[pos];
            let current_col = &current[pos];

            // Check for column rename at this position
            if baseline_col.name != current_col.name {
                columns_renamed.push(ColumnRename {
                    from: baseline_col.name.clone(),
                    to: current_col.name.clone(),
                });
            }

            // Check for type change at this position
            if baseline_col.data_type != current_col.data_type {
                type_changes.push(TypeChange {
                    column: current_col.name.clone(), // Use current name in case it was renamed
                    from: baseline_col.data_type.clone(),
                    to: current_col.data_type.clone(),
                });
            }
        }

        Ok(SchemaChanges {
            column_order,
            columns_added,
            columns_removed,
            columns_renamed,
            type_changes,
        })
    }
    
    /// Compute hash for a row (consistent with existing hash computation)
    /// TODO: Debug why identical data produces different hashes between CSV and Parquet
    pub fn compute_row_hash(row_values: &[String]) -> String {
        use blake3;
        
        // Debug: Log the actual values being hashed to identify inconsistencies
        #[cfg(debug_assertions)]
        if log::log_enabled!(log::Level::Debug) {
            log::debug!("Hashing row values: {:?}", row_values);
        }
        
        let row_content = row_values.join("||"); // Use || to avoid conflicts
        let hash = blake3::hash(row_content.as_bytes());
        
        #[cfg(debug_assertions)]
        if log::log_enabled!(log::Level::Debug) {
            log::debug!("Row content: '{}' -> hash: {}", row_content, hash.to_hex());
        }
        
        hash.to_hex().to_string()
    }

    /// High-level streaming comparison API - combines all three phases
    /// This is the main entry point for CLI, Python, and Java wrappers
    pub async fn compare_data_sources(
        baseline_source: DataSource,
        current_source: DataSource,
        options: ComparisonOptions,
        progress_callback: Option<Box<dyn Fn(StreamingProgress) + Send>>,
    ) -> Result<ChangeDetectionResult> {
        // Phase 1: Build hash sets from both data sources
        let (baseline_hashes, current_hashes) = Self::build_hash_sets_from_sources(
            &baseline_source,
            &current_source,
            &options,
            progress_callback.as_ref(),
        ).await?;

        // Phase 2: Identify changed rows using hash comparison
        if let Some(ref callback) = progress_callback {
            callback(StreamingProgress {
                phase: StreamingPhase::IdentifyingChanges,
                processed_rows: 0,
                total_rows: 0,
                message: "Identifying changed rows...".to_string(),
            });
        }
        
        let changed_rows = Self::identify_changed_rows(&baseline_hashes, &current_hashes);
        
        #[cfg(debug_assertions)]
        {
            eprintln!("DEBUG ChangedRowsResult:");
            eprintln!("  baseline_changed: {:?}", changed_rows.baseline_changed);
            eprintln!("  current_changed: {:?}", changed_rows.current_changed); 
            eprintln!("  unchanged_count: {}", changed_rows.unchanged_count);
        }
        
        // Phase 3: Detailed analysis of changed rows only
        if changed_rows.baseline_changed.is_empty() && changed_rows.current_changed.is_empty() {
            // No changes detected - return empty result
            return Ok(ChangeDetectionResult {
                schema_changes: SchemaChanges {
                    column_order: None,
                    columns_added: Vec::new(),
                    columns_removed: Vec::new(),
                    columns_renamed: Vec::new(),
                    type_changes: Vec::new(),
                },
                row_changes: RowChanges {
                    modified: Vec::new(),
                    added: Vec::new(),
                    removed: Vec::new(),
                },
            });
        }

        if let Some(ref callback) = progress_callback {
            callback(StreamingProgress {
                phase: StreamingPhase::AnalyzingDetails,
                processed_rows: 0,
                total_rows: changed_rows.baseline_changed.len() as u64 + changed_rows.current_changed.len() as u64,
                message: format!("Analyzing {} changed rows in detail...", changed_rows.baseline_changed.len() + changed_rows.current_changed.len()),
            });
        }

        let final_result = Self::analyze_changed_data_sources(&changed_rows, &baseline_source, &current_source, &options).await?;
        
        #[cfg(debug_assertions)]
        {
            eprintln!("DEBUG Final ChangeDetectionResult:");
            eprintln!("  schema_changes.has_changes(): {}", final_result.schema_changes.has_changes());
            eprintln!("  row_changes.has_changes(): {}", final_result.row_changes.has_changes());
            eprintln!("  modified: {}", final_result.row_changes.modified.len());
            eprintln!("  added: {}", final_result.row_changes.added.len());
            eprintln!("  removed: {}", final_result.row_changes.removed.len());
        }
        
        Ok(final_result)
    }

    /// Phase 1: Build hash sets from data sources with built-in filtering
    async fn build_hash_sets_from_sources(
        baseline_source: &DataSource,
        current_source: &DataSource,
        options: &ComparisonOptions,
        progress_callback: Option<&Box<dyn Fn(StreamingProgress) + Send>>,
    ) -> Result<(RowHashSet, RowHashSet)> {
        // Load baseline data
        let (baseline_data, baseline_schema) = Self::load_data_from_source(baseline_source).await?;
        
        // Load current data
        let (current_data, current_schema) = Self::load_data_from_source(current_source).await?;
        
        // Get original column count for filtering (exclude metadata)
        let original_column_count = if options.exclude_metadata_columns {
            baseline_schema.len() // Original columns from schema, not including snapbase metadata
        } else {
            baseline_data.first().map(|row| row.len()).unwrap_or(0)
        };

        let mut baseline_hashes = RowHashSet::new();
        let mut current_hashes = RowHashSet::new();

        // Build baseline hash set with filtering
        for (index, row) in baseline_data.into_iter().enumerate() {
            let filtered_row = if options.exclude_metadata_columns {
                row.iter().take(original_column_count).cloned().collect()
            } else {
                row
            };
            let hash = Self::compute_row_hash(&filtered_row);
            #[cfg(debug_assertions)]
            if index < 5 { // Only debug first 5 rows
                eprintln!("DEBUG baseline row {}: {:?} -> hash: {}", index, filtered_row, hash);
            }
            baseline_hashes.add_row(index as u64, hash);
        }

        // Build current hash set with filtering  
        for (index, row) in current_data.into_iter().enumerate() {
            let filtered_row = if options.exclude_metadata_columns {
                row.iter().take(original_column_count).cloned().collect()
            } else {
                row
            };
            let hash = Self::compute_row_hash(&filtered_row);
            #[cfg(debug_assertions)]
            if index < 5 { // Only debug first 5 rows
                eprintln!("DEBUG current row {}: {:?} -> hash: {}", index, filtered_row, hash);
            }
            current_hashes.add_row(index as u64, hash);
        }

        if let Some(callback) = progress_callback {
            callback(StreamingProgress {
                phase: StreamingPhase::BuildingHashSets,
                processed_rows: baseline_hashes.len() as u64 + current_hashes.len() as u64,
                total_rows: baseline_hashes.len() as u64 + current_hashes.len() as u64,
                message: "Hash sets built successfully".to_string(),
            });
        }

        Ok((baseline_hashes, current_hashes))
    }

    /// Phase 3: Analyze changed rows from data sources  
    async fn analyze_changed_data_sources(
        changed_rows: &ChangedRowsResult,
        baseline_source: &DataSource,
        current_source: &DataSource,
        options: &ComparisonOptions,
    ) -> Result<ChangeDetectionResult> {
        // Load schemas for analysis
        let (_, baseline_schema) = Self::load_data_from_source(baseline_source).await?;
        let (_, current_schema) = Self::load_data_from_source(current_source).await?;

        // Load only the changed rows 
        let baseline_changed_data = Self::load_specific_rows_from_source(
            baseline_source, 
            &changed_rows.baseline_changed,
            options
        ).await?;
        
        let current_changed_data = Self::load_specific_rows_from_source(
            current_source,
            &changed_rows.current_changed, 
            options
        ).await?;

        // Delegate to existing detailed analysis
        Self::analyze_changed_rows(
            changed_rows,
            &baseline_schema,
            &current_schema,
            baseline_changed_data,
            current_changed_data,
        ).await
    }

    /// Load data from any data source type
    async fn load_data_from_source(source: &DataSource) -> Result<(Vec<Vec<String>>, Vec<ColumnInfo>)> {
        match source {
            DataSource::File(path) => {
                let mut processor = DataProcessor::new()?;
                let data_info = processor.load_file(path)?;
                let data = processor.stream_rows_async(None::<fn(u64, u64, &str)>).await?;
                let row_data: Vec<Vec<String>> = data.into_iter().map(|(_, row)| row).collect();
                Ok((row_data, data_info.columns))
            },
            DataSource::StoredSnapshot { path, workspace } => {
                let mut processor = DataProcessor::new_with_workspace(workspace)?;
                let data = processor.load_cloud_storage_data(path, workspace).await?;
                
                // Load actual schema from snapshot metadata instead of creating fake schema
                let metadata_path = path.replace("data.parquet", "metadata.json");
                let metadata_data = workspace.storage().read_file(&metadata_path).await
                    .map_err(|e| crate::error::SnapbaseError::data_processing(
                        format!("Failed to load snapshot metadata from '{}': {}", metadata_path, e)
                    ))?;
                    
                let metadata: crate::snapshot::SnapshotMetadata = serde_json::from_slice(&metadata_data)
                    .map_err(|e| crate::error::SnapbaseError::data_processing(
                        format!("Failed to parse snapshot metadata: {}", e)
                    ))?;
                
                // Use the actual schema from snapshot metadata
                Ok((data, metadata.columns))
            },
            DataSource::DatabaseQuery { connection_string: _, query: _ } => {
                // TODO: Implement database query support
                unimplemented!("Database query support not yet implemented")
            },
        }
    }

    /// Load specific rows from any data source type
    async fn load_specific_rows_from_source(
        source: &DataSource,
        row_indices: &[u64],
        options: &ComparisonOptions,
    ) -> Result<HashMap<u64, Vec<String>>> {
        if row_indices.is_empty() {
            return Ok(HashMap::new());
        }

        match source {
            DataSource::File(path) => {
                let mut processor = DataProcessor::new()?;
                let data_info = processor.load_file(path)?;
                let full_data = processor.load_specific_rows(row_indices).await?;
                
                // Files don't have metadata columns, but may have row index as first column
                if options.exclude_metadata_columns {
                    let expected_column_count = data_info.columns.len(); // Get expected column count from schema
                    
                    // Check if row data includes the row index as first column
                    let filtered_data: HashMap<u64, Vec<String>> = full_data.into_iter()
                        .map(|(row_index, row_data)| {
                            // If the first element is the row index as string, skip it and take expected columns
                            if !row_data.is_empty() && row_data[0] == row_index.to_string() {
                                let filtered_row: Vec<String> = row_data.iter()
                                    .skip(1)  // Skip row index
                                    .take(expected_column_count)  // Take all expected data columns
                                    .cloned()
                                    .collect();
                                
                                #[cfg(debug_assertions)]
                                {
                                    eprintln!("DEBUG File row filtering: row_index={} row_data({} cols): {:?}", row_index, row_data.len(), row_data);
                                    eprintln!("  Expected {} columns, taking after skip(1): {:?}", expected_column_count, filtered_row);
                                }
                                
                                (row_index, filtered_row)
                            } else {
                                // No row index contamination - take expected columns as-is
                                let filtered_row: Vec<String> = row_data.iter()
                                    .take(expected_column_count)
                                    .cloned()
                                    .collect();
                                (row_index, filtered_row)
                            }
                        })
                        .collect();
                    Ok(filtered_data)
                } else {
                    Ok(full_data)
                }
            },
            DataSource::StoredSnapshot { path, workspace } => {
                let mut processor = DataProcessor::new_with_workspace(workspace)?;
                let full_data = processor.load_specific_rows_from_storage(path, workspace, row_indices).await?;
                
                // Apply filtering to exclude snapbase metadata columns
                if options.exclude_metadata_columns {
                    // Load schema to get original column count
                    let metadata_path = path.replace("data.parquet", "metadata.json");
                    let metadata_data = workspace.storage().read_file(&metadata_path).await
                        .map_err(|e| crate::error::SnapbaseError::data_processing(
                            format!("Failed to load snapshot metadata for filtering: {}", e)
                        ))?;
                        
                    let metadata: crate::snapshot::SnapshotMetadata = serde_json::from_slice(&metadata_data)
                        .map_err(|e| crate::error::SnapbaseError::data_processing(
                            format!("Failed to parse snapshot metadata for filtering: {}", e)
                        ))?;
                    
                    let original_column_count = metadata.columns.len();
                    
                    #[cfg(debug_assertions)]
                    {
                        eprintln!("DEBUG Filtering: original_column_count={}", original_column_count);
                        eprintln!("  metadata.columns: {:?}", metadata.columns.iter().map(|c| &c.name).collect::<Vec<_>>());
                    }
                    
                    // Filter each row to exclude metadata columns
                    let filtered_data: HashMap<u64, Vec<String>> = full_data.into_iter()
                        .map(|(row_index, row_data)| {
                            #[cfg(debug_assertions)]
                            {
                                eprintln!("DEBUG Raw row_index={} row_data({} cols): {:?}", row_index, row_data.len(), row_data);
                            }
                            
                            // Skip the first column (row index) and take the next original_column_count columns
                            let filtered_row: Vec<String> = row_data.iter()
                                .skip(1)  // Skip row index
                                .take(original_column_count)
                                .cloned()
                                .collect();
                                
                            #[cfg(debug_assertions)]
                            {
                                eprintln!("  -> Filtered({} cols): {:?}", filtered_row.len(), filtered_row);
                            }
                            
                            (row_index, filtered_row)
                        })
                        .collect();
                    
                    Ok(filtered_data)
                } else {
                    Ok(full_data)
                }
            },
            DataSource::DatabaseQuery { connection_string: _, query: _ } => {
                unimplemented!("Database query support not yet implemented")
            },
        }
    }
}

/// Comprehensive change detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeDetectionResult {
    pub schema_changes: SchemaChanges,
    pub row_changes: RowChanges,
}

/// Schema-level changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChanges {
    pub column_order: Option<ColumnOrderChange>,
    pub columns_added: Vec<ColumnAddition>,
    pub columns_removed: Vec<ColumnRemoval>,
    pub columns_renamed: Vec<ColumnRename>,
    pub type_changes: Vec<TypeChange>,
}

/// Column order change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnOrderChange {
    pub before: Vec<String>,
    pub after: Vec<String>,
}

/// Column addition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnAddition {
    pub name: String,
    pub data_type: String,
    pub position: usize,
    pub nullable: bool,
    pub default_value: Option<String>,
}

/// Column removal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnRemoval {
    pub name: String,
    pub data_type: String,
    pub position: usize,
    pub nullable: bool,
}

/// Column rename
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnRename {
    pub from: String,
    pub to: String,
}

/// Type change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeChange {
    pub column: String,
    pub from: String,
    pub to: String,
}

/// Row-level changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowChanges {
    pub modified: Vec<RowModification>,
    pub added: Vec<RowAddition>,
    pub removed: Vec<RowRemoval>,
}

/// Row modification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowModification {
    pub row_index: u64,
    pub changes: HashMap<String, CellChange>,
}

/// Cell change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellChange {
    pub before: String,
    pub after: String,
}

/// Row addition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowAddition {
    pub row_index: u64,
    pub data: HashMap<String, String>,
}

/// Row removal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowRemoval {
    pub row_index: u64,
    pub data: HashMap<String, String>,
}


/// Change detector for comprehensive analysis
pub struct ChangeDetector;

impl ChangeDetector {
    /// Detect all changes between baseline and current data
    pub fn detect_changes(
        baseline_schema: &[ColumnInfo],
        baseline_data: &[Vec<String>],
        current_schema: &[ColumnInfo],
        current_data: &[Vec<String>],
    ) -> Result<ChangeDetectionResult> {
        let schema_changes = Self::detect_schema_changes(baseline_schema, current_schema)?;
        let row_changes = Self::detect_row_changes(
            baseline_schema,
            baseline_data,
            current_schema,
            current_data,
        )?;
        Ok(ChangeDetectionResult {
            schema_changes,
            row_changes,
        })
    }

    /// Detect schema changes using position-based comparison
    fn detect_schema_changes(
        baseline: &[ColumnInfo],
        current: &[ColumnInfo],
    ) -> Result<SchemaChanges> {
        let baseline_names: Vec<String> = baseline.iter().map(|c| c.name.clone()).collect();
        let current_names: Vec<String> = current.iter().map(|c| c.name.clone()).collect();

        // Detect column order changes (if column names are reordered)
        let column_order = if baseline_names != current_names && baseline.len() == current.len() {
            // Check if it's just a reordering (same columns, different order)
            let mut baseline_sorted = baseline_names.clone();
            let mut current_sorted = current_names.clone();
            baseline_sorted.sort();
            current_sorted.sort();
            
            if baseline_sorted == current_sorted {
                Some(ColumnOrderChange {
                    before: baseline_names.clone(),
                    after: current_names.clone(),
                })
            } else {
                None // Not just reordering, there are additions/removals/renames
            }
        } else {
            None
        };

        let mut columns_added = Vec::new();
        let mut columns_removed = Vec::new();
        let mut columns_renamed = Vec::new();
        let mut type_changes = Vec::new();

        // Handle different column counts (additions/removals)
        if baseline.len() != current.len() {
            if current.len() > baseline.len() {
                // Columns were added at the end
                for (pos, col) in current.iter().enumerate().skip(baseline.len()) {
                    columns_added.push(ColumnAddition {
                        name: col.name.clone(),
                        data_type: col.data_type.clone(),
                        position: pos,
                        nullable: col.nullable,
                        default_value: None,
                    });
                }
            } else {
                // Columns were removed from the end
                for (pos, col) in baseline.iter().enumerate().skip(current.len()) {
                    columns_removed.push(ColumnRemoval {
                        name: col.name.clone(),
                        data_type: col.data_type.clone(),
                        position: pos,
                        nullable: col.nullable,
                    });
                }
            }
        }

        // Compare columns position by position (for common length)
        let min_len = baseline.len().min(current.len());
        for pos in 0..min_len {
            let baseline_col = &baseline[pos];
            let current_col = &current[pos];

            // Check for column rename at this position
            if baseline_col.name != current_col.name {
                columns_renamed.push(ColumnRename {
                    from: baseline_col.name.clone(),
                    to: current_col.name.clone(),
                });
            }

            // Check for type change at this position
            if baseline_col.data_type != current_col.data_type {
                type_changes.push(TypeChange {
                    column: current_col.name.clone(), // Use current name in case it was renamed
                    from: baseline_col.data_type.clone(),
                    to: current_col.data_type.clone(),
                });
            }
        }

        Ok(SchemaChanges {
            column_order,
            columns_added,
            columns_removed,
            columns_renamed,
            type_changes,
        })
    }

    /// Detect row changes using optimized hash-based comparison with intelligent modification detection
    fn detect_row_changes(
        baseline_schema: &[ColumnInfo],
        baseline_data: &[Vec<String>],
        current_schema: &[ColumnInfo],
        current_data: &[Vec<String>],
    ) -> Result<RowChanges> {
        // Phase 1: Fast hash-based filtering to identify changed rows
        let hash_computer = crate::hash::HashComputer::new();
        let baseline_hashes = hash_computer.hash_rows(baseline_data)?;
        let current_hashes = hash_computer.hash_rows(current_data)?;
        let comparison = hash_computer.compare_row_hashes(&baseline_hashes, &current_hashes);
        
        // Phase 2: Intelligent row classification for changed subset only
        let (modifications, genuine_additions, genuine_removals) = Self::classify_changed_rows(
            baseline_schema,
            baseline_data,
            current_schema,
            current_data,
            &comparison.added_rows,
            &comparison.removed_rows,
        )?;
        
        // Phase 3: Parallel cell-level analysis for modifications only
        let detailed_modifications = Self::analyze_modifications_parallel(
            baseline_schema,
            baseline_data,
            current_schema,
            current_data,
            &modifications,
        )?;
        
        // Convert results to final format
        let added = Self::convert_additions_parallel(current_schema, current_data, &genuine_additions)?;
        let removed = Self::convert_removals_parallel(baseline_schema, baseline_data, &genuine_removals)?;

        Ok(RowChanges {
            modified: detailed_modifications,
            added,
            removed,
        })
    }

    /// Classify changed rows into modifications vs genuine additions/removals
    fn classify_changed_rows(
        baseline_schema: &[ColumnInfo],
        baseline_data: &[Vec<String>],
        current_schema: &[ColumnInfo],
        current_data: &[Vec<String>],
        added_indices: &[u64],
        removed_indices: &[u64],
    ) -> Result<ChangeResult> {
        use rayon::prelude::*;
        
        // Early exit if no changes
        if added_indices.is_empty() && removed_indices.is_empty() {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }
        
        // Create column mapping for schema-aware comparison
        let common_columns = Self::find_common_columns(baseline_schema, current_schema);
        
        // Parallel matching: find likely modifications using position and content heuristics
        let mut modifications = Vec::new();
        let mut unmatched_added = added_indices.to_vec();
        let mut unmatched_removed = removed_indices.to_vec();
        
        // Strategy 1: Hash-based exact matching (fastest, most reliable)
        if !unmatched_removed.is_empty() && !unmatched_added.is_empty() {
            let hash_matches = Self::find_hash_matches_parallel(
                baseline_data,
                current_data,
                &unmatched_removed,
                &unmatched_added,
            )?;
            
            for &(removed_idx, added_idx) in &hash_matches {
                modifications.push((removed_idx, added_idx));
                unmatched_removed.retain(|&x| x != removed_idx);
                unmatched_added.retain(|&x| x != added_idx);
            }
        }
        
        // Strategy 2: Content-based matching for remaining rows (handles partial similarities)
        if !unmatched_removed.is_empty() && !unmatched_added.is_empty() && !common_columns.is_empty() {
            let content_matches = Self::find_content_matches_parallel(
                baseline_data,
                current_data,
                &unmatched_removed,
                &unmatched_added,
                &common_columns,
            )?;
            
            for &(removed_idx, added_idx) in &content_matches {
                modifications.push((removed_idx, added_idx));
                unmatched_removed.retain(|&x| x != removed_idx);
                unmatched_added.retain(|&x| x != added_idx);
            }
        }
        
        // Strategy 3: Position-based matching for remaining rows (only when hash and content matching didn't work)
        let position_matches: Vec<_> = unmatched_removed
            .par_iter()
            .filter_map(|&removed_idx| {
                // Look for an added row at the same position
                unmatched_added.iter().position(|&added_idx| added_idx == removed_idx).map(|added_pos| (removed_idx, unmatched_added[added_pos]))
            })
            .collect();
        
        // Remove position matches from unmatched lists
        for &(removed_idx, added_idx) in &position_matches {
            modifications.push((removed_idx, added_idx));
            unmatched_removed.retain(|&x| x != removed_idx);
            unmatched_added.retain(|&x| x != added_idx);
        }
        
        Ok((modifications, unmatched_added, unmatched_removed))
    }
    
    /// Find common columns between schemas for content matching
    fn find_common_columns(baseline_schema: &[ColumnInfo], current_schema: &[ColumnInfo]) -> Vec<String> {
        let current_names: std::collections::HashSet<_> = current_schema.iter().map(|c| &c.name).collect();
        baseline_schema
            .iter()
            .filter_map(|col| {
                if current_names.contains(&col.name) {
                    Some(col.name.clone())
                } else {
                    None
                }
            })
            .collect()
    }
    
    /// Find hash-based exact matches using parallel processing (fastest method)
    fn find_hash_matches_parallel(
        baseline_data: &[Vec<String>],
        current_data: &[Vec<String>],
        removed_indices: &[u64],
        added_indices: &[u64],
    ) -> Result<Vec<(u64, u64)>> {
        use rayon::prelude::*;
        use std::collections::HashMap;
        
        // Compute hashes for added rows only (we need to match against these)
        let hash_computer = crate::hash::HashComputer::new();
        let mut added_row_hashes = HashMap::new();
        
        for &added_idx in added_indices {
            if let Some(row) = current_data.get(added_idx as usize) {
                let hash = hash_computer.hash_values(row);
                added_row_hashes.insert(hash, added_idx);
            }
        }
        
        // Find hash matches in parallel
        let matches: Vec<_> = removed_indices
            .par_iter()
            .filter_map(|&removed_idx| {
                if let Some(removed_row) = baseline_data.get(removed_idx as usize) {
                    let removed_hash = hash_computer.hash_values(removed_row);
                    // Check if this hash exists in the added rows
                    if let Some(&added_idx) = added_row_hashes.get(&removed_hash) {
                        return Some((removed_idx, added_idx));
                    }
                }
                None
            })
            .collect();
        
        Ok(matches)
    }
    
    /// Find content-based matches using parallel processing
    fn find_content_matches_parallel(
        baseline_data: &[Vec<String>],
        current_data: &[Vec<String>],
        removed_indices: &[u64],
        added_indices: &[u64],
        common_columns: &[String],
    ) -> Result<Vec<(u64, u64)>> {
        use rayon::prelude::*;
        
        // Create column index mappings
        let baseline_col_map: std::collections::HashMap<String, usize> = common_columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();
        
        // Parallel content matching with similarity scoring
        let matches: Vec<_> = removed_indices
            .par_iter()
            .filter_map(|&removed_idx| {
                let removed_row = baseline_data.get(removed_idx as usize)?;
                
                // Find best match among added rows
                let best_match = added_indices
                    .iter()
                    .filter_map(|&added_idx| {
                        let added_row = current_data.get(added_idx as usize)?;
                        let similarity = Self::calculate_row_similarity(
                            removed_row,
                            added_row,
                            common_columns,
                            &baseline_col_map,
                        );
                        Some((added_idx, similarity))
                    })
                    .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                
                // Only consider it a match if similarity is above threshold
                if let Some((added_idx, similarity)) = best_match {
                    if similarity > 0.5 { // At least 50% of key columns match
                        Some((removed_idx, added_idx))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        
        Ok(matches)
    }
    
    /// Calculate similarity between two rows based on common columns
    fn calculate_row_similarity(
        row1: &[String],
        row2: &[String],
        common_columns: &[String],
        col_map: &std::collections::HashMap<String, usize>,
    ) -> f64 {
        let mut matches = 0;
        let mut total = 0;
        
        for col_name in common_columns {
            if let Some(&col_idx) = col_map.get(col_name) {
                if let (Some(val1), Some(val2)) = (row1.get(col_idx), row2.get(col_idx)) {
                    total += 1;
                    if val1 == val2 {
                        matches += 1;
                    }
                }
            }
        }
        
        if total > 0 {
            matches as f64 / total as f64
        } else {
            0.0
        }
    }
    
    /// Analyze modifications in parallel to detect cell-level changes
    fn analyze_modifications_parallel(
        baseline_schema: &[ColumnInfo],
        baseline_data: &[Vec<String>],
        current_schema: &[ColumnInfo],
        current_data: &[Vec<String>],
        modifications: &[(u64, u64)],
    ) -> Result<Vec<RowModification>> {
        use rayon::prelude::*;
        
        // Create column mappings for schema-aware comparison
        let baseline_col_map: std::collections::HashMap<String, usize> = baseline_schema
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
        
        let current_col_map: std::collections::HashMap<String, usize> = current_schema
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();
        
        // Parallel cell-level analysis
        let detailed_modifications: Vec<_> = modifications
            .par_iter()
            .filter_map(|&(baseline_idx, current_idx)| {
                let baseline_row = baseline_data.get(baseline_idx as usize)?;
                let current_row = current_data.get(current_idx as usize)?;
                
                let changes = Self::compare_rows_schema_aware(
                    baseline_row,
                    current_row,
                    &baseline_col_map,
                    &current_col_map,
                );
                
                if !changes.is_empty() {
                    Some(RowModification {
                        row_index: current_idx, // Use current position as the canonical index
                        changes,
                    })
                } else {
                    None
                }
            })
            .collect();
        
        Ok(detailed_modifications)
    }
    
    /// Compare two rows with schema awareness
    fn compare_rows_schema_aware(
        baseline_row: &[String],
        current_row: &[String],
        baseline_col_map: &std::collections::HashMap<String, usize>,
        current_col_map: &std::collections::HashMap<String, usize>,
    ) -> HashMap<String, CellChange> {
        let mut changes = HashMap::new();
        
        // Compare common columns only
        for col_name in baseline_col_map.keys() {
            if let (Some(&baseline_idx), Some(&current_idx)) = 
                (baseline_col_map.get(col_name), current_col_map.get(col_name)) {
                
                let baseline_value = baseline_row.get(baseline_idx).map(|s| s.as_str()).unwrap_or("");
                let current_value = current_row.get(current_idx).map(|s| s.as_str()).unwrap_or("");
                
                if baseline_value != current_value {
                    changes.insert(col_name.clone(), CellChange {
                        before: baseline_value.to_string(),
                        after: current_value.to_string(),
                    });
                }
            }
        }
        
        changes
    }
    
    /// Convert genuine additions to RowAddition format in parallel
    fn convert_additions_parallel(
        current_schema: &[ColumnInfo],
        current_data: &[Vec<String>],
        added_indices: &[u64],
    ) -> Result<Vec<RowAddition>> {
        use rayon::prelude::*;
        
        let additions: Vec<_> = added_indices
            .par_iter()
            .filter_map(|&row_idx| {
                let row_data = current_data.get(row_idx as usize)?;
                let mut data = HashMap::new();
                
                for (col_idx, col) in current_schema.iter().enumerate() {
                    if let Some(value) = row_data.get(col_idx) {
                        data.insert(col.name.clone(), value.clone());
                    }
                }
                
                Some(RowAddition { row_index: row_idx, data })
            })
            .collect();
        
        Ok(additions)
    }
    
    /// Convert genuine removals to RowRemoval format in parallel
    fn convert_removals_parallel(
        baseline_schema: &[ColumnInfo],
        baseline_data: &[Vec<String>],
        removed_indices: &[u64],
    ) -> Result<Vec<RowRemoval>> {
        use rayon::prelude::*;
        
        let removals: Vec<_> = removed_indices
            .par_iter()
            .filter_map(|&row_idx| {
                let row_data = baseline_data.get(row_idx as usize)?;
                let mut data = HashMap::new();
                
                for (col_idx, col) in baseline_schema.iter().enumerate() {
                    if let Some(value) = row_data.get(col_idx) {
                        data.insert(col.name.clone(), value.clone());
                    }
                }
                
                Some(RowRemoval { row_index: row_idx, data })
            })
            .collect();
        
        Ok(removals)
    }


}

impl SchemaChanges {
    /// Check if there are any schema changes
    pub fn has_changes(&self) -> bool {
        self.column_order.is_some()
            || !self.columns_added.is_empty()
            || !self.columns_removed.is_empty()
            || !self.columns_renamed.is_empty()
            || !self.type_changes.is_empty()
    }
}

impl RowChanges {
    /// Check if there are any row changes
    pub fn has_changes(&self) -> bool {
        !self.modified.is_empty() || !self.added.is_empty() || !self.removed.is_empty()
    }

    /// Get total number of changed rows
    pub fn total_changes(&self) -> usize {
        self.modified.len() + self.added.len() + self.removed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_change_detection() {
        let baseline = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
            },
        ];

        let current = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "VARCHAR".to_string(), // Type changed
                nullable: true,
            },
            ColumnInfo {
                name: "email".to_string(), // Added column
                data_type: "TEXT".to_string(),
                nullable: true,
            },
        ];

        let changes = ChangeDetector::detect_schema_changes(&baseline, &current).unwrap();

        assert!(changes.has_changes());
        assert_eq!(changes.columns_added.len(), 1);
        assert_eq!(changes.columns_added[0].name, "email");
        assert_eq!(changes.type_changes.len(), 1);
        assert_eq!(changes.type_changes[0].column, "name");
        assert_eq!(changes.type_changes[0].from, "TEXT");
        assert_eq!(changes.type_changes[0].to, "VARCHAR");
    }

    #[test]
    fn test_row_change_detection() {
        let schema = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
            },
        ];

        let baseline_data = vec![
            vec!["1".to_string(), "Alice".to_string()],
            vec!["2".to_string(), "Bob".to_string()],
        ];

        let current_data = vec![
            vec!["1".to_string(), "Alice Smith".to_string()], // Modified
            vec!["2".to_string(), "Bob".to_string()],         // Unchanged
            vec!["3".to_string(), "Charlie".to_string()],     // Added
        ];

        let changes = ChangeDetector::detect_row_changes(&schema, &baseline_data, &schema, &current_data).unwrap();

        assert!(changes.has_changes());
        assert_eq!(changes.modified.len(), 1);
        assert_eq!(changes.modified[0].row_index, 0);
        assert!(changes.modified[0].changes.contains_key("name"));
        assert_eq!(changes.added.len(), 1);
        assert_eq!(changes.added[0].row_index, 2);
        assert_eq!(changes.removed.len(), 0);
    }

    #[test]
    fn test_streaming_hash_set_operations() {
        // Test basic hash set operations
        let mut hash_set = RowHashSet::new();
        
        // Add some rows
        hash_set.add_row(0, "hash1".to_string());
        hash_set.add_row(1, "hash2".to_string());
        hash_set.add_row(2, "hash3".to_string());
        
        // Test contains operations
        assert!(hash_set.contains_hash("hash1"));
        assert!(hash_set.contains_hash("hash2"));
        assert!(!hash_set.contains_hash("nonexistent"));
        
        // Test row retrieval
        assert_eq!(hash_set.get_first_row_by_hash("hash1"), Some(0));
        assert_eq!(hash_set.get_first_row_by_hash("hash2"), Some(1));
        assert_eq!(hash_set.get_first_row_by_hash("nonexistent"), None);
        
        // Test size
        assert_eq!(hash_set.len(), 3);
    }

    #[test]
    fn test_streaming_changed_rows_identification() {
        // Create two hash sets representing different dataset states
        let mut baseline_hashes = RowHashSet::new();
        baseline_hashes.add_row(0, "unchanged_row".to_string());
        baseline_hashes.add_row(1, "modified_row_old".to_string());
        baseline_hashes.add_row(2, "removed_row".to_string());
        
        let mut current_hashes = RowHashSet::new();
        current_hashes.add_row(0, "unchanged_row".to_string());
        current_hashes.add_row(1, "modified_row_new".to_string());
        current_hashes.add_row(2, "added_row".to_string());
        
        // Identify changes
        let changed_rows = StreamingChangeDetector::identify_changed_rows(&baseline_hashes, &current_hashes);
        
        // Verify results
        assert_eq!(changed_rows.unchanged_count, 1); // "unchanged_row"
        assert_eq!(changed_rows.baseline_changed.len(), 2); // "modified_row_old", "removed_row"
        assert_eq!(changed_rows.current_changed.len(), 2);  // "modified_row_new", "added_row"
        
        // Verify specific row indices
        assert!(changed_rows.baseline_changed.contains(&1)); // modified row in baseline
        assert!(changed_rows.baseline_changed.contains(&2)); // removed row
        assert!(changed_rows.current_changed.contains(&1));  // modified row in current
        assert!(changed_rows.current_changed.contains(&2));  // added row
    }

    #[test]
    fn test_streaming_row_hash_computation() {
        let row1 = vec!["Alice".to_string(), "30".to_string(), "Engineer".to_string()];
        let row2 = vec!["Bob".to_string(), "25".to_string(), "Designer".to_string()];
        let row3 = vec!["Alice".to_string(), "30".to_string(), "Engineer".to_string()]; // Same as row1
        
        let hash1 = StreamingChangeDetector::compute_row_hash(&row1);
        let hash2 = StreamingChangeDetector::compute_row_hash(&row2);
        let hash3 = StreamingChangeDetector::compute_row_hash(&row3);
        
        // Same data should produce same hash
        assert_eq!(hash1, hash3);
        
        // Different data should produce different hash
        assert_ne!(hash1, hash2);
        
        // Hashes should be consistent (Blake3 hex strings)
        assert_eq!(hash1.len(), 64); // Blake3 produces 32-byte hashes = 64 hex chars
        assert!(hash1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_row_deletion_with_shift_detection() {
        // This test reproduces the exact issue found in CLI testing:
        // When a row is deleted from the middle, subsequent rows shift up.
        // The algorithm should correctly identify:
        // 1. One removed row (the deleted one)
        // 2. One modified row (if any data actually changed)
        // 3. NOT incorrectly report all shifted rows as modifications
        
        let schema = vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
            },
            ColumnInfo {
                name: "age".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: true,
            },
            ColumnInfo {
                name: "department".to_string(),
                data_type: "TEXT".to_string(),
                nullable: true,
            },
            ColumnInfo {
                name: "salary".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: true,
            },
        ];

        // Original data (15 rows)
        let baseline_data = vec![
            vec!["1".to_string(), "Alice Johnson".to_string(), "28".to_string(), "Engineering".to_string(), "75000".to_string()],
            vec!["2".to_string(), "Bob Smith".to_string(), "35".to_string(), "Marketing".to_string(), "62000".to_string()],
            vec!["3".to_string(), "Carol Davis".to_string(), "42".to_string(), "Engineering".to_string(), "88000".to_string()],
            vec!["4".to_string(), "David Wilson".to_string(), "29".to_string(), "Sales".to_string(), "58000".to_string()],
            vec!["5".to_string(), "Eve Brown".to_string(), "31".to_string(), "Engineering".to_string(), "72000".to_string()],
            vec!["6".to_string(), "Frank Miller".to_string(), "45".to_string(), "Marketing".to_string(), "65000".to_string()], // This row gets deleted
            vec!["7".to_string(), "Grace Lee".to_string(), "33".to_string(), "Sales".to_string(), "61000".to_string()],
            vec!["8".to_string(), "Henry Taylor".to_string(), "38".to_string(), "Engineering".to_string(), "82000".to_string()],
            vec!["9".to_string(), "Ivy Chen".to_string(), "26".to_string(), "Marketing".to_string(), "55000".to_string()],
            vec!["10".to_string(), "Jack Anderson".to_string(), "41".to_string(), "Sales".to_string(), "67000".to_string()],
            vec!["11".to_string(), "Kate Williams".to_string(), "30".to_string(), "Engineering".to_string(), "76000".to_string()],
            vec!["12".to_string(), "Leo Garcia".to_string(), "36".to_string(), "Marketing".to_string(), "63000".to_string()],
            vec!["13".to_string(), "Mia Rodriguez".to_string(), "27".to_string(), "Sales".to_string(), "59000".to_string()],
            vec!["14".to_string(), "Noah Martinez".to_string(), "44".to_string(), "Engineering".to_string(), "85000".to_string()],
            vec!["15".to_string(), "Olivia Thomas".to_string(), "32".to_string(), "Marketing".to_string(), "64000".to_string()], // This row gets modified (age 32->33)
        ];

        // Current data (14 rows) - Frank Miller removed, Olivia Thomas age changed
        let current_data = vec![
            vec!["1".to_string(), "Alice Johnson".to_string(), "28".to_string(), "Engineering".to_string(), "75000".to_string()],
            vec!["2".to_string(), "Bob Smith".to_string(), "35".to_string(), "Marketing".to_string(), "62000".to_string()],
            vec!["3".to_string(), "Carol Davis".to_string(), "42".to_string(), "Engineering".to_string(), "88000".to_string()],
            vec!["4".to_string(), "David Wilson".to_string(), "29".to_string(), "Sales".to_string(), "58000".to_string()],
            vec!["5".to_string(), "Eve Brown".to_string(), "31".to_string(), "Engineering".to_string(), "72000".to_string()],
            // Frank Miller (row 5 in 0-indexed) is missing
            vec!["7".to_string(), "Grace Lee".to_string(), "33".to_string(), "Sales".to_string(), "61000".to_string()],     // Now at index 5
            vec!["8".to_string(), "Henry Taylor".to_string(), "38".to_string(), "Engineering".to_string(), "82000".to_string()], // Now at index 6
            vec!["9".to_string(), "Ivy Chen".to_string(), "26".to_string(), "Marketing".to_string(), "55000".to_string()],   // Now at index 7
            vec!["10".to_string(), "Jack Anderson".to_string(), "41".to_string(), "Sales".to_string(), "67000".to_string()], // Now at index 8
            vec!["11".to_string(), "Kate Williams".to_string(), "30".to_string(), "Engineering".to_string(), "76000".to_string()], // Now at index 9
            vec!["12".to_string(), "Leo Garcia".to_string(), "36".to_string(), "Marketing".to_string(), "63000".to_string()], // Now at index 10
            vec!["13".to_string(), "Mia Rodriguez".to_string(), "27".to_string(), "Sales".to_string(), "59000".to_string()], // Now at index 11
            vec!["14".to_string(), "Noah Martinez".to_string(), "44".to_string(), "Engineering".to_string(), "85000".to_string()], // Now at index 12
            vec!["15".to_string(), "Olivia Thomas".to_string(), "33".to_string(), "Marketing".to_string(), "64000".to_string()], // Now at index 13, age changed 32->33
        ];

        let changes = ChangeDetector::detect_row_changes(&schema, &baseline_data, &schema, &current_data).unwrap();

        // Verify the algorithm correctly identifies the changes
        assert!(changes.has_changes(), "Should detect changes");
        
        // Should detect exactly 1 removed row (Frank Miller)
        assert_eq!(changes.removed.len(), 1, "Should detect exactly 1 removed row");
        assert_eq!(changes.removed[0].row_index, 5, "Should identify Frank Miller (index 5) as removed");
        assert_eq!(changes.removed[0].data.get("name").unwrap(), "Frank Miller");
        
        // Should detect exactly 1 modified row (Olivia Thomas age change)
        assert_eq!(changes.modified.len(), 1, "Should detect exactly 1 modified row, not 9 due to position shifts");
        assert_eq!(changes.modified[0].row_index, 13, "Olivia Thomas should be at new index 13");
        assert_eq!(changes.modified[0].changes.len(), 1, "Should have exactly 1 field change");
        assert!(changes.modified[0].changes.contains_key("age"), "Should detect age change");
        assert_eq!(changes.modified[0].changes.get("age").unwrap().before, "32");
        assert_eq!(changes.modified[0].changes.get("age").unwrap().after, "33");
        
        // Should detect no additions
        assert_eq!(changes.added.len(), 0, "Should detect no added rows");
        
        // This test ensures that hash-based matching correctly identifies that rows 6-14 
        // in the original file are the same content as rows 5-13 in the current file
        // (they just shifted up due to the deletion), rather than treating them as 
        // completely different rows requiring expensive content comparison.
    }
}
