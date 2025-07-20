//! Comprehensive change detection and rollback system for snapbase

use crate::error::Result;
use crate::hash::ColumnInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Type alias for complex return types
type ChangeResult = (Vec<(u64, u64)>, Vec<u64>, Vec<u64>);

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
