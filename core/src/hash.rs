//! Hashing utilities for snapbase operations

use crate::error::Result;
use blake3::Hasher;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// A hash value represented as a hex string
pub type HashValue = String;

/// Row hash with index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowHash {
    pub row_index: u64,
    pub hash: HashValue,
}

/// Column hash information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnHash {
    pub column_name: String,
    pub column_type: String,
    pub hash: HashValue,
}

/// Schema hash information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaHash {
    pub hash: HashValue,
    pub column_count: usize,
    pub columns: Vec<ColumnInfo>,
}

/// Column information for schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Hash computer for various data structures
pub struct HashComputer;

impl Default for HashComputer {
    fn default() -> Self {
        Self::new()
    }
}

impl HashComputer {
    pub fn new() -> Self {
        Self
    }

    /// Compute hash for a single value
    pub fn hash_value(&self, value: &str) -> HashValue {
        let mut hasher = Hasher::new();
        hasher.update(value.as_bytes());
        hasher.finalize().to_hex().to_string()
    }

    /// Compute hash for multiple values (e.g., a row)
    pub fn hash_values(&self, values: &[String]) -> HashValue {
        let mut hasher = Hasher::new();
        for value in values {
            hasher.update(value.as_bytes());
            hasher.update(b"|"); // Separator to avoid hash collisions
        }
        hasher.finalize().to_hex().to_string()
    }

    /// Compute schema hash from column information
    pub fn hash_schema(&self, columns: &[ColumnInfo]) -> Result<SchemaHash> {
        let mut hasher = Hasher::new();

        // Create a sorted copy for deterministic hashing, but preserve original order in result
        let mut sorted_columns = columns.to_vec();
        sorted_columns.sort_by(|a, b| a.name.cmp(&b.name));

        for col in &sorted_columns {
            hasher.update(col.name.as_bytes());
            hasher.update(b"|");
            hasher.update(col.data_type.as_bytes());
            hasher.update(b"|");
            hasher.update(if col.nullable { b"1" } else { b"0" });
            hasher.update(b"||");
        }

        let hash = hasher.finalize().to_hex().to_string();

        Ok(SchemaHash {
            hash,
            column_count: columns.len(),
            columns: columns.to_vec(), // Preserve original order
        })
    }

    /// Compute column hashes from data
    pub fn hash_columns(
        &self,
        column_data: &HashMap<String, Vec<String>>,
    ) -> Result<Vec<ColumnHash>> {
        let mut column_hashes = Vec::new();

        for (column_name, values) in column_data {
            let mut hasher = Hasher::new();

            // Hash all values in the column
            for value in values {
                hasher.update(value.as_bytes());
                hasher.update(b"|");
            }

            let hash = hasher.finalize().to_hex().to_string();

            // Infer column type from first non-empty value
            let column_type = self.infer_column_type(values);

            column_hashes.push(ColumnHash {
                column_name: column_name.clone(),
                column_type,
                hash,
            });
        }

        // Preserve original column order - don't sort alphabetically
        Ok(column_hashes)
    }

    /// Compute row hashes from data
    pub fn hash_rows(&self, row_data: &[Vec<String>]) -> Result<Vec<RowHash>> {
        let total_rows = row_data.len();

        if total_rows == 0 {
            return Ok(Vec::new());
        }

        // Compute hashes in parallel for all rows
        let row_hashes: Vec<RowHash> = (0..total_rows)
            .into_par_iter()
            .map(|idx| {
                let row = &row_data[idx];
                let hash = self.hash_values(row);
                RowHash {
                    row_index: idx as u64,
                    hash,
                }
            })
            .collect();

        Ok(row_hashes)
    }

    /// Compute row hashes using DuckDB-native operations (high performance)
    pub fn hash_rows_with_processor(
        &self,
        data_processor: &mut crate::data::DataProcessor,
    ) -> Result<Vec<RowHash>> {
        // Use the optimized DuckDB-native hash computation
        data_processor.compute_row_hashes_sql()
    }

    /// Compute row hashes with progress reporting
    pub fn hash_rows_with_processor_and_progress(
        &self,
        data_processor: &mut crate::data::DataProcessor,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<RowHash>> {
        // Use the optimized DuckDB-native hash computation with progress
        data_processor.compute_row_hashes_with_progress(progress_callback)
    }

    /// Compute column hashes using DuckDB-native operations (high performance)
    pub fn hash_columns_with_processor(
        &self,
        data_processor: &mut crate::data::DataProcessor,
    ) -> Result<Vec<ColumnHash>> {
        // Use the optimized DuckDB-native column hash computation
        data_processor.compute_column_hashes_sql()
    }

    /// Infer column type from values
    fn infer_column_type(&self, values: &[String]) -> String {
        for value in values {
            if !value.is_empty() {
                // Simple type inference
                if value.parse::<i64>().is_ok() {
                    return "INTEGER".to_string();
                } else if value.parse::<f64>().is_ok() {
                    return "FLOAT".to_string();
                } else if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false")
                {
                    return "BOOLEAN".to_string();
                } else {
                    return "TEXT".to_string();
                }
            }
        }
        "TEXT".to_string() // Default
    }

    /// Compare two sets of row hashes using content-based comparison with quality metrics
    pub fn compare_row_hashes(
        &self,
        base_hashes: &[RowHash],
        compare_hashes: &[RowHash],
    ) -> RowHashComparison {
        // Create sets of content hashes for efficient lookup
        let base_content_set: HashSet<&str> =
            base_hashes.iter().map(|rh| rh.hash.as_str()).collect();

        let compare_content_set: HashSet<&str> =
            compare_hashes.iter().map(|rh| rh.hash.as_str()).collect();

        // Compute hash quality metrics
        let total_base_hashes = base_hashes.len() as u64;
        let unique_base_hashes = base_content_set.len() as u64;
        let total_compare_hashes = compare_hashes.len() as u64;
        let unique_compare_hashes = compare_content_set.len() as u64;

        let base_collision_count = total_base_hashes.saturating_sub(unique_base_hashes);
        let compare_collision_count = total_compare_hashes.saturating_sub(unique_compare_hashes);

        let base_collision_rate = if total_base_hashes > 0 {
            base_collision_count as f64 / total_base_hashes as f64
        } else {
            0.0
        };

        let compare_collision_rate = if total_compare_hashes > 0 {
            compare_collision_count as f64 / total_compare_hashes as f64
        } else {
            0.0
        };

        let hash_quality = HashQualityMetrics {
            total_base_hashes,
            unique_base_hashes,
            total_compare_hashes,
            unique_compare_hashes,
            base_collision_count,
            compare_collision_count,
            base_collision_rate,
            compare_collision_rate,
        };

        // Print diagnostics for debugging
        // hash_quality.print_diagnostics();

        // Create maps from content hash to row indices for tracking which rows changed
        let mut base_content_to_indices: HashMap<&str, Vec<u64>> = HashMap::new();
        for rh in base_hashes {
            base_content_to_indices
                .entry(rh.hash.as_str())
                .or_default()
                .push(rh.row_index);
        }

        let mut compare_content_to_indices: HashMap<&str, Vec<u64>> = HashMap::new();
        for rh in compare_hashes {
            compare_content_to_indices
                .entry(rh.hash.as_str())
                .or_default()
                .push(rh.row_index);
        }

        let mut changed_rows = Vec::new();
        let mut added_rows = Vec::new();
        let mut removed_rows = Vec::new();

        // Find removed content (exists in base but not in compare)
        for content_hash in &base_content_set {
            if !compare_content_set.contains(content_hash) {
                // This content was removed - add all row indices that had this content
                if let Some(indices) = base_content_to_indices.get(content_hash) {
                    removed_rows.extend(indices);
                }
            }
        }

        // Find added content (exists in compare but not in base)
        for content_hash in &compare_content_set {
            if !base_content_set.contains(content_hash) {
                // This content was added - add all row indices that have this content
                if let Some(indices) = compare_content_to_indices.get(content_hash) {
                    added_rows.extend(indices);
                }
            }
        }

        // For content that exists in both, check if the count changed (indicating duplicates added/removed)
        for content_hash in base_content_set.intersection(&compare_content_set) {
            let base_count = base_content_to_indices
                .get(content_hash)
                .map(|v| v.len())
                .unwrap_or(0);
            let compare_count = compare_content_to_indices
                .get(content_hash)
                .map(|v| v.len())
                .unwrap_or(0);

            if base_count != compare_count {
                // Same content but different number of occurrences
                // This could indicate duplicate rows were added or removed
                if compare_count > base_count {
                    // More instances in compare - some were added
                    if let Some(indices) = compare_content_to_indices.get(content_hash) {
                        // Add the "extra" indices as added rows
                        for &idx in indices.iter().skip(base_count) {
                            added_rows.push(idx);
                        }
                    }
                } else {
                    // Fewer instances in compare - some were removed
                    if let Some(indices) = base_content_to_indices.get(content_hash) {
                        // Add the "missing" indices as removed rows
                        for &idx in indices.iter().skip(compare_count) {
                            removed_rows.push(idx);
                        }
                    }
                }
            }
        }

        // Sort the results for consistent output
        changed_rows.sort_unstable();
        added_rows.sort_unstable();
        removed_rows.sort_unstable();

        RowHashComparison {
            changed_rows,
            added_rows,
            removed_rows,
            total_base: base_hashes.len(),
            total_compare: compare_hashes.len(),
            hash_quality,
        }
    }
}

/// Result of comparing row hashes
#[derive(Debug, Clone)]
pub struct RowHashComparison {
    pub changed_rows: Vec<u64>,
    pub added_rows: Vec<u64>,
    pub removed_rows: Vec<u64>,
    pub total_base: usize,
    pub total_compare: usize,
    pub hash_quality: HashQualityMetrics,
}

impl RowHashComparison {
    pub fn has_changes(&self) -> bool {
        !self.changed_rows.is_empty()
            || !self.added_rows.is_empty()
            || !self.removed_rows.is_empty()
    }

    pub fn total_changes(&self) -> usize {
        self.changed_rows.len() + self.added_rows.len() + self.removed_rows.len()
    }
}

/// Hash quality metrics for debugging
#[derive(Debug, Clone)]
pub struct HashQualityMetrics {
    pub total_base_hashes: u64,
    pub unique_base_hashes: u64,
    pub total_compare_hashes: u64,
    pub unique_compare_hashes: u64,
    pub base_collision_count: u64,
    pub compare_collision_count: u64,
    pub base_collision_rate: f64,
    pub compare_collision_rate: f64,
}

impl Default for HashQualityMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl HashQualityMetrics {
    pub fn new() -> Self {
        Self {
            total_base_hashes: 0,
            unique_base_hashes: 0,
            total_compare_hashes: 0,
            unique_compare_hashes: 0,
            base_collision_count: 0,
            compare_collision_count: 0,
            base_collision_rate: 0.0,
            compare_collision_rate: 0.0,
        }
    }

    pub fn has_significant_collisions(&self) -> bool {
        self.base_collision_rate > 0.01 || self.compare_collision_rate > 0.01 // More than 1% collision rate
    }

    pub fn print_diagnostics(&self) {
        eprintln!("=== Hash Quality Diagnostics ===");
        eprintln!("Base dataset:");
        eprintln!("  Total hashes: {}", self.total_base_hashes);
        eprintln!("  Unique hashes: {}", self.unique_base_hashes);
        eprintln!(
            "  Collisions: {} ({:.4}%)",
            self.base_collision_count,
            self.base_collision_rate * 100.0
        );
        eprintln!("Compare dataset:");
        eprintln!("  Total hashes: {}", self.total_compare_hashes);
        eprintln!("  Unique hashes: {}", self.unique_compare_hashes);
        eprintln!(
            "  Collisions: {} ({:.4}%)",
            self.compare_collision_count,
            self.compare_collision_rate * 100.0
        );

        if self.has_significant_collisions() {
            eprintln!(
                "⚠️  WARNING: High collision rate detected! This may cause false change detection."
            );
        } else {
            eprintln!("✅ Hash quality looks good - low collision rate.");
        }
        eprintln!("===============================");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_value() {
        let computer = HashComputer::new();
        let hash1 = computer.hash_value("test");
        let hash2 = computer.hash_value("test");
        let hash3 = computer.hash_value("different");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_hash_values() {
        let computer = HashComputer::new();
        let values1 = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let values2 = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let values3 = vec!["a".to_string(), "c".to_string(), "b".to_string()];

        let hash1 = computer.hash_values(&values1);
        let hash2 = computer.hash_values(&values2);
        let hash3 = computer.hash_values(&values3);

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3); // Order matters
    }

    #[test]
    fn test_hash_rows() {
        let computer = HashComputer::new();
        let row_data = vec![
            vec!["1".to_string(), "a".to_string()],
            vec!["2".to_string(), "b".to_string()],
            vec!["3".to_string(), "c".to_string()],
            vec!["4".to_string(), "d".to_string()],
        ];

        let hashes = computer.hash_rows(&row_data).unwrap();
        assert_eq!(hashes.len(), 4);

        // Check that each row has the correct index
        for (i, row_hash) in hashes.iter().enumerate() {
            assert_eq!(row_hash.row_index, i as u64);
        }
    }

    #[test]
    fn test_content_based_row_comparison() {
        let computer = HashComputer::new();

        // Create baseline data: rows A, B, C, D, E
        let baseline_data = vec![
            vec!["A".to_string(), "1".to_string()], // Row 0
            vec!["B".to_string(), "2".to_string()], // Row 1
            vec!["C".to_string(), "3".to_string()], // Row 2
            vec!["D".to_string(), "4".to_string()], // Row 3
            vec!["E".to_string(), "5".to_string()], // Row 4
        ];

        // Create current data: rows A, C, D, E (removed B from middle)
        let current_data = vec![
            vec!["A".to_string(), "1".to_string()], // Row 0 (same content as baseline row 0)
            vec!["C".to_string(), "3".to_string()], // Row 1 (same content as baseline row 2)
            vec!["D".to_string(), "4".to_string()], // Row 2 (same content as baseline row 3)
            vec!["E".to_string(), "5".to_string()], // Row 3 (same content as baseline row 4)
        ];

        let baseline_hashes = computer.hash_rows(&baseline_data).unwrap();
        let current_hashes = computer.hash_rows(&current_data).unwrap();

        let comparison = computer.compare_row_hashes(&baseline_hashes, &current_hashes);

        // Should detect that row with content "B,2" was removed
        assert_eq!(comparison.removed_rows.len(), 1);
        assert_eq!(comparison.added_rows.len(), 0);
        assert_eq!(comparison.changed_rows.len(), 0);
        assert_eq!(comparison.total_base, 5);
        assert_eq!(comparison.total_compare, 4);
        assert_eq!(comparison.total_changes(), 1);

        // The removed row should be the one that contained "B,2" (originally at index 1)
        assert!(comparison.removed_rows.contains(&1));

        // Check hash quality metrics
        assert_eq!(comparison.hash_quality.total_base_hashes, 5);
        assert_eq!(comparison.hash_quality.total_compare_hashes, 4);
        assert!(!comparison.hash_quality.has_significant_collisions());
    }
}
