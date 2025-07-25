use anyhow::Result;
use chrono::Local;
use rand::Rng;
use std::collections::HashMap;
use std::path::Path;

pub struct SnapshotNamer {
    pattern: String,
}

impl SnapshotNamer {
    pub fn new(pattern: String) -> Self {
        Self { pattern }
    }

    pub fn generate_name(&self, source_path: &str, existing_names: &[String]) -> Result<String> {
        let variables = self.build_variables(source_path, existing_names)?;
        let mut result = self.pattern.clone();

        // Replace variables in the pattern
        for (key, value) in variables {
            result = result.replace(&format!("{{{key}}}"), &value);
        }

        Ok(result)
    }

    fn build_variables(
        &self,
        source_path: &str,
        existing_names: &[String],
    ) -> Result<HashMap<String, String>> {
        let mut variables = HashMap::new();

        // Source file information
        let path = Path::new(source_path);
        let source_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        let source_ext = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();

        variables.insert("source".to_string(), source_name);
        variables.insert("source_ext".to_string(), source_ext.clone());

        // Format detection
        let format = self.detect_format(&source_ext);
        variables.insert("format".to_string(), format);

        // Timestamp information
        let now = Local::now();
        variables.insert(
            "timestamp".to_string(),
            now.format("%Y%m%d_%H%M%S").to_string(),
        );
        variables.insert("date".to_string(), now.format("%Y%m%d").to_string());
        variables.insert("time".to_string(), now.format("%H%M%S").to_string());
        variables.insert("iso_date".to_string(), now.format("%Y-%m-%d").to_string());
        variables.insert("iso_time".to_string(), now.format("%H:%M:%S").to_string());

        // Sequential numbering
        let seq = self.calculate_next_sequence(existing_names)?;
        variables.insert("seq".to_string(), seq.to_string());

        // Random hash
        let hash = self.generate_hash();
        variables.insert("hash".to_string(), hash);

        // User information
        let user = whoami::username();
        variables.insert("user".to_string(), user);

        Ok(variables)
    }

    fn detect_format(&self, extension: &str) -> String {
        match extension.to_lowercase().as_str() {
            "csv" => "csv",
            "json" => "json",
            "jsonl" | "ndjson" => "jsonl",
            "parquet" => "parquet",
            "sql" => "sql",
            "tsv" => "tsv",
            "txt" => "txt",
            "xlsx" | "xls" => "excel",
            _ => "data",
        }
        .to_string()
    }

    fn calculate_next_sequence(&self, existing_names: &[String]) -> Result<i32> {
        // Extract numeric sequences from existing names that match the pattern
        let mut max_seq = 0;

        for name in existing_names {
            // Simple heuristic: find the last number in the name
            let numbers: Vec<i32> = name
                .chars()
                .collect::<Vec<_>>()
                .split(|c| !c.is_ascii_digit())
                .filter_map(|s| {
                    let num_str: String = s.iter().collect();
                    num_str.parse::<i32>().ok()
                })
                .collect();

            if let Some(&last_num) = numbers.last() {
                max_seq = max_seq.max(last_num);
            }
        }

        Ok(max_seq + 1)
    }

    fn generate_hash(&self) -> String {
        let mut rng = rand::thread_rng();
        let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyz0123456789".chars().collect();
        (0..7)
            .map(|_| chars[rng.gen_range(0..chars.len())])
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_pattern_substitution() {
        let namer = SnapshotNamer::new("{source}_{format}_{seq}".to_string());
        let result = namer.generate_name("sales.csv", &[]).unwrap();
        assert!(result.starts_with("sales_csv_"));
        assert!(result.ends_with("_1"));
    }

    #[test]
    fn test_sequential_numbering() {
        let namer = SnapshotNamer::new("{source}_{seq}".to_string());
        let existing = vec!["sales_1".to_string(), "sales_2".to_string()];
        let result = namer.generate_name("sales.csv", &existing).unwrap();
        assert_eq!(result, "sales_3");
    }

    #[test]
    fn test_format_detection() {
        let namer = SnapshotNamer::new("{format}".to_string());

        assert_eq!(namer.generate_name("test.csv", &[]).unwrap(), "csv");
        assert_eq!(namer.generate_name("test.json", &[]).unwrap(), "json");
        assert_eq!(namer.generate_name("test.parquet", &[]).unwrap(), "parquet");
        assert_eq!(namer.generate_name("test.sql", &[]).unwrap(), "sql");
        assert_eq!(namer.generate_name("test.unknown", &[]).unwrap(), "data");
    }

    #[test]
    fn test_timestamp_pattern() {
        let namer = SnapshotNamer::new("{source}_{timestamp}".to_string());
        let result = namer.generate_name("test.csv", &[]).unwrap();
        assert!(result.starts_with("test_"));
        assert!(result.len() > 10); // Should contain timestamp
    }
}
