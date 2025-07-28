use super::StorageBackend;
use crate::snapshot::SnapshotMetadata;
use anyhow::Result;
use async_trait::async_trait;
use indicatif::ProgressBar;
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct LocalStorage {
    base_path: PathBuf,
}

impl LocalStorage {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }
}

#[async_trait]
impl StorageBackend for LocalStorage {
    fn get_base_path(&self) -> String {
        self.base_path.to_string_lossy().to_string()
    }

    async fn ensure_directory(&self, path: &str) -> Result<()> {
        let full_path = self.base_path.join(path);
        std::fs::create_dir_all(&full_path)?;
        Ok(())
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
        self.write_file_with_progress(path, data, None).await
    }

    async fn write_file_with_progress(
        &self,
        path: &str,
        data: &[u8],
        progress_bar: Option<&ProgressBar>,
    ) -> Result<()> {
        let full_path = self.base_path.join(path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // For local storage, we can simulate chunked writing for progress
        let mut file = std::fs::File::create(&full_path)?;
        let total_size = data.len() as u64;
        let chunk_size = 64 * 1024; // 64KB chunks

        if let Some(pb) = progress_bar {
            pb.set_length(total_size);
            pb.set_message(format!("Writing {path}"));
        }

        let mut written = 0;
        for chunk in data.chunks(chunk_size) {
            file.write_all(chunk)?;
            written += chunk.len() as u64;

            if let Some(pb) = progress_bar {
                pb.set_position(written);

                // Small delay to make progress visible for small files
                if total_size > 1024 * 1024 {
                    // Only delay for files > 1MB
                    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                }
            }
        }

        file.flush()?;

        if let Some(pb) = progress_bar {
            pb.finish_with_message(format!("✅ Local file written: {path}"));
        }

        Ok(())
    }

    async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let full_path = self.base_path.join(path);
        let data = std::fs::read(full_path)?;
        Ok(data)
    }

    async fn list_directories(&self, path: &str) -> Result<Vec<String>> {
        let full_path = self.base_path.join(path);
        let mut directories = Vec::new();

        if full_path.exists() {
            for entry in std::fs::read_dir(full_path)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    if let Some(name) = entry.file_name().to_str() {
                        directories.push(name.to_string());
                    }
                }
            }
        }

        Ok(directories)
    }

    async fn delete_file(&self, path: &str) -> Result<()> {
        let full_path = self.base_path.join(path);
        std::fs::remove_file(full_path)?;
        Ok(())
    }

    fn supports_duckdb_direct_access(&self) -> bool {
        true // Local files can be accessed directly by DuckDB
    }

    fn get_duckdb_path(&self, path: &str) -> String {
        self.base_path.join(path).to_string_lossy().to_string()
    }

    async fn list_snapshots(&self, source: &str) -> Result<Vec<SnapshotMetadata>> {
        let source_path = Path::new("sources").join(source);
        let snapshot_names = self
            .list_directories(&source_path.to_string_lossy())
            .await?;
        let mut snapshots = Vec::new();

        for snapshot_name in snapshot_names {
            if let Some(_name) = snapshot_name.strip_prefix("snapshot_name=") {
                let snapshot_dir = source_path.join(&snapshot_name);
                let timestamps = self
                    .list_directories(&snapshot_dir.to_string_lossy())
                    .await?;

                for timestamp in timestamps {
                    if let Some(_ts) = timestamp.strip_prefix("snapshot_timestamp=") {
                        let metadata_path = snapshot_dir.join(&timestamp).join("metadata.json");
                        if let Ok(metadata_data) =
                            self.read_file(&metadata_path.to_string_lossy()).await
                        {
                            if let Ok(metadata) =
                                serde_json::from_slice::<SnapshotMetadata>(&metadata_data)
                            {
                                snapshots.push(metadata);
                            }
                        }
                    }
                }
            }
        }

        Ok(snapshots)
    }

    async fn list_all_snapshots(&self) -> Result<Vec<SnapshotMetadata>> {
        // Use the same traversal logic as list_snapshots_for_all_sources
        let mut sources_with_snapshots = std::collections::HashMap::new();
        self.traverse_sources_directory("sources", &mut sources_with_snapshots)
            .await?;

        let mut all_snapshots = Vec::new();

        // For each source that has snapshots, load the metadata
        for (source_path, _snapshot_names) in sources_with_snapshots {
            let snapshots = self.list_snapshots(&source_path).await?;
            all_snapshots.extend(snapshots);
        }

        all_snapshots.sort_by(|a, b| b.created.cmp(&a.created)); // Sort by creation time, newest first
        Ok(all_snapshots)
    }

    async fn list_snapshots_for_all_sources(
        &self,
    ) -> Result<std::collections::HashMap<String, Vec<String>>> {
        let mut result = std::collections::HashMap::new();

        // Recursively traverse sources directory to find all snapshot directories
        self.traverse_sources_directory("sources", &mut result)
            .await?;

        Ok(result)
    }

    async fn file_exists(&self, path: &str) -> Result<bool> {
        let full_path = self.base_path.join(path);
        Ok(full_path.exists())
    }
}

impl LocalStorage {
    // Helper method to traverse sources directory using breadth-first search
    async fn traverse_sources_directory(
        &self,
        start_path: &str,
        result: &mut std::collections::HashMap<String, Vec<String>>,
    ) -> Result<()> {
        let mut dirs_to_check = vec![start_path.to_string()];

        while let Some(path) = dirs_to_check.pop() {
            let dirs = self.list_directories(&path).await?;

            for dir in dirs {
                let dir_path = Path::new(&path).join(&dir);

                // Check if this directory contains snapshot_name= subdirectories
                let subdirs = self.list_directories(&dir_path.to_string_lossy()).await?;
                let mut snapshot_names = Vec::new();
                let mut has_snapshots = false;

                for subdir in &subdirs {
                    if let Some(name) = subdir.strip_prefix("snapshot_name=") {
                        snapshot_names.push(name.to_string());
                        has_snapshots = true;
                    }
                }

                if has_snapshots {
                    // This is a source file directory - extract the source path
                    let dir_path_str = dir_path.to_string_lossy();
                    let source_path = dir_path_str
                        .strip_prefix("sources/")
                        .unwrap_or(&dir_path_str);
                    snapshot_names.sort();
                    result.insert(source_path.to_string(), snapshot_names);
                } else {
                    // This might be a nested directory - add to check list
                    dirs_to_check.push(dir_path.to_string_lossy().to_string());
                }
            }
        }

        Ok(())
    }
}
