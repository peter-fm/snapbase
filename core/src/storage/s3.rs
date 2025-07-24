use super::StorageBackend;
use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use crate::snapshot::SnapshotMetadata;
use indicatif::ProgressBar;

pub struct S3Storage {
    client: Client,
    bucket: String,
    prefix: String,
    _region: String,
    use_express: bool,
    availability_zone: Option<String>,
}

impl S3Storage {
    pub async fn new(bucket: String, prefix: String, region: String, use_express: bool, availability_zone: Option<String>) -> Result<Self> {
        log::info!("🔧 Initializing S3 storage - bucket: {bucket}, prefix: {prefix}, region: {region}, express: {use_express}");
        
        let mut config_builder = aws_config::load_defaults(aws_config::BehaviorVersion::latest())
            .await
            .to_builder()
            .region(aws_config::Region::new(region.clone()));
            
        // For S3 Express, configure the endpoint
        if use_express {
            if let Some(ref az) = availability_zone {
                let endpoint = format!("https://s3express-{}.{}.amazonaws.com", az, region);
                log::info!("🚀 Using S3 Express endpoint: {endpoint}");
                config_builder = config_builder.endpoint_url(endpoint);
            } else {
                return Err(anyhow::anyhow!("Availability zone is required when using S3 Express"));
            }
        }
        
        let config = config_builder.build();
        let client = Client::new(&config);
        
        // Test S3 connection with a simple operation
        log::debug!("🔍 Testing S3 connection...");
        let actual_bucket = if use_express {
            if let Some(ref az) = availability_zone {
                format!("{}--{}--x-s3", bucket, az)
            } else {
                return Err(anyhow::anyhow!("Availability zone is required when using S3 Express"));
            }
        } else {
            bucket.clone()
        };
        
        match client.head_bucket().bucket(&actual_bucket).send().await {
            Ok(_) => {
                log::info!("✅ S3 connection successful");
            }
            Err(e) => {
                log::error!("❌ S3 connection failed: {e}");
                
                // Provide helpful debugging information
                let mut error_details = Vec::new();
                error_details.push(format!("Original error: {e}"));
                
                // Check for common issues
                if e.to_string().contains("dispatch failure") {
                    error_details.push("This is likely a network connectivity or credential issue:".to_string());
                    error_details.push("1. Check your internet connection".to_string());
                    error_details.push("2. Verify AWS credentials are configured:".to_string());
                    error_details.push("   - AWS_ACCESS_KEY_ID environment variable".to_string());
                    error_details.push("   - AWS_SECRET_ACCESS_KEY environment variable".to_string());
                    error_details.push("   - Or ~/.aws/credentials file".to_string());
                    error_details.push("3. Verify bucket name and region are correct".to_string());
                    error_details.push("4. Check if you have access to the S3 bucket".to_string());
                }
                
                return Err(anyhow::anyhow!("Failed to connect to S3 bucket '{}':\n{}", actual_bucket, error_details.join("\n")));
            }
        }
        
        Ok(Self {
            client,
            bucket,
            prefix,
            _region: region,
            use_express,
            availability_zone,
        })
    }
    
    fn get_bucket_name(&self) -> String {
        if self.use_express {
            if let Some(ref az) = self.availability_zone {
                format!("{}--{}--x-s3", self.bucket, az)
            } else {
                // This should not happen as we validate in constructor
                self.bucket.clone()
            }
        } else {
            self.bucket.clone()
        }
    }
    
    fn get_key(&self, path: &str) -> String {
        // Always normalize paths for S3 - convert Windows backslashes to forward slashes
        let normalized_path = path.replace("\\", "/");
        if self.prefix.is_empty() {
            normalized_path
        } else {
            format!("{}/{}", self.prefix, normalized_path)
        }
    }
}

#[async_trait]
impl StorageBackend for S3Storage {
    fn get_base_path(&self) -> String {
        format!("s3://{}/{}", self.get_bucket_name(), self.prefix)
    }
    
    async fn ensure_directory(&self, _path: &str) -> Result<()> {
        // S3 doesn't require directory creation
        Ok(())
    }
    
    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
        self.write_file_with_progress(path, data, None).await
    }
    
    async fn write_file_with_progress(&self, path: &str, data: &[u8], progress_bar: Option<&ProgressBar>) -> Result<()> {
        let key = self.get_key(path);
        let total_size = data.len() as u64;
        
        if let Some(pb) = progress_bar {
            pb.set_length(total_size);
            pb.set_message(format!("Uploading to S3: {path}"));
        }
        
        // For smaller files, use regular upload with simulated progress
        if total_size < 5 * 1024 * 1024 { // < 5MB
            // Simulate upload progress for small files
            if let Some(pb) = progress_bar {
                let chunks = (total_size / 1024).max(1); // 1KB chunks minimum
                let chunk_size = total_size / chunks;
                
                for i in 0..chunks {
                    let progress = ((i + 1) * chunk_size).min(total_size);
                    pb.set_position(progress);
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                }
            }
            
            let bucket_name = self.get_bucket_name();
            match self.client
                .put_object()
                .bucket(&bucket_name)
                .key(&key)
                .body(ByteStream::from(data.to_vec()))
                .send()
                .await {
                Ok(_) => {
                    if let Some(pb) = progress_bar {
                        pb.finish_with_message(format!("✅ S3 upload complete: s3://{}/{}", bucket_name, key));
                    }
                    log::info!("✅ Successfully wrote file to S3: s3://{}/{}", bucket_name, key);
                    Ok(())
                }
                Err(e) => {
                    if let Some(pb) = progress_bar {
                        pb.finish_with_message(format!("❌ S3 upload failed: {e}"));
                    }
                    log::error!("❌ Failed to write file to S3: s3://{}/{}: {}", bucket_name, key, e);
                    Err(e.into())
                }
            }
        } else {
            // For larger files, we could implement multipart upload with real progress
            // For now, we'll use the simple approach but show progress simulation
            if let Some(pb) = progress_bar {
                pb.set_message(format!("Uploading large file to S3: {path}"));
                
                // Simulate realistic upload progress
                let chunk_count = 20;
                let chunk_size = total_size / chunk_count;
                
                for i in 0..chunk_count {
                    let progress = ((i + 1) * chunk_size).min(total_size);
                    pb.set_position(progress);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
            
            let bucket_name = self.get_bucket_name();
            match self.client
                .put_object()
                .bucket(&bucket_name)
                .key(&key)
                .body(ByteStream::from(data.to_vec()))
                .send()
                .await {
                Ok(_) => {
                    if let Some(pb) = progress_bar {
                        pb.finish_with_message(format!("✅ S3 upload complete: s3://{}/{}", bucket_name, key));
                    }
                    log::info!("✅ Successfully wrote file to S3: s3://{}/{}", bucket_name, key);
                    Ok(())
                }
                Err(e) => {
                    if let Some(pb) = progress_bar {
                        pb.finish_with_message(format!("❌ S3 upload failed: {e}"));
                    }
                    log::error!("❌ Failed to write file to S3: s3://{}/{}: {}", bucket_name, key, e);
                    Err(e.into())
                }
            }
        }
    }
    
    async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let key = self.get_key(path);
        let bucket_name = self.get_bucket_name();
        
        let response = self.client
            .get_object()
            .bucket(&bucket_name)
            .key(&key)
            .send()
            .await?;
        
        let data = response.body.collect().await?;
        Ok(data.into_bytes().to_vec())
    }
    
    async fn list_directories(&self, path: &str) -> Result<Vec<String>> {
        let prefix = self.get_key(&format!("{path}/"));
        let bucket_name = self.get_bucket_name();
        
        log::debug!("📋 Listing S3 directories - bucket: {}, prefix: {}, path: {}", bucket_name, prefix, path);
        
        let response = self.client
            .list_objects_v2()
            .bucket(&bucket_name)
            .prefix(&prefix)
            .delimiter("/")
            .send()
            .await
            .map_err(|e| {
                log::error!("❌ S3 ListObjects failed for bucket '{}', prefix '{}': {}", bucket_name, prefix, e);
                anyhow::anyhow!("Failed to list S3 directories in bucket '{}' with prefix '{}': {}", bucket_name, prefix, e)
            })?;
        
        let mut directories = Vec::new();
        if let Some(common_prefixes) = response.common_prefixes {
            for prefix in common_prefixes {
                if let Some(prefix_str) = prefix.prefix {
                    if let Some(dir_name) = prefix_str.trim_end_matches('/').split('/').next_back() {
                        directories.push(dir_name.to_string());
                    }
                }
            }
        }
        
        Ok(directories)
    }
    
    async fn delete_file(&self, path: &str) -> Result<()> {
        let key = self.get_key(path);
        let bucket_name = self.get_bucket_name();
        
        self.client
            .delete_object()
            .bucket(&bucket_name)
            .key(&key)
            .send()
            .await?;
        
        Ok(())
    }
    
    fn supports_duckdb_direct_access(&self) -> bool {
        true // DuckDB can read S3 directly with httpfs extension
    }
    
    fn get_duckdb_path(&self, path: &str) -> String {
        let bucket_name = self.get_bucket_name();
        if self.prefix.is_empty() {
            format!("s3://{}/{}", bucket_name, path)
        } else {
            format!("s3://{}/{}/{}", bucket_name, self.prefix, path)
        }
    }
    
    async fn list_snapshots(&self, source: &str) -> Result<Vec<SnapshotMetadata>> {
        let source_path = format!("sources/{source}");
        let snapshot_names = self.list_directories(&source_path).await?;
        let mut snapshots = Vec::new();
        
        for snapshot_name in snapshot_names {
            if let Some(_name) = snapshot_name.strip_prefix("snapshot_name=") {
                let snapshot_dir = format!("{source_path}/{snapshot_name}");
                let timestamps = self.list_directories(&snapshot_dir).await?;
                
                for timestamp in timestamps {
                    if let Some(_ts) = timestamp.strip_prefix("snapshot_timestamp=") {
                        let metadata_path = format!("{snapshot_dir}/{timestamp}/metadata.json");
                        if let Ok(metadata_data) = self.read_file(&metadata_path).await {
                            if let Ok(metadata) = serde_json::from_slice::<SnapshotMetadata>(&metadata_data) {
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
        let sources = self.list_directories("sources").await?;
        let mut all_snapshots = Vec::new();
        
        for source in sources {
            let snapshots = self.list_snapshots(&source).await?;
            all_snapshots.extend(snapshots);
        }
        
        all_snapshots.sort_by(|a, b| b.created.cmp(&a.created)); // Sort by creation time, newest first
        Ok(all_snapshots)
    }
    
    async fn list_snapshots_for_all_sources(&self) -> Result<std::collections::HashMap<String, Vec<String>>> {
        let mut result = std::collections::HashMap::new();
        
        // Recursively traverse sources directory to find all snapshot directories
        self.traverse_sources_directory("sources", &mut result).await?;
        
        Ok(result)
    }
    
    async fn file_exists(&self, path: &str) -> Result<bool> {
        let key = self.get_key(path);
        let bucket_name = self.get_bucket_name();
        
        match self.client
            .head_object()
            .bucket(&bucket_name)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}

impl S3Storage {
    // Helper method to traverse sources directory using breadth-first search
    async fn traverse_sources_directory(&self, start_path: &str, result: &mut std::collections::HashMap<String, Vec<String>>) -> Result<()> {
        let mut dirs_to_check = vec![start_path.to_string()];
        
        while let Some(path) = dirs_to_check.pop() {
            let dirs = self.list_directories(&path).await?;
            
            for dir in dirs {
                let dir_path = format!("{path}/{dir}");
                
                // Check if this directory contains snapshot_name= subdirectories
                let subdirs = self.list_directories(&dir_path).await?;
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
                    let source_path = dir_path.strip_prefix("sources/").unwrap_or(&dir_path);
                    snapshot_names.sort();
                    result.insert(source_path.to_string(), snapshot_names);
                } else {
                    // This might be a nested directory - add to check list
                    dirs_to_check.push(dir_path);
                }
            }
        }
        
        Ok(())
    }
}
