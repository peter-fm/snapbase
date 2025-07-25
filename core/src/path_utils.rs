//! Path utilities for storage backend operations
//!
//! This module provides path normalization functions specifically for storage backends.
//! Local filesystem operations should continue using std::path::Path directly.

use crate::storage::StorageBackend;

/// Normalize a path for use with a storage backend
///
/// - For cloud storage (S3, etc.): Always use Unix-style forward slashes
/// - For local storage: Keep OS-native path separators
pub fn normalize_for_storage_backend(path: &str, backend: &dyn StorageBackend) -> String {
    if is_cloud_storage(backend) {
        // Cloud storage always uses Unix-style paths
        path.replace("\\", "/")
    } else {
        // Local storage keeps OS-native separators
        path.to_string()
    }
}

/// Join path components for use with a storage backend
///
/// - For cloud storage (S3, etc.): Always use Unix-style forward slashes
/// - For local storage: Use OS-native path separators
pub fn join_for_storage_backend(components: &[&str], backend: &dyn StorageBackend) -> String {
    let separator = if is_cloud_storage(backend) {
        "/" // Cloud storage always uses forward slashes
    } else {
        std::path::MAIN_SEPARATOR_STR // Local storage uses OS-native separator
    };

    components.join(separator)
}

/// Check if a storage backend is cloud-based
///
/// This determines whether we need to normalize paths to Unix-style
fn is_cloud_storage(backend: &dyn StorageBackend) -> bool {
    let base_path = backend.get_base_path();
    base_path.starts_with("s3://")
        || base_path.starts_with("gs://")
        || base_path.starts_with("azure://")
        || base_path.starts_with("http://")
        || base_path.starts_with("https://")
}

/// Normalize a path specifically for S3 storage
///
/// This is a convenience function for cases where we know we're dealing with S3
pub fn normalize_for_s3(path: &str) -> String {
    path.replace("\\", "/")
}

/// Join path components specifically for S3 storage
///
/// This is a convenience function for cases where we know we're dealing with S3
pub fn join_for_s3(components: &[&str]) -> String {
    components.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalStorage, S3Storage};
    use std::path::PathBuf;

    #[test]
    fn test_normalize_for_local_storage() {
        let local_storage = LocalStorage::new(PathBuf::from("/tmp"));

        // Local storage should preserve the original path
        let windows_path = "sources\\test\\snapshot_name=test1";
        let result = normalize_for_storage_backend(windows_path, &local_storage);
        assert_eq!(result, windows_path);

        let unix_path = "sources/test/snapshot_name=test1";
        let result = normalize_for_storage_backend(unix_path, &local_storage);
        assert_eq!(result, unix_path);
    }

    #[tokio::test]
    async fn test_normalize_for_s3_storage() {
        // Note: This test requires AWS credentials to create S3Storage
        // In a real test environment, you'd mock this or use a test double
        if std::env::var("AWS_ACCESS_KEY_ID").is_ok() {
            let s3_storage = S3Storage::new(
                "test-bucket".to_string(),
                "test-prefix".to_string(),
                "us-east-1".to_string(),
                false,
                None,
            )
            .await;

            if let Ok(s3_storage) = s3_storage {
                // S3 storage should normalize backslashes to forward slashes
                let windows_path = "sources\\test\\snapshot_name=test1";
                let result = normalize_for_storage_backend(windows_path, &s3_storage);
                assert_eq!(result, "sources/test/snapshot_name=test1");

                let unix_path = "sources/test/snapshot_name=test1";
                let result = normalize_for_storage_backend(unix_path, &s3_storage);
                assert_eq!(result, unix_path);
            }
        }
    }

    #[test]
    fn test_join_for_local_storage() {
        let local_storage = LocalStorage::new(PathBuf::from("/tmp"));

        let components = &["sources", "test", "snapshot_name=test1"];
        let result = join_for_storage_backend(components, &local_storage);

        // Should use OS-native separator
        #[cfg(windows)]
        assert_eq!(result, "sources\\test\\snapshot_name=test1");

        #[cfg(not(windows))]
        assert_eq!(result, "sources/test/snapshot_name=test1");
    }

    #[test]
    fn test_s3_convenience_functions() {
        let windows_path = "sources\\test\\snapshot_name=test1";
        let result = normalize_for_s3(windows_path);
        assert_eq!(result, "sources/test/snapshot_name=test1");

        let components = &["sources", "test", "snapshot_name=test1"];
        let result = join_for_s3(components);
        assert_eq!(result, "sources/test/snapshot_name=test1");
    }
}
