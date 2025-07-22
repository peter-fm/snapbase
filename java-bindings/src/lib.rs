//! Java bindings for snapbase-core
//! 
//! This module provides Java Native Interface (JNI) bindings for the snapbase core library.
//! It enables Java applications to use snapbase functionality through native method calls.

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jboolean, jlong, jobject, jstring, JNI_TRUE, JNI_FALSE};
use jni::JNIEnv;
use std::path::{Path, PathBuf};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::Array;

use snapbase_core::{
    SnapbaseWorkspace, 
    Result as SnapbaseResult,
    data::DataProcessor,
    change_detection::ChangeDetector,
    resolver::SnapshotResolver,
    snapshot::SnapshotMetadata,
    query::SnapshotQueryEngine,
    naming::SnapshotNamer,
    config::get_snapshot_config,
};

/// Wrapper for SnapbaseWorkspace that can be safely passed through JNI
struct WorkspaceHandle {
    workspace: SnapbaseWorkspace,
    runtime: tokio::runtime::Runtime,
}

impl WorkspaceHandle {
    fn new(workspace: SnapbaseWorkspace) -> SnapbaseResult<Self> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| snapbase_core::error::SnapbaseError::workspace(format!("Failed to create async runtime: {e}")))?;
        Ok(WorkspaceHandle { workspace, runtime })
    }
}

/// Convert a Java string to a Rust String
fn jstring_to_string(env: &mut JNIEnv, jstr: &JString) -> Result<String, jni::errors::Error> {
    env.get_string(jstr).map(|s| s.into())
}

/// Convert a Rust string to a Java string
fn string_to_jstring<'local>(env: &mut JNIEnv<'local>, s: &str) -> Result<JString<'local>, jni::errors::Error> {
    env.new_string(s)
}

/// Convert a SnapbaseResult to a JNI result, throwing Java exceptions on error
fn handle_result<T>(env: &mut JNIEnv, result: SnapbaseResult<T>) -> Result<T, jni::errors::Error> {
    result.map_err(|e| {
        let _ = env.throw_new("com/snapbase/SnapbaseException", format!("{e}"));
        jni::errors::Error::JavaException
    })
}

/// Create a new workspace
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeCreateWorkspace<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    workspace_path: JString<'local>,
) -> jlong {
    let workspace_path_str = match jstring_to_string(&mut env, &workspace_path) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert workspace path");
            return 0;
        }
    };
    
    let path = PathBuf::from(workspace_path_str);
    let workspace = match SnapbaseWorkspace::find_or_create(Some(&path)) {
        Ok(w) => w,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to create workspace: {e}"));
            return 0;
        }
    };
    
    let handle = match WorkspaceHandle::new(workspace) {
        Ok(h) => h,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to create workspace handle: {e}"));
            return 0;
        }
    };
    
    Box::into_raw(Box::new(handle)) as jlong
}

/// Initialize the workspace
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeInit<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    let workspace_handle = unsafe { &mut *(handle as *mut WorkspaceHandle) };
    
    let result = workspace_handle.workspace.create_config_with_force(false);
    
    if handle_result(&mut env, result).is_err() {
        // Error already thrown
    }
}

/// Create a snapshot
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeCreateSnapshot<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    file_path: JString<'local>,
    name: JObject<'local>,
) -> jstring {
    let workspace_handle = unsafe { &mut *(handle as *mut WorkspaceHandle) };
    
    let file_path_str = match jstring_to_string(&mut env, &file_path) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert file path");
            return std::ptr::null_mut();
        }
    };
    
    let name_str = if name.is_null() {
        None
    } else {
        match jstring_to_string(&mut env, &JString::from(name)) {
            Ok(s) => Some(s),
            Err(_) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert snapshot name");
                return std::ptr::null_mut();
            }
        }
    };
    
    // Convert file path to absolute path
    let input_path = if Path::new(&file_path_str).is_absolute() {
        PathBuf::from(&file_path_str)
    } else {
        workspace_handle.workspace.root.join(&file_path_str)
    };
    
    // Generate snapshot name if not provided
    let snapshot_name = if let Some(name) = name_str {
        name
    } else {
        let canonical_path = input_path.canonicalize()
            .unwrap_or_else(|_| input_path.clone())
            .to_string_lossy()
            .to_string();
            
        let existing_snapshots = match workspace_handle.runtime.block_on(async {
            let all_snapshots = workspace_handle.workspace.storage().list_snapshots_for_all_sources().await?;
            Ok::<Vec<String>, snapbase_core::error::SnapbaseError>(
                all_snapshots.get(&canonical_path).cloned().unwrap_or_default()
            )
        }) {
            Ok(snapshots) => snapshots,
            Err(e) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to list existing snapshots: {e}"));
                return std::ptr::null_mut();
            }
        };
        
        let snapshot_config = match get_snapshot_config() {
            Ok(config) => config,
            Err(e) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to get snapshot config: {e}"));
                return std::ptr::null_mut();
            }
        };
        
        let namer = SnapshotNamer::new(snapshot_config.default_name_pattern);
        match namer.generate_name(&file_path_str, &existing_snapshots) {
            Ok(name) => name,
            Err(e) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to generate snapshot name: {e}"));
                return std::ptr::null_mut();
            }
        }
    };
    
    // Extract source name from file path (like CLI does)
    let source_name = input_path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(&file_path_str);
    
    // Create the snapshot
    let metadata = match create_hive_snapshot(&workspace_handle.workspace, &input_path, source_name, &snapshot_name) {
        Ok(m) => m,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to create snapshot: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    let result_message = format!(
        "Created snapshot '{}' with {} rows, {} columns", 
        metadata.name, metadata.row_count, metadata.column_count
    );
    
    match string_to_jstring(&mut env, &result_message) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to create result string");
            std::ptr::null_mut()
        }
    }
}

/// Detect changes between current file and baseline
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeDetectChanges<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    file_path: JString<'local>,
    baseline: JString<'local>,
) -> jstring {
    let workspace_handle = unsafe { &mut *(handle as *mut WorkspaceHandle) };
    
    let file_path_str = match jstring_to_string(&mut env, &file_path) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert file path");
            return std::ptr::null_mut();
        }
    };
    
    let baseline_str = match jstring_to_string(&mut env, &baseline) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert baseline name");
            return std::ptr::null_mut();
        }
    };
    
    let resolver = SnapshotResolver::new(workspace_handle.workspace.clone());
    
    // Convert file path to absolute path
    let input_path = if Path::new(&file_path_str).is_absolute() {
        PathBuf::from(&file_path_str)
    } else {
        workspace_handle.workspace.root.join(&file_path_str)
    };
    
    // Extract source name from file path (like CLI does)
    let source_name = input_path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(&file_path_str);
    
    // Resolve baseline snapshot
    let baseline_snapshot = match resolver.resolve_by_name_for_source(&baseline_str, Some(source_name)) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to resolve baseline snapshot: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    // Load baseline metadata and data
    let baseline_metadata = if let Some(preloaded) = baseline_snapshot.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &baseline_snapshot.json_path {
        let metadata_data = match workspace_handle.runtime.block_on(async {
            workspace_handle.workspace.storage().read_file(json_path).await
        }) {
            Ok(data) => data,
            Err(e) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to read baseline metadata: {e}"));
                return std::ptr::null_mut();
            }
        };
        match serde_json::from_slice::<SnapshotMetadata>(&metadata_data) {
            Ok(metadata) => metadata,
            Err(e) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to parse baseline metadata: {e}"));
                return std::ptr::null_mut();
            }
        }
    } else {
        let _ = env.throw_new("com/snapbase/SnapbaseException", "Baseline snapshot not found");
        return std::ptr::null_mut();
    };
    
    // Load baseline data
    let data_path = match baseline_snapshot.data_path.as_ref() {
        Some(path) => path,
        None => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Baseline snapshot has no data path");
            return std::ptr::null_mut();
        }
    };
    
    let mut data_processor = match DataProcessor::new_with_workspace(&workspace_handle.workspace) {
        Ok(processor) => processor,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to create data processor: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    // Convert storage path to DuckDB-accessible path
    let baseline_row_data = match workspace_handle.runtime.block_on(async {
        data_processor.load_cloud_storage_data(&data_path, &workspace_handle.workspace).await
    }) {
        Ok(data) => data,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to load baseline data: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    // Load current data
    let mut current_data_processor = match DataProcessor::new_with_workspace(&workspace_handle.workspace) {
        Ok(processor) => processor,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to create current data processor: {e}"));
            return std::ptr::null_mut();
        }
    };
    let current_data_info = match current_data_processor.load_file(&input_path) {
        Ok(info) => info,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to load current file: {e}"));
            return std::ptr::null_mut();
        }
    };
    let current_row_data = match current_data_processor.extract_all_data() {
        Ok(data) => data,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to extract current data: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    // Perform change detection
    let changes = match ChangeDetector::detect_changes(
        &baseline_metadata.columns,
        &baseline_row_data,
        &current_data_info.columns,
        &current_row_data,
    ) {
        Ok(changes) => changes,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to detect changes: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    // Convert to JSON string
    let changes_json = match serde_json::to_value(&changes) {
        Ok(json) => json,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to serialize changes: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    let result_str = match serde_json::to_string_pretty(&changes_json) {
        Ok(s) => s,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to format changes: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    match string_to_jstring(&mut env, &result_str) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to create result string");
            std::ptr::null_mut()
        }
    }
}

/// Query historical snapshots using SQL with zero-copy Arrow return
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeQueryArrow<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    source: JString<'local>,
    sql: JString<'local>,
    array_ptr: jlong,
    schema_ptr: jlong,
) {
    let workspace_handle = unsafe { &mut *(handle as *mut WorkspaceHandle) };
    
    let source_str = match jstring_to_string(&mut env, &source) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert source");
            return;
        }
    };
    
    let sql_str = match jstring_to_string(&mut env, &sql) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert SQL");
            return;
        }
    };
    
    // Create query engine and execute query to get Arrow RecordBatch
    let mut query_engine = match SnapshotQueryEngine::new(workspace_handle.workspace.clone()) {
        Ok(engine) => engine,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to create query engine: {e}"));
            return;
        }
    };
    
    let record_batch = match query_engine.query_arrow(&source_str, &sql_str) {
        Ok(batch) => batch,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Query failed: {e}"));
            return;
        }
    };
    
    // Export to C Data Interface for zero-copy transfer to Java
    let array_ptr = array_ptr as *mut FFI_ArrowArray;
    let schema_ptr = schema_ptr as *mut FFI_ArrowSchema;
    
    // Convert RecordBatch to FFI structures using proper Arrow FFI API
    use arrow::array::StructArray;
    
    // Convert RecordBatch to StructArray
    let struct_array: StructArray = record_batch.into();
    
    // Get the underlying ArrayData
    let array_data = struct_array.into_data();
    
    // Convert to FFI using arrow::ffi::to_ffi
    match arrow::ffi::to_ffi(&array_data) {
        Ok((ffi_array, ffi_schema)) => {
            unsafe {
                // Copy FFI structures to the provided pointers
                *array_ptr = ffi_array;
                *schema_ptr = ffi_schema;
            }
        }
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to export to FFI: {e}"));
        }
    }
}

/// Get workspace path
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeGetPath<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    let workspace_handle = unsafe { &*(handle as *const WorkspaceHandle) };
    
    let path_str = workspace_handle.workspace.root.to_string_lossy().to_string();
    
    match string_to_jstring(&mut env, &path_str) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to create path string");
            std::ptr::null_mut()
        }
    }
}

/// List all snapshots
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeListSnapshots<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jobject {
    let workspace_handle = unsafe { &*(handle as *const WorkspaceHandle) };
    
    let snapshots = match workspace_handle.workspace.list_snapshots() {
        Ok(snapshots) => snapshots,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to list snapshots: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    // Create Java ArrayList
    let array_list_class = match env.find_class("java/util/ArrayList") {
        Ok(class) => class,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to find ArrayList class");
            return std::ptr::null_mut();
        }
    };
    
    let array_list = match env.new_object(&array_list_class, "()V", &[]) {
        Ok(list) => list,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to create ArrayList");
            return std::ptr::null_mut();
        }
    };
    
    for snapshot in snapshots {
        let jstr = match string_to_jstring(&mut env, &snapshot) {
            Ok(s) => s,
            Err(_) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to create snapshot string");
                return std::ptr::null_mut();
            }
        };
        
        if env.call_method(&array_list, "add", "(Ljava/lang/Object;)Z", &[(&jstr).into()]).is_err() {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to add snapshot to list");
            return std::ptr::null_mut();
        }
    }
    
    array_list.into_raw()
}

/// List snapshots for a specific source
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeListSnapshotsForSource<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    source_path: JString<'local>,
) -> jobject {
    let workspace_handle = unsafe { &*(handle as *const WorkspaceHandle) };
    
    let source_path_str = match jstring_to_string(&mut env, &source_path) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert source path");
            return std::ptr::null_mut();
        }
    };
    
    let snapshots = match workspace_handle.workspace.list_snapshots_for_source(&source_path_str) {
        Ok(snapshots) => snapshots,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to list snapshots for source: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    // Create Java ArrayList (same as above)
    let array_list_class = match env.find_class("java/util/ArrayList") {
        Ok(class) => class,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to find ArrayList class");
            return std::ptr::null_mut();
        }
    };
    
    let array_list = match env.new_object(&array_list_class, "()V", &[]) {
        Ok(list) => list,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to create ArrayList");
            return std::ptr::null_mut();
        }
    };
    
    for snapshot in snapshots {
        let jstr = match string_to_jstring(&mut env, &snapshot) {
            Ok(s) => s,
            Err(_) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to create snapshot string");
                return std::ptr::null_mut();
            }
        };
        
        if env.call_method(&array_list, "add", "(Ljava/lang/Object;)Z", &[(&jstr).into()]).is_err() {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to add snapshot to list");
            return std::ptr::null_mut();
        }
    }
    
    array_list.into_raw()
}

/// Check if a snapshot exists
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeSnapshotExists<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
) -> jboolean {
    let workspace_handle = unsafe { &mut *(handle as *mut WorkspaceHandle) };
    
    let name_str = match jstring_to_string(&mut env, &name) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert snapshot name");
            return JNI_FALSE;
        }
    };
    
    let exists = match workspace_handle.runtime.block_on(async {
        let all_snapshots = workspace_handle.workspace.storage().list_all_snapshots().await?;
        Ok::<bool, snapbase_core::error::SnapbaseError>(
            all_snapshots.iter().any(|snapshot| snapshot.name == name_str)
        )
    }) {
        Ok(exists) => exists,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to check snapshot existence: {e}"));
            return JNI_FALSE;
        }
    };
    
    if exists { JNI_TRUE } else { JNI_FALSE }
}

/// Get workspace statistics
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeStats<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    let workspace_handle = unsafe { &*(handle as *const WorkspaceHandle) };
    
    let stats = match workspace_handle.workspace.stats() {
        Ok(stats) => stats,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to get workspace stats: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    let stats_json = serde_json::json!({
        "snapshot_count": stats.snapshot_count,
        "diff_count": stats.diff_count,
        "total_archive_size": stats.total_archive_size,
        "total_json_size": stats.total_json_size,
        "total_diff_size": stats.total_diff_size
    });
    
    let stats_str = match serde_json::to_string_pretty(&stats_json) {
        Ok(s) => s,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to serialize stats: {e}"));
            return std::ptr::null_mut();
        }
    };
    
    match string_to_jstring(&mut env, &stats_str) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to create stats string");
            std::ptr::null_mut()
        }
    }
}

/// Compare two snapshots
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeDiff<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    source: JString<'local>,
    from_snapshot: JString<'local>,
    to_snapshot: JString<'local>,
) -> jstring {
    let workspace_handle = unsafe { &mut *(handle as *mut WorkspaceHandle) };
    
    let source_str = match jstring_to_string(&mut env, &source) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert source");
            return std::ptr::null_mut();
        }
    };
    
    let from_str = match jstring_to_string(&mut env, &from_snapshot) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert from snapshot");
            return std::ptr::null_mut();
        }
    };
    
    let to_str = match jstring_to_string(&mut env, &to_snapshot) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert to snapshot");
            return std::ptr::null_mut();
        }
    };
    
    let resolver = SnapshotResolver::new(workspace_handle.workspace.clone());
    
    // Resolve both snapshots
    let from_resolved = match resolver.resolve_by_name_for_source(&from_str, Some(&source_str)) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to resolve from snapshot '{}': {}", from_str, e));
            return std::ptr::null_mut();
        }
    };
    
    let to_resolved = match resolver.resolve_by_name_for_source(&to_str, Some(&source_str)) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to resolve to snapshot '{}': {}", to_str, e));
            return std::ptr::null_mut();
        }
    };

    // Create async runtime for data loading operations  
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to create runtime: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    // Load metadata for both snapshots
    let from_metadata = if let Some(preloaded) = from_resolved.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &from_resolved.json_path {
        let metadata_data = match rt.block_on(async {
            workspace_handle.workspace.storage().read_file(json_path).await
        }) {
            Ok(data) => data,
            Err(e) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to read from metadata: {}", e));
                return std::ptr::null_mut();
            }
        };
        match serde_json::from_slice::<SnapshotMetadata>(&metadata_data) {
            Ok(metadata) => metadata,
            Err(e) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to parse from metadata: {}", e));
                return std::ptr::null_mut();
            }
        }
    } else {
        let _ = env.throw_new("com/snapbase/SnapbaseException", "From snapshot not found");
        return std::ptr::null_mut();
    };
    
    let to_metadata = if let Some(preloaded) = to_resolved.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &to_resolved.json_path {
        let metadata_data = match rt.block_on(async {
            workspace_handle.workspace.storage().read_file(json_path).await
        }) {
            Ok(data) => data,
            Err(e) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to read to metadata: {}", e));
                return std::ptr::null_mut();
            }
        };
        match serde_json::from_slice::<SnapshotMetadata>(&metadata_data) {
            Ok(metadata) => metadata,
            Err(e) => {
                let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to parse to metadata: {}", e));
                return std::ptr::null_mut();
            }
        }
    } else {
        let _ = env.throw_new("com/snapbase/SnapbaseException", "To snapshot not found");
        return std::ptr::null_mut();
    };
    
    // Load data for both snapshots
    let mut data_processor = match DataProcessor::new_with_workspace(&workspace_handle.workspace) {
        Ok(processor) => processor,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to create data processor: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    let from_data_path = match from_resolved.data_path.as_ref() {
        Some(path) => path,
        None => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "From snapshot has no data path");
            return std::ptr::null_mut();
        }
    };
    
    let to_data_path = match to_resolved.data_path.as_ref() {
        Some(path) => path,
        None => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "To snapshot has no data path");
            return std::ptr::null_mut();
        }
    };
    
    let from_row_data = match rt.block_on(async {
        data_processor.load_cloud_storage_data(&from_data_path, &workspace_handle.workspace).await
    }) {
        Ok(data) => data,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to load from data: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    let to_row_data = match rt.block_on(async {
        data_processor.load_cloud_storage_data(&to_data_path, &workspace_handle.workspace).await
    }) {
        Ok(data) => data,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to load to data: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    // Perform change detection
    let changes = match ChangeDetector::detect_changes(
        &from_metadata.columns,
        &from_row_data,
        &to_metadata.columns,
        &to_row_data,
    ) {
        Ok(changes) => changes,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to detect changes: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    // Convert to JSON
    let diff_json = match serde_json::to_value(&changes) {
        Ok(json) => json,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to serialize diff: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    let result_str = match serde_json::to_string_pretty(&diff_json) {
        Ok(s) => s,
        Err(e) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", format!("Failed to format diff: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    match string_to_jstring(&mut env, &result_str) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to create result string");
            std::ptr::null_mut()
        }
    }
}

/// Export snapshot data to a file
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeExport<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    source: JString<'local>,
    output_file: JString<'local>,
    to_snapshot: JString<'local>,
    force: jboolean,
) -> jstring {
    let workspace_handle = unsafe { &mut *(handle as *mut WorkspaceHandle) };
    
    let source_str = match jstring_to_string(&mut env, &source) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert source");
            return std::ptr::null_mut();
        }
    };
    
    let output_str = match jstring_to_string(&mut env, &output_file) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert output file");
            return std::ptr::null_mut();
        }
    };
    
    let to_str = match jstring_to_string(&mut env, &to_snapshot) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert to snapshot");
            return std::ptr::null_mut();
        }
    };
    
    let force_bool = force == JNI_TRUE;
    
    // Resolve target snapshot
    let resolver = SnapshotResolver::new(workspace_handle.workspace.clone());
    let target_snapshot = match resolver.resolve_by_name_for_source(&to_str, Some(&source_str)) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let error_msg = format!("Failed to resolve target snapshot: {}", e);
            let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
            return std::ptr::null_mut();
        }
    };
    
    // Load target snapshot data
    let metadata = if let Some(preloaded) = target_snapshot.get_metadata() {
        preloaded.clone()
    } else if let Some(json_path) = &target_snapshot.json_path {
        let metadata_data = match workspace_handle.runtime.block_on(async {
            workspace_handle.workspace.storage().read_file(json_path).await
        }) {
            Ok(data) => data,
            Err(e) => {
                let error_msg = format!("Failed to read target metadata: {}", e);
                let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
                return std::ptr::null_mut();
            }
        };
        match serde_json::from_slice::<SnapshotMetadata>(&metadata_data) {
            Ok(meta) => meta,
            Err(e) => {
                let error_msg = format!("Failed to parse target metadata: {}", e);
                let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
                return std::ptr::null_mut();
            }
        }
    } else {
        let _ = env.throw_new("com/snapbase/SnapbaseException", "Target snapshot not found");
        return std::ptr::null_mut();
    };
    
    let target_schema = metadata.columns.clone();
    
    // Load row data from storage
    let data_path = match target_snapshot.data_path.as_ref() {
        Some(path) => path,
        None => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Target snapshot has no data path");
            return std::ptr::null_mut();
        }
    };
    
    let mut data_processor = match DataProcessor::new_with_workspace(&workspace_handle.workspace) {
        Ok(processor) => processor,
        Err(e) => {
            let error_msg = format!("Failed to create data processor: {}", e);
            let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
            return std::ptr::null_mut();
        }
    };
    
    let target_row_data = match workspace_handle.runtime.block_on(async {
        // Pass data_path directly - load_cloud_storage_data handles get_duckdb_path internally
        data_processor.load_cloud_storage_data(data_path, &workspace_handle.workspace).await
    }) {
        Ok(data) => data,
        Err(e) => {
            let error_msg = format!("Failed to load target data: {}", e);
            let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
            return std::ptr::null_mut();
        }
    };
    
    // Check if output file exists
    let output_path = Path::new(&output_str);
    if output_path.exists() && !force_bool {
        let error_msg = format!("Output file '{}' already exists. Use force=true to overwrite.", output_str);
        let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
        return std::ptr::null_mut();
    }
    
    // Create CSV content (simplified - always export as CSV for now)
    let mut csv_content = String::new();
    
    // Filter out snapbase metadata columns - only keep original data columns
    let original_columns: Vec<(usize, &snapbase_core::hash::ColumnInfo)> = target_schema
        .iter()
        .enumerate()
        .filter(|(_, col)| {
            !col.name.starts_with("__snapbase_") && 
            col.name != "snapshot_name" && 
            col.name != "snapshot_timestamp"
        })
        .collect();
    
    // Write header
    let headers: Vec<&str> = original_columns.iter().map(|(_, col)| col.name.as_str()).collect();
    csv_content.push_str(&headers.join(","));
    csv_content.push('\n');
    
    // Write data rows
    for row in &target_row_data {
        let row_values: Vec<String> = original_columns
            .iter()
            .map(|(idx, _)| {
                row.get(*idx).cloned().unwrap_or_default()
            })
            .collect();
        csv_content.push_str(&row_values.join(","));
        csv_content.push('\n');
    }
    
    // Write to file
    if let Err(e) = std::fs::write(output_path, csv_content) {
        let error_msg = format!("Failed to write export file: {}", e);
        let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
        return std::ptr::null_mut();
    }
    
    let result_message = format!(
        "Exported snapshot '{}' from '{}' to '{}' ({} rows, {} columns)",
        to_str, source_str, output_str, target_row_data.len(), target_schema.len()
    );
    
    match string_to_jstring(&mut env, &result_message) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to create result string");
            std::ptr::null_mut()
        }
    }
}

/// Close the workspace and free resources
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeClose<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    if handle != 0 {
        let _workspace_handle = unsafe { Box::from_raw(handle as *mut WorkspaceHandle) };
        // The workspace will be automatically dropped when the Box goes out of scope
    }
}

/// Create a Hive snapshot (adapted from CLI implementation)
fn create_hive_snapshot(
    workspace: &SnapbaseWorkspace,
    input_path: &Path,
    source_name: &str,
    snapshot_name: &str,
) -> SnapbaseResult<SnapshotMetadata> {
    use snapbase_core::data::DataProcessor;
    use snapbase_core::path_utils;
    use chrono::Utc;

    // Create timestamp
    let timestamp = Utc::now();
    let timestamp_str = timestamp.format("%Y%m%dT%H%M%S%.6fZ").to_string();
    
    // Create Hive directory structure path
    let hive_path_str = path_utils::join_for_storage_backend(&[
        "sources",
        source_name,
        &format!("snapshot_name={snapshot_name}"),
        &format!("snapshot_timestamp={timestamp_str}")
    ], workspace.storage());
    
    // Use async runtime to handle storage backend operations
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        workspace.storage().ensure_directory(&hive_path_str).await
    })?;
    
    // Process data with workspace-configured processor
    let mut processor = DataProcessor::new_with_workspace(workspace)?;
    let data_info = processor.load_file(input_path)?;
    
    // Create Parquet file using DuckDB COPY
    let parquet_relative_path = format!("{hive_path_str}/data.parquet");
    let parquet_path = workspace.storage().get_duckdb_path(&parquet_relative_path);
    
    // Export to Parquet using the same method as CLI
    let temp_path = std::path::Path::new(&parquet_path);
    processor.export_to_parquet_with_flags(temp_path, None)?;
    
    // Create metadata
    let metadata = SnapshotMetadata {
        format_version: "1.0.0".to_string(),
        name: snapshot_name.to_string(),
        created: timestamp,
        source: input_path.to_string_lossy().to_string(),
        source_hash: {
            let source_content = std::fs::read_to_string(input_path)?;
            use blake3::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(source_content.as_bytes());
            hasher.finalize().to_hex().to_string()
        },
        row_count: data_info.row_count,
        column_count: data_info.columns.len(),
        columns: data_info.columns.clone(),
        archive_size: None,
        parent_snapshot: None,
        sequence_number: 0,
        delta_from_parent: None,
        can_reconstruct_parent: false,
        source_path: Some(input_path.to_string_lossy().to_string()),
        source_fingerprint: Some({
            let source_content = std::fs::read_to_string(input_path)?;
            use blake3::Hasher;
            let mut hasher = Hasher::new();
            hasher.update(source_name.as_bytes());
            hasher.update(b":");
            hasher.update(source_content.as_bytes());
            format!("{}:{}", source_name, hasher.finalize().to_hex())
        }),
    };
    
    let metadata_json = serde_json::to_string_pretty(&metadata)?;
    let metadata_path = format!("{hive_path_str}/metadata.json");
    
    // Write metadata using storage backend
    let metadata_bytes = metadata_json.as_bytes();
    rt.block_on(async {
        workspace.storage().write_file(&metadata_path, metadata_bytes).await
    })?;
    
    Ok(metadata)
}