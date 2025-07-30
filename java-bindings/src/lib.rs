//! Java bindings for snapbase-core
//!
//! This module provides Java Native Interface (JNI) bindings for the snapbase core library.
//! It enables Java applications to use snapbase functionality through native method calls.

use arrow::array::Array;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jboolean, jlong, jobject, jstring, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use std::path::{Path, PathBuf};

use snapbase_core::{
    change_detection::StreamingChangeDetector,
    config::get_snapshot_config_with_workspace,
    naming::SnapshotNamer,
    query::execute_query_with_describe,
    query_engine::{create_configured_connection, register_workspace_source_views},
    resolver::SnapshotResolver,
    snapshot::SnapshotMetadata,
    ExportFormat, ExportOptions, Result as SnapbaseResult, SnapbaseWorkspace, UnifiedExporter,
};

/// Wrapper for SnapbaseWorkspace that can be safely passed through JNI
struct WorkspaceHandle {
    workspace: SnapbaseWorkspace,
    runtime: tokio::runtime::Runtime,
}

impl WorkspaceHandle {
    fn new(workspace: SnapbaseWorkspace) -> SnapbaseResult<Self> {
        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            snapbase_core::error::SnapbaseError::workspace(format!(
                "Failed to create async runtime: {e}"
            ))
        })?;
        Ok(WorkspaceHandle { workspace, runtime })
    }
}

/// Convert a Java string to a Rust String
fn jstring_to_string(env: &mut JNIEnv, jstr: &JString) -> Result<String, jni::errors::Error> {
    env.get_string(jstr).map(|s| s.into())
}

/// Convert a Rust string to a Java string
fn string_to_jstring<'local>(
    env: &mut JNIEnv<'local>,
    s: &str,
) -> Result<JString<'local>, jni::errors::Error> {
    env.new_string(s)
}

/// Convert a SnapbaseResult to a JNI result, throwing Java exceptions on error
fn handle_result<T>(env: &mut JNIEnv, result: SnapbaseResult<T>) -> Result<T, jni::errors::Error> {
    result.map_err(|e| {
        let _ = env.throw_new("com/snapbase/SnapbaseException", format!("{e}"));
        jni::errors::Error::JavaException
    })
}

/// Convert ChangeDetectionResult to Java object
fn change_detection_result_to_jobject<'local>(
    env: &mut JNIEnv<'local>,
    result: &snapbase_core::change_detection::ChangeDetectionResult,
) -> Result<JObject<'local>, jni::errors::Error> {
    // Create SchemaChanges object
    let schema_changes = schema_changes_to_jobject(env, &result.schema_changes)?;

    // Create RowChanges object
    let row_changes = row_changes_to_jobject(env, &result.row_changes)?;

    // Create ChangeDetectionResult object
    let class = env.find_class("com/snapbase/ChangeDetectionResult")?;
    let constructor_sig = "(Lcom/snapbase/SchemaChanges;Lcom/snapbase/RowChanges;)V";
    let obj = env.new_object(
        &class,
        constructor_sig,
        &[(&schema_changes).into(), (&row_changes).into()],
    )?;

    Ok(obj)
}

/// Convert SchemaChanges to Java object
fn schema_changes_to_jobject<'local>(
    env: &mut JNIEnv<'local>,
    schema_changes: &snapbase_core::change_detection::SchemaChanges,
) -> Result<JObject<'local>, jni::errors::Error> {
    // Create ColumnOrderChange object (nullable)
    let column_order = match &schema_changes.column_order {
        Some(change) => {
            let class = env.find_class("com/snapbase/ColumnOrderChange")?;
            let before_list = string_vec_to_arraylist(env, &change.before)?;
            let after_list = string_vec_to_arraylist(env, &change.after)?;
            let obj = env.new_object(
                &class,
                "(Ljava/util/List;Ljava/util/List;)V",
                &[(&before_list).into(), (&after_list).into()],
            )?;
            Some(obj)
        }
        None => None,
    };

    // Create lists for additions, removals, renames, and type changes
    let columns_added = column_additions_to_arraylist(env, &schema_changes.columns_added)?;
    let columns_removed = column_removals_to_arraylist(env, &schema_changes.columns_removed)?;
    let columns_renamed = column_renames_to_arraylist(env, &schema_changes.columns_renamed)?;
    let type_changes = type_changes_to_arraylist(env, &schema_changes.type_changes)?;

    // Create SchemaChanges object
    let class = env.find_class("com/snapbase/SchemaChanges")?;
    let constructor_sig = "(Lcom/snapbase/ColumnOrderChange;Ljava/util/List;Ljava/util/List;Ljava/util/List;Ljava/util/List;)V";

    let null_obj = JObject::null();
    let column_order_arg = match column_order {
        Some(ref obj) => obj.into(),
        None => (&null_obj).into(),
    };

    let obj = env.new_object(
        &class,
        constructor_sig,
        &[
            column_order_arg,
            (&columns_added).into(),
            (&columns_removed).into(),
            (&columns_renamed).into(),
            (&type_changes).into(),
        ],
    )?;

    Ok(obj)
}

/// Convert RowChanges to Java object
fn row_changes_to_jobject<'local>(
    env: &mut JNIEnv<'local>,
    row_changes: &snapbase_core::change_detection::RowChanges,
) -> Result<JObject<'local>, jni::errors::Error> {
    let modified = row_modifications_to_arraylist(env, &row_changes.modified)?;
    let added = row_additions_to_arraylist(env, &row_changes.added)?;
    let removed = row_removals_to_arraylist(env, &row_changes.removed)?;

    let class = env.find_class("com/snapbase/RowChanges")?;
    let constructor_sig = "(Ljava/util/List;Ljava/util/List;Ljava/util/List;)V";
    let obj = env.new_object(
        &class,
        constructor_sig,
        &[(&modified).into(), (&added).into(), (&removed).into()],
    )?;

    Ok(obj)
}

/// Helper function to create ArrayList from Vec<String>
fn string_vec_to_arraylist<'local>(
    env: &mut JNIEnv<'local>,
    vec: &[String],
) -> Result<JObject<'local>, jni::errors::Error> {
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let array_list = env.new_object(&array_list_class, "()V", &[])?;

    for item in vec {
        let jstr = string_to_jstring(env, item)?;
        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&jstr).into()],
        )?;
    }

    Ok(array_list)
}

/// Helper function to create ArrayList of ColumnAddition objects
fn column_additions_to_arraylist<'local>(
    env: &mut JNIEnv<'local>,
    additions: &[snapbase_core::change_detection::ColumnAddition],
) -> Result<JObject<'local>, jni::errors::Error> {
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let array_list = env.new_object(&array_list_class, "()V", &[])?;

    for addition in additions {
        let class = env.find_class("com/snapbase/ColumnAddition")?;
        let name = string_to_jstring(env, &addition.name)?;
        let data_type = string_to_jstring(env, &addition.data_type)?;
        let position = addition.position as i32;
        let nullable = addition.nullable;
        let default_jstring = match &addition.default_value {
            Some(val) => Some(string_to_jstring(env, val)?),
            None => None,
        };
        let null_obj = JObject::null();
        let default_value = match default_jstring {
            Some(ref jstr) => jstr.into(),
            None => (&null_obj).into(),
        };

        let obj = env.new_object(
            &class,
            "(Ljava/lang/String;Ljava/lang/String;IZLjava/lang/String;)V",
            &[
                (&name).into(),
                (&data_type).into(),
                position.into(),
                nullable.into(),
                default_value,
            ],
        )?;

        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&obj).into()],
        )?;
    }

    Ok(array_list)
}

/// Helper function to create ArrayList of ColumnRemoval objects
fn column_removals_to_arraylist<'local>(
    env: &mut JNIEnv<'local>,
    removals: &[snapbase_core::change_detection::ColumnRemoval],
) -> Result<JObject<'local>, jni::errors::Error> {
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let array_list = env.new_object(&array_list_class, "()V", &[])?;

    for removal in removals {
        let class = env.find_class("com/snapbase/ColumnRemoval")?;
        let name = string_to_jstring(env, &removal.name)?;
        let data_type = string_to_jstring(env, &removal.data_type)?;
        let position = removal.position as i32;
        let nullable = removal.nullable;

        let obj = env.new_object(
            &class,
            "(Ljava/lang/String;Ljava/lang/String;IZ)V",
            &[
                (&name).into(),
                (&data_type).into(),
                position.into(),
                nullable.into(),
            ],
        )?;

        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&obj).into()],
        )?;
    }

    Ok(array_list)
}

/// Helper function to create ArrayList of ColumnRename objects
fn column_renames_to_arraylist<'local>(
    env: &mut JNIEnv<'local>,
    renames: &[snapbase_core::change_detection::ColumnRename],
) -> Result<JObject<'local>, jni::errors::Error> {
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let array_list = env.new_object(&array_list_class, "()V", &[])?;

    for rename in renames {
        let class = env.find_class("com/snapbase/ColumnRename")?;
        let from = string_to_jstring(env, &rename.from)?;
        let to = string_to_jstring(env, &rename.to)?;

        let obj = env.new_object(
            &class,
            "(Ljava/lang/String;Ljava/lang/String;)V",
            &[(&from).into(), (&to).into()],
        )?;

        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&obj).into()],
        )?;
    }

    Ok(array_list)
}

/// Helper function to create ArrayList of TypeChange objects
fn type_changes_to_arraylist<'local>(
    env: &mut JNIEnv<'local>,
    type_changes: &[snapbase_core::change_detection::TypeChange],
) -> Result<JObject<'local>, jni::errors::Error> {
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let array_list = env.new_object(&array_list_class, "()V", &[])?;

    for type_change in type_changes {
        let class = env.find_class("com/snapbase/TypeChange")?;
        let column = string_to_jstring(env, &type_change.column)?;
        let from = string_to_jstring(env, &type_change.from)?;
        let to = string_to_jstring(env, &type_change.to)?;

        let obj = env.new_object(
            &class,
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
            &[(&column).into(), (&from).into(), (&to).into()],
        )?;

        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&obj).into()],
        )?;
    }

    Ok(array_list)
}

/// Helper function to create ArrayList of RowModification objects
fn row_modifications_to_arraylist<'local>(
    env: &mut JNIEnv<'local>,
    modifications: &[snapbase_core::change_detection::RowModification],
) -> Result<JObject<'local>, jni::errors::Error> {
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let array_list = env.new_object(&array_list_class, "()V", &[])?;

    for modification in modifications {
        let class = env.find_class("com/snapbase/RowModification")?;
        let row_index = modification.row_index as i64;
        let changes_map = cell_changes_to_hashmap(env, &modification.changes)?;

        let obj = env.new_object(
            &class,
            "(JLjava/util/Map;)V",
            &[row_index.into(), (&changes_map).into()],
        )?;

        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&obj).into()],
        )?;
    }

    Ok(array_list)
}

/// Helper function to create ArrayList of RowAddition objects
fn row_additions_to_arraylist<'local>(
    env: &mut JNIEnv<'local>,
    additions: &[snapbase_core::change_detection::RowAddition],
) -> Result<JObject<'local>, jni::errors::Error> {
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let array_list = env.new_object(&array_list_class, "()V", &[])?;

    for addition in additions {
        let class = env.find_class("com/snapbase/RowAddition")?;
        let row_index = addition.row_index as i64;
        let data_map = string_map_to_hashmap(env, &addition.data)?;

        let obj = env.new_object(
            &class,
            "(JLjava/util/Map;)V",
            &[row_index.into(), (&data_map).into()],
        )?;

        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&obj).into()],
        )?;
    }

    Ok(array_list)
}

/// Helper function to create ArrayList of RowRemoval objects
fn row_removals_to_arraylist<'local>(
    env: &mut JNIEnv<'local>,
    removals: &[snapbase_core::change_detection::RowRemoval],
) -> Result<JObject<'local>, jni::errors::Error> {
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let array_list = env.new_object(&array_list_class, "()V", &[])?;

    for removal in removals {
        let class = env.find_class("com/snapbase/RowRemoval")?;
        let row_index = removal.row_index as i64;
        let data_map = string_map_to_hashmap(env, &removal.data)?;

        let obj = env.new_object(
            &class,
            "(JLjava/util/Map;)V",
            &[row_index.into(), (&data_map).into()],
        )?;

        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&obj).into()],
        )?;
    }

    Ok(array_list)
}

/// Helper function to create HashMap from HashMap<String, CellChange>
fn cell_changes_to_hashmap<'local>(
    env: &mut JNIEnv<'local>,
    changes: &std::collections::HashMap<String, snapbase_core::change_detection::CellChange>,
) -> Result<JObject<'local>, jni::errors::Error> {
    let hashmap_class = env.find_class("java/util/HashMap")?;
    let hashmap = env.new_object(&hashmap_class, "()V", &[])?;

    for (key, value) in changes {
        let key_str = string_to_jstring(env, key)?;

        // Create CellChange object
        let cell_change_class = env.find_class("com/snapbase/CellChange")?;
        let before = string_to_jstring(env, &value.before)?;
        let after = string_to_jstring(env, &value.after)?;
        let cell_change = env.new_object(
            &cell_change_class,
            "(Ljava/lang/String;Ljava/lang/String;)V",
            &[(&before).into(), (&after).into()],
        )?;

        env.call_method(
            &hashmap,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[(&key_str).into(), (&cell_change).into()],
        )?;
    }

    Ok(hashmap)
}

/// Helper function to create HashMap from HashMap<String, String>
fn string_map_to_hashmap<'local>(
    env: &mut JNIEnv<'local>,
    map: &std::collections::HashMap<String, String>,
) -> Result<JObject<'local>, jni::errors::Error> {
    let hashmap_class = env.find_class("java/util/HashMap")?;
    let hashmap = env.new_object(&hashmap_class, "()V", &[])?;

    for (key, value) in map {
        let key_str = string_to_jstring(env, key)?;
        let value_str = string_to_jstring(env, value)?;

        env.call_method(
            &hashmap,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[(&key_str).into(), (&value_str).into()],
        )?;
    }

    Ok(hashmap)
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
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to convert workspace path",
            );
            return 0;
        }
    };

    let path = PathBuf::from(workspace_path_str);
    // Use create_at_path for explicit workspace paths to avoid directory traversal
    let workspace = match SnapbaseWorkspace::create_at_path(&path) {
        Ok(w) => w,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to create workspace: {e}"),
            );
            return 0;
        }
    };

    let handle = match WorkspaceHandle::new(workspace) {
        Ok(h) => h,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to create workspace handle: {e}"),
            );
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
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to convert file path",
            );
            return std::ptr::null_mut();
        }
    };

    let name_str = if name.is_null() {
        None
    } else {
        match jstring_to_string(&mut env, &JString::from(name)) {
            Ok(s) => Some(s),
            Err(_) => {
                let _ = env.throw_new(
                    "com/snapbase/SnapbaseException",
                    "Failed to convert snapshot name",
                );
                return std::ptr::null_mut();
            }
        }
    };

    // Convert file path to absolute path
    let input_path = if Path::new(&file_path_str).is_absolute() {
        PathBuf::from(&file_path_str)
    } else {
        workspace_handle.workspace.root().join(&file_path_str)
    };

    // Generate snapshot name if not provided
    let snapshot_name = if let Some(name) = name_str {
        name
    } else {
        let canonical_path = input_path
            .canonicalize()
            .unwrap_or_else(|_| input_path.clone())
            .to_string_lossy()
            .to_string();

        let existing_snapshots = match workspace_handle.runtime.block_on(async {
            let all_snapshots = workspace_handle
                .workspace
                .storage()
                .list_snapshots_for_all_sources()
                .await?;
            Ok::<Vec<String>, snapbase_core::error::SnapbaseError>(
                all_snapshots
                    .get(&canonical_path)
                    .cloned()
                    .unwrap_or_default(),
            )
        }) {
            Ok(snapshots) => snapshots,
            Err(e) => {
                let _ = env.throw_new(
                    "com/snapbase/SnapbaseException",
                    format!("Failed to list existing snapshots: {e}"),
                );
                return std::ptr::null_mut();
            }
        };

        let snapshot_config =
            match get_snapshot_config_with_workspace(Some(workspace_handle.workspace.root())) {
                Ok(config) => config,
                Err(e) => {
                    let _ = env.throw_new(
                        "com/snapbase/SnapbaseException",
                        format!("Failed to get snapshot config: {e}"),
                    );
                    return std::ptr::null_mut();
                }
            };

        let namer = SnapshotNamer::new(snapshot_config.default_name_pattern);
        match namer.generate_name(&file_path_str, &existing_snapshots) {
            Ok(name) => name,
            Err(e) => {
                let _ = env.throw_new(
                    "com/snapbase/SnapbaseException",
                    format!("Failed to generate snapshot name: {e}"),
                );
                return std::ptr::null_mut();
            }
        }
    };

    // Extract source name from file path (like CLI does)
    let source_name = input_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(&file_path_str);

    // Check if snapshot with this name already exists for this source
    let snapshot_exists = match workspace_handle
        .workspace
        .snapshot_exists_for_source(source_name, &snapshot_name)
    {
        Ok(exists) => exists,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to check existing snapshots: {e}"),
            );
            return std::ptr::null_mut();
        }
    };

    if snapshot_exists {
        let _ = env.throw_new(
            "com/snapbase/SnapbaseException",
            format!("Snapshot '{}' already exists. Use a different name or remove the existing snapshot.", snapshot_name),
        );
        return std::ptr::null_mut();
    }

    // Create the snapshot
    let metadata = match create_hive_snapshot(
        &workspace_handle.workspace,
        &input_path,
        source_name,
        &snapshot_name,
    ) {
        Ok(m) => m,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to create snapshot: {e}"),
            );
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
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to create result string",
            );
            std::ptr::null_mut()
        }
    }
}

/// Check status of current file against baseline
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeStatus<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    file_path: JString<'local>,
    baseline: JString<'local>,
) -> jobject {
    let workspace_handle = unsafe { &mut *(handle as *mut WorkspaceHandle) };

    let file_path_str = match jstring_to_string(&mut env, &file_path) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to convert file path",
            );
            return std::ptr::null_mut();
        }
    };

    let baseline_str = match jstring_to_string(&mut env, &baseline) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to convert baseline name",
            );
            return std::ptr::null_mut();
        }
    };

    let resolver = SnapshotResolver::new(workspace_handle.workspace.clone());

    // Convert file path to absolute path
    let input_path = if Path::new(&file_path_str).is_absolute() {
        PathBuf::from(&file_path_str)
    } else {
        workspace_handle.workspace.root().join(&file_path_str)
    };

    // Extract source name from file path (like CLI does)
    let source_name = input_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(&file_path_str);

    // Resolve baseline snapshot
    let baseline_snapshot =
        match resolver.resolve_by_name_for_source(&baseline_str, Some(source_name)) {
            Ok(snapshot) => snapshot,
            Err(e) => {
                let _ = env.throw_new(
                    "com/snapbase/SnapbaseException",
                    format!("Failed to resolve baseline snapshot: {e}"),
                );
                return std::ptr::null_mut();
            }
        };

    // Get baseline data path for streaming comparison
    let data_path = match baseline_snapshot.data_path.as_ref() {
        Some(path) => path,
        None => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Baseline snapshot has no data path",
            );
            return std::ptr::null_mut();
        }
    };

    // Create data sources for streaming comparison
    let baseline_source = snapbase_core::change_detection::DataSource::StoredSnapshot {
        path: data_path.clone(),
        workspace: workspace_handle.workspace.clone(),
    };
    let current_source = snapbase_core::change_detection::DataSource::File(input_path);

    // Configure comparison options
    let options = snapbase_core::change_detection::ComparisonOptions::default();

    // Perform streaming change detection
    let changes = match workspace_handle.runtime.block_on(async {
        StreamingChangeDetector::compare_data_sources(
            baseline_source,
            current_source,
            options,
            None, // No progress callback for now
        )
        .await
    }) {
        Ok(changes) => changes,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to detect changes: {e}"),
            );
            return std::ptr::null_mut();
        }
    };

    // Convert to Java object
    match change_detection_result_to_jobject(&mut env, &changes) {
        Ok(obj) => obj.into_raw(),
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to create result object",
            );
            std::ptr::null_mut()
        }
    }
}

/// Query workspace sources with SQL (workspace-wide queries)
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeQueryArrow<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    sql: JString<'local>,
    array_ptr: jlong,
    schema_ptr: jlong,
) {
    let workspace_handle = unsafe { &mut *(handle as *mut WorkspaceHandle) };

    let sql_str = match jstring_to_string(&mut env, &sql) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("com/snapbase/SnapbaseException", "Failed to convert SQL");
            return;
        }
    };

    // Create DuckDB connection configured for the workspace storage backend
    let connection = match create_configured_connection(&workspace_handle.workspace) {
        Ok(conn) => conn,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to create connection: {e}"),
            );
            return;
        }
    };

    // Register workspace-wide views (all sources with * pattern)
    let registered_views =
        match register_workspace_source_views(&connection, &workspace_handle.workspace, "*") {
            Ok(views) => views,
            Err(e) => {
                let _ = env.throw_new(
                    "com/snapbase/SnapbaseException",
                    format!("Failed to register workspace views: {e}"),
                );
                return;
            }
        };

    if registered_views.is_empty() {
        let _ = env.throw_new(
            "com/snapbase/SnapbaseException",
            "No sources found in workspace",
        );
        return;
    }

    // Execute query using the shared helper function
    let result = match execute_query_with_describe(&connection, &sql_str) {
        Ok(res) => res,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Workspace query failed: {e}"),
            );
            return;
        }
    };

    // Convert QueryResult to Arrow RecordBatch
    let record_batch = match query_result_to_arrow(&result) {
        Ok(batch) => batch,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to convert to Arrow: {e}"),
            );
            return;
        }
    };

    // Export to C Data Interface for zero-copy transfer to Java
    export_arrow_to_ffi(&mut env, record_batch, array_ptr, schema_ptr);
}

/// Get workspace path
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeGetPath<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    let workspace_handle = unsafe { &*(handle as *const WorkspaceHandle) };

    let path_str = workspace_handle
        .workspace
        .root()
        .to_string_lossy()
        .to_string();

    match string_to_jstring(&mut env, &path_str) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to create path string",
            );
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
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to list snapshots: {e}"),
            );
            return std::ptr::null_mut();
        }
    };

    // Create Java ArrayList
    let array_list_class = match env.find_class("java/util/ArrayList") {
        Ok(class) => class,
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to find ArrayList class",
            );
            return std::ptr::null_mut();
        }
    };

    let array_list = match env.new_object(&array_list_class, "()V", &[]) {
        Ok(list) => list,
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to create ArrayList",
            );
            return std::ptr::null_mut();
        }
    };

    for snapshot in snapshots {
        let jstr = match string_to_jstring(&mut env, &snapshot) {
            Ok(s) => s,
            Err(_) => {
                let _ = env.throw_new(
                    "com/snapbase/SnapbaseException",
                    "Failed to create snapshot string",
                );
                return std::ptr::null_mut();
            }
        };

        if env
            .call_method(
                &array_list,
                "add",
                "(Ljava/lang/Object;)Z",
                &[(&jstr).into()],
            )
            .is_err()
        {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to add snapshot to list",
            );
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
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to convert source path",
            );
            return std::ptr::null_mut();
        }
    };

    let snapshots = match workspace_handle
        .workspace
        .list_snapshots_for_source(&source_path_str)
    {
        Ok(snapshots) => snapshots,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to list snapshots for source: {e}"),
            );
            return std::ptr::null_mut();
        }
    };

    // Create Java ArrayList (same as above)
    let array_list_class = match env.find_class("java/util/ArrayList") {
        Ok(class) => class,
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to find ArrayList class",
            );
            return std::ptr::null_mut();
        }
    };

    let array_list = match env.new_object(&array_list_class, "()V", &[]) {
        Ok(list) => list,
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to create ArrayList",
            );
            return std::ptr::null_mut();
        }
    };

    for snapshot in snapshots {
        let jstr = match string_to_jstring(&mut env, &snapshot) {
            Ok(s) => s,
            Err(_) => {
                let _ = env.throw_new(
                    "com/snapbase/SnapbaseException",
                    "Failed to create snapshot string",
                );
                return std::ptr::null_mut();
            }
        };

        if env
            .call_method(
                &array_list,
                "add",
                "(Ljava/lang/Object;)Z",
                &[(&jstr).into()],
            )
            .is_err()
        {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to add snapshot to list",
            );
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
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to convert snapshot name",
            );
            return JNI_FALSE;
        }
    };

    let exists = match workspace_handle.runtime.block_on(async {
        let all_snapshots = workspace_handle
            .workspace
            .storage()
            .list_all_snapshots()
            .await?;
        Ok::<bool, snapbase_core::error::SnapbaseError>(
            all_snapshots
                .iter()
                .any(|snapshot| snapshot.name == name_str),
        )
    }) {
        Ok(exists) => exists,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to check snapshot existence: {e}"),
            );
            return JNI_FALSE;
        }
    };

    if exists {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
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
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to get workspace stats: {e}"),
            );
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
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to serialize stats: {e}"),
            );
            return std::ptr::null_mut();
        }
    };

    match string_to_jstring(&mut env, &stats_str) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to create stats string",
            );
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
) -> jobject {
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
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to convert from snapshot",
            );
            return std::ptr::null_mut();
        }
    };

    let to_str = match jstring_to_string(&mut env, &to_snapshot) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to convert to snapshot",
            );
            return std::ptr::null_mut();
        }
    };

    let resolver = SnapshotResolver::new(workspace_handle.workspace.clone());

    // Resolve both snapshots
    let from_resolved = match resolver.resolve_by_name_for_source(&from_str, Some(&source_str)) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to resolve from snapshot '{from_str}': {e}"),
            );
            return std::ptr::null_mut();
        }
    };

    let to_resolved = match resolver.resolve_by_name_for_source(&to_str, Some(&source_str)) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to resolve to snapshot '{to_str}': {e}"),
            );
            return std::ptr::null_mut();
        }
    };

    // Create async runtime for data loading operations
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to create runtime: {e}"),
            );
            return std::ptr::null_mut();
        }
    };

    let from_data_path = match from_resolved.data_path.as_ref() {
        Some(path) => path,
        None => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "From snapshot has no data path",
            );
            return std::ptr::null_mut();
        }
    };

    let to_data_path = match to_resolved.data_path.as_ref() {
        Some(path) => path,
        None => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "To snapshot has no data path",
            );
            return std::ptr::null_mut();
        }
    };

    // Create data sources for streaming comparison
    let baseline_source = snapbase_core::change_detection::DataSource::StoredSnapshot {
        path: from_data_path.clone(),
        workspace: workspace_handle.workspace.clone(),
    };
    let current_source = snapbase_core::change_detection::DataSource::StoredSnapshot {
        path: to_data_path.clone(),
        workspace: workspace_handle.workspace.clone(),
    };

    // Configure comparison options
    let options = snapbase_core::change_detection::ComparisonOptions::default();

    // Perform streaming change detection
    let changes = match rt.block_on(async {
        StreamingChangeDetector::compare_data_sources(
            baseline_source,
            current_source,
            options,
            None, // No progress callback for now
        )
        .await
    }) {
        Ok(changes) => changes,
        Err(e) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to detect changes: {e}"),
            );
            return std::ptr::null_mut();
        }
    };

    // Convert to Java object
    match change_detection_result_to_jobject(&mut env, &changes) {
        Ok(obj) => obj.into_raw(),
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to create result object",
            );
            std::ptr::null_mut()
        }
    }
}

/// Export snapshot data to a file using unified export functionality
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
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to convert output file",
            );
            return std::ptr::null_mut();
        }
    };

    let to_str = match jstring_to_string(&mut env, &to_snapshot) {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to convert to snapshot",
            );
            return std::ptr::null_mut();
        }
    };

    let force_bool = force == JNI_TRUE;

    // Build export options
    let options = ExportOptions {
        include_header: true,
        delimiter: ',',
        force: force_bool,
        snapshot_name: Some(to_str.clone()),
        snapshot_date: None,
    };

    let output_path = Path::new(&output_str);

    // Determine output format for reporting
    let export_format = match ExportFormat::from_extension(output_path) {
        Ok(format) => format,
        Err(e) => {
            let error_msg = format!("Invalid output format: {e}");
            let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
            return std::ptr::null_mut();
        }
    };

    // Use the unified exporter
    let mut exporter = match UnifiedExporter::new(workspace_handle.workspace.clone()) {
        Ok(exp) => exp,
        Err(e) => {
            let error_msg = format!("Failed to create exporter: {e}");
            let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
            return std::ptr::null_mut();
        }
    };

    if let Err(e) = exporter.export(&source_str, output_path, options) {
        let error_msg = format!("Export failed: {e}");
        let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
        return std::ptr::null_mut();
    }

    let result_message = format!(
        "Exported snapshot '{to_str}' from '{source_str}' to '{output_str}' ({export_format:?} format)"
    );

    match string_to_jstring(&mut env, &result_message) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to create result string",
            );
            std::ptr::null_mut()
        }
    }
}

/// Get configuration resolution information for debugging
#[no_mangle]
pub extern "system" fn Java_com_snapbase_SnapbaseWorkspace_nativeGetConfigInfo<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    let workspace_handle = unsafe { &mut *(handle as *mut WorkspaceHandle) };

    let resolution_info = match snapbase_core::config::get_config_resolution_info(Some(
        workspace_handle.workspace.root(),
    )) {
        Ok(info) => info,
        Err(e) => {
            let error_msg = format!("Failed to get config info: {e}");
            let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
            return std::ptr::null_mut();
        }
    };

    let info_json = serde_json::json!({
        "config_source": resolution_info.config_source,
        "config_path": resolution_info.config_path,
        "workspace_path": resolution_info.workspace_path,
        "resolution_order": resolution_info.resolution_order
    });

    let json_string = match serde_json::to_string_pretty(&info_json) {
        Ok(s) => s,
        Err(e) => {
            let error_msg = format!("Failed to serialize config info: {e}");
            let _ = env.throw_new("com/snapbase/SnapbaseException", &error_msg);
            return std::ptr::null_mut();
        }
    };

    match string_to_jstring(&mut env, &json_string) {
        Ok(jstr) => jstr.into_raw(),
        Err(_) => {
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                "Failed to create result string",
            );
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

/// Convert QueryResult to Arrow RecordBatch with proper type preservation
fn query_result_to_arrow(
    result: &snapbase_core::query::QueryResult,
) -> SnapbaseResult<arrow::record_batch::RecordBatch> {
    use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    // Determine column types by examining the first non-null value in each column
    let mut column_types: Vec<DataType> = vec![DataType::Utf8; result.columns.len()];

    for row in &result.rows {
        for (col_idx, value) in row.iter().enumerate() {
            if column_types[col_idx] == DataType::Utf8 {
                // Only update if we haven't determined the type yet (still default string)
                match value {
                    snapbase_core::query::QueryValue::Integer(_) => {
                        column_types[col_idx] = DataType::Int64;
                    }
                    snapbase_core::query::QueryValue::Float(_) => {
                        column_types[col_idx] = DataType::Float64;
                    }
                    snapbase_core::query::QueryValue::Boolean(_) => {
                        column_types[col_idx] = DataType::Boolean;
                    }
                    snapbase_core::query::QueryValue::String(_) => {
                        // Keep as Utf8 (default)
                    }
                    snapbase_core::query::QueryValue::Null => {
                        // Skip nulls when determining type
                    }
                }
            }
        }
    }

    // Build schema with proper data types
    let fields: Vec<Field> = result
        .columns
        .iter()
        .zip(column_types.iter())
        .map(|(name, data_type)| Field::new(name, data_type.clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Create column builders for each column based on its determined type
    let mut column_data: Vec<Vec<snapbase_core::query::QueryValue>> =
        vec![vec![]; result.columns.len()];

    // Collect all values for each column
    for row in &result.rows {
        for (col_idx, value) in row.iter().enumerate() {
            column_data[col_idx].push(value.clone());
        }
    }

    // Build Arrow arrays based on column types
    let arrays: Vec<ArrayRef> = column_types
        .iter()
        .enumerate()
        .map(|(col_idx, col_type)| {
            let column_values = &column_data[col_idx];

            match col_type {
                DataType::Int64 => {
                    let int_values: Vec<Option<i64>> = column_values
                        .iter()
                        .map(|v| match v {
                            snapbase_core::query::QueryValue::Integer(i) => Some(*i),
                            snapbase_core::query::QueryValue::Null => None,
                            _ => None, // Should not happen if type detection worked
                        })
                        .collect();
                    Arc::new(Int64Array::from(int_values)) as ArrayRef
                }
                DataType::Float64 => {
                    let float_values: Vec<Option<f64>> = column_values
                        .iter()
                        .map(|v| match v {
                            snapbase_core::query::QueryValue::Float(f) => Some(*f),
                            snapbase_core::query::QueryValue::Null => None,
                            _ => None, // Should not happen if type detection worked
                        })
                        .collect();
                    Arc::new(Float64Array::from(float_values)) as ArrayRef
                }
                DataType::Boolean => {
                    let bool_values: Vec<Option<bool>> = column_values
                        .iter()
                        .map(|v| match v {
                            snapbase_core::query::QueryValue::Boolean(b) => Some(*b),
                            snapbase_core::query::QueryValue::Null => None,
                            _ => None, // Should not happen if type detection worked
                        })
                        .collect();
                    Arc::new(BooleanArray::from(bool_values)) as ArrayRef
                }
                DataType::Utf8 => {
                    let string_values: Vec<Option<String>> = column_values
                        .iter()
                        .map(|v| match v {
                            snapbase_core::query::QueryValue::String(s) => Some(s.clone()),
                            snapbase_core::query::QueryValue::Null => None,
                            _ => None, // Should not happen if type detection worked
                        })
                        .collect();
                    Arc::new(StringArray::from(string_values)) as ArrayRef
                }
                _ => {
                    // Fallback to string for unknown types
                    let string_values: Vec<Option<String>> = column_values
                        .iter()
                        .map(|v| match v {
                            snapbase_core::query::QueryValue::String(s) => Some(s.clone()),
                            snapbase_core::query::QueryValue::Integer(i) => Some(i.to_string()),
                            snapbase_core::query::QueryValue::Float(f) => Some(f.to_string()),
                            snapbase_core::query::QueryValue::Boolean(b) => Some(b.to_string()),
                            snapbase_core::query::QueryValue::Null => None,
                        })
                        .collect();
                    Arc::new(StringArray::from(string_values)) as ArrayRef
                }
            }
        })
        .collect();

    RecordBatch::try_new(schema, arrays).map_err(|e| {
        snapbase_core::error::SnapbaseError::data_processing(format!(
            "Failed to create Arrow batch: {e}"
        ))
    })
}

/// Export Arrow RecordBatch to FFI structures for Java consumption
fn export_arrow_to_ffi(
    env: &mut JNIEnv,
    record_batch: arrow::record_batch::RecordBatch,
    array_ptr: jlong,
    schema_ptr: jlong,
) {
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
            let _ = env.throw_new(
                "com/snapbase/SnapbaseException",
                format!("Failed to export to FFI: {e}"),
            );
        }
    }
}

/// Create a Hive snapshot (adapted from CLI implementation)
fn create_hive_snapshot(
    workspace: &SnapbaseWorkspace,
    input_path: &Path,
    source_name: &str,
    snapshot_name: &str,
) -> SnapbaseResult<SnapshotMetadata> {
    use chrono::Utc;
    use snapbase_core::data::DataProcessor;
    use snapbase_core::path_utils;

    // Create timestamp
    let timestamp = Utc::now();
    let timestamp_str = timestamp.format("%Y%m%dT%H%M%S%.6fZ").to_string();

    // Create Hive directory structure path
    let hive_path_str = path_utils::join_for_storage_backend(
        &[
            "sources",
            source_name,
            &format!("snapshot_name={snapshot_name}"),
            &format!("snapshot_timestamp={timestamp_str}"),
        ],
        workspace.storage(),
    );

    // Use async runtime to handle storage backend operations
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async { workspace.storage().ensure_directory(&hive_path_str).await })?;

    // Process data with workspace-configured processor
    let mut processor = DataProcessor::new_with_workspace(workspace)?;
    let data_info = processor.load_file(input_path)?;

    // Create Parquet file using DuckDB COPY
    let parquet_relative_path = format!("{hive_path_str}/data.parquet");
    let parquet_path = workspace.storage().get_duckdb_path(&parquet_relative_path);

    // Export to Parquet using the same method as CLI
    let temp_path = std::path::Path::new(&parquet_path);
    processor.export_to_parquet(temp_path)?;

    // Create metadata
    let metadata = SnapshotMetadata {
        format_version: "1.0.0".to_string(),
        name: snapshot_name.to_string(),
        created: timestamp,
        source: input_path.to_string_lossy().to_string(),
        row_count: data_info.row_count,
        column_count: data_info.columns.len(),
        columns: data_info.columns.clone(),
        archive_size: None,
        parent_snapshot: None,
        sequence_number: 0,
        delta_from_parent: None,
        can_reconstruct_parent: false,
        source_path: Some(input_path.to_string_lossy().to_string()),
    };

    let metadata_json = serde_json::to_string_pretty(&metadata)?;
    let metadata_path = format!("{hive_path_str}/metadata.json");

    // Write metadata using storage backend
    let metadata_bytes = metadata_json.as_bytes();
    rt.block_on(async {
        workspace
            .storage()
            .write_file(&metadata_path, metadata_bytes)
            .await
    })?;

    Ok(metadata)
}
