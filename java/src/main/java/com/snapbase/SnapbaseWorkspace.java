package com.snapbase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Java API for Snapbase workspace operations.
 * 
 * This class provides Java bindings for the Snapbase core library,
 * allowing snapshot creation, change detection, and querying operations
 * from Java applications.
 * 
 * Example usage:
 * <pre>
 * try (SnapbaseWorkspace workspace = new SnapbaseWorkspace("/path/to/workspace")) {
 *     workspace.init();
 *     String result = workspace.createSnapshot("data.csv", "v1");
 *     String changes = workspace.status("data.csv", "v1");
 * }
 * </pre>
 */
public class SnapbaseWorkspace implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(SnapbaseWorkspace.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    
    // Native handle to the Rust workspace
    private long nativeHandle;
    private final ExecutorService executor;
    private final Path workspacePath;
    private final BufferAllocator allocator;
    
    static {
        loadNativeLibrary();
    }
    
    /**
     * Create a new workspace at the specified path.
     * 
     * @param workspacePath Path to the workspace directory
     * @throws SnapbaseException if workspace creation fails
     */
    public SnapbaseWorkspace(String workspacePath) throws SnapbaseException {
        this(Paths.get(workspacePath));
    }
    
    /**
     * Create a new workspace at the specified path.
     * 
     * @param workspacePath Path to the workspace directory
     * @throws SnapbaseException if workspace creation fails
     */
    public SnapbaseWorkspace(Path workspacePath) throws SnapbaseException {
        this.workspacePath = workspacePath;
        this.allocator = new RootAllocator();
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "snapbase-async");
            t.setDaemon(true);
            return t;
        });
        
        this.nativeHandle = nativeCreateWorkspace(workspacePath.toString());
        if (this.nativeHandle == 0) {
            throw new SnapbaseException("Failed to create workspace");
        }
    }
    
    /**
     * Initialize the workspace (creates config and directory structure).
     * 
     * @throws SnapbaseException if initialization fails
     */
    public void init() throws SnapbaseException {
        checkHandle();
        nativeInit(nativeHandle);
    }
    
    /**
     * Create a snapshot of the given file.
     * 
     * @param filePath Path to the file to snapshot
     * @return Result message indicating success and snapshot metadata
     * @throws SnapbaseException if snapshot creation fails
     */
    public String createSnapshot(String filePath) throws SnapbaseException {
        return createSnapshot(filePath, null);
    }
    
    /**
     * Create a snapshot of the given file with a specific name.
     * 
     * @param filePath Path to the file to snapshot
     * @param name Optional snapshot name (auto-generated if null)
     * @return Result message indicating success and snapshot metadata
     * @throws SnapbaseException if snapshot creation fails
     */
    public String createSnapshot(String filePath, String name) throws SnapbaseException {
        checkHandle();
        return nativeCreateSnapshot(nativeHandle, filePath, name);
    }
    
    /**
     * Create a snapshot asynchronously.
     * 
     * @param filePath Path to the file to snapshot
     * @param name Optional snapshot name (auto-generated if null)
     * @return CompletableFuture that completes with the result message
     */
    public CompletableFuture<String> createSnapshotAsync(String filePath, String name) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return createSnapshot(filePath, name);
            } catch (SnapbaseException e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }
    
    /**
     * Check status of current file against a baseline snapshot.
     * 
     * @param filePath Path to the current file
     * @param baseline Name of the baseline snapshot
     * @return JSON string containing status information
     * @throws SnapbaseException if status check fails
     */
    public String status(String filePath, String baseline) throws SnapbaseException {
        checkHandle();
        return nativeStatus(nativeHandle, filePath, baseline);
    }
    
    /**
     * Check status of current file against a baseline snapshot.
     * 
     * @param filePath Path to the current file
     * @param baseline Name of the baseline snapshot
     * @return Parsed status as a JsonNode
     * @throws SnapbaseException if status check fails
     */
    public JsonNode statusAsJson(String filePath, String baseline) throws SnapbaseException {
        String statusJson = status(filePath, baseline);
        try {
            return objectMapper.readTree(statusJson);
        } catch (IOException e) {
            throw new SnapbaseException("Failed to parse status JSON: " + e.getMessage(), e);
        }
    }
    
    /**
     * Query historical snapshots using SQL with zero-copy Arrow return.
     * 
     * @param source Source file or pattern
     * @param sql SQL query to execute
     * @return VectorSchemaRoot containing query results with zero-copy performance
     * @throws SnapbaseException if query fails
     */
    public VectorSchemaRoot query(String source, String sql) throws SnapbaseException {
        return query(source, sql, null);
    }
    
    /**
     * Query historical snapshots using SQL with a limit and zero-copy Arrow return.
     * 
     * @param source Source file or pattern
     * @param sql SQL query to execute
     * @param limit Optional limit on number of results
     * @return VectorSchemaRoot containing query results with zero-copy performance
     * @throws SnapbaseException if query fails
     */
    public VectorSchemaRoot query(String source, String sql, Integer limit) throws SnapbaseException {
        checkHandle();
        
        // Apply limit if specified
        String finalSql = (limit != null) ? sql + " LIMIT " + limit : sql;
        
        // Allocate Arrow C structures for zero-copy data transfer
        try (ArrowArray arrowArray = ArrowArray.allocateNew(allocator);
             ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)) {
            
            // Call native method with C structure pointers
            nativeQueryArrow(nativeHandle, source, finalSql, 
                           arrowArray.memoryAddress(), 
                           arrowSchema.memoryAddress());
            
            // Import from C Data Interface to Java VectorSchemaRoot
            return Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null);
        }
    }
    
    /**
     * Query historical snapshots and get row count efficiently.
     * 
     * @param source Source file or pattern
     * @param sql SQL query to execute
     * @return Number of rows in the result
     * @throws SnapbaseException if query fails
     */
    public int queryRowCount(String source, String sql) throws SnapbaseException {
        try (VectorSchemaRoot result = query(source, sql)) {
            return result.getRowCount();
        }
    }
    
    /**
     * Query historical snapshots and access data by column name.
     * 
     * @param source Source file or pattern
     * @param sql SQL query to execute
     * @param columnName Column name to access
     * @return FieldVector for the specified column
     * @throws SnapbaseException if query fails or column not found
     */
    public FieldVector queryColumn(String source, String sql, String columnName) throws SnapbaseException {
        VectorSchemaRoot result = query(source, sql);
        FieldVector column = result.getVector(columnName);
        if (column == null) {
            result.close();
            throw new SnapbaseException("Column not found: " + columnName);
        }
        return column;
    }
    
    /**
     * Get the workspace path.
     * 
     * @return Path to the workspace directory
     * @throws SnapbaseException if operation fails
     */
    public String getPath() throws SnapbaseException {
        checkHandle();
        return nativeGetPath(nativeHandle);
    }
    
    /**
     * List all snapshots in the workspace.
     * 
     * @return List of snapshot names
     * @throws SnapbaseException if operation fails
     */
    public List<String> listSnapshots() throws SnapbaseException {
        checkHandle();
        return nativeListSnapshots(nativeHandle);
    }
    
    /**
     * List snapshots for a specific source.
     * 
     * @param sourcePath Path to the source file
     * @return List of snapshot names for the source
     * @throws SnapbaseException if operation fails
     */
    public List<String> listSnapshotsForSource(String sourcePath) throws SnapbaseException {
        checkHandle();
        return nativeListSnapshotsForSource(nativeHandle, sourcePath);
    }
    
    /**
     * Check if a snapshot exists.
     * 
     * @param name Snapshot name to check
     * @return true if snapshot exists, false otherwise
     * @throws SnapbaseException if operation fails
     */
    public boolean snapshotExists(String name) throws SnapbaseException {
        checkHandle();
        return nativeSnapshotExists(nativeHandle, name);
    }
    
    /**
     * Get workspace statistics.
     * 
     * @return JSON string containing workspace statistics
     * @throws SnapbaseException if operation fails
     */
    public String stats() throws SnapbaseException {
        checkHandle();
        return nativeStats(nativeHandle);
    }
    
    /**
     * Get workspace statistics as JsonNode.
     * 
     * @return Parsed statistics as a JsonNode
     * @throws SnapbaseException if operation fails
     */
    public JsonNode statsAsJson() throws SnapbaseException {
        String statsJson = stats();
        try {
            return objectMapper.readTree(statsJson);
        } catch (IOException e) {
            throw new SnapbaseException("Failed to parse stats JSON: " + e.getMessage(), e);
        }
    }
    
    /**
     * Compare two snapshots.
     * 
     * @param source Source file or pattern
     * @param fromSnapshot Name of the from snapshot
     * @param toSnapshot Name of the to snapshot
     * @return JSON string containing diff information
     * @throws SnapbaseException if diff operation fails
     */
    public String diff(String source, String fromSnapshot, String toSnapshot) throws SnapbaseException {
        checkHandle();
        return nativeDiff(nativeHandle, source, fromSnapshot, toSnapshot);
    }
    
    /**
     * Compare two snapshots and return results as JsonNode.
     * 
     * @param source Source file or pattern
     * @param fromSnapshot Name of the from snapshot
     * @param toSnapshot Name of the to snapshot
     * @return Parsed diff results as a JsonNode
     * @throws SnapbaseException if diff operation fails
     */
    public JsonNode diffAsJson(String source, String fromSnapshot, String toSnapshot) throws SnapbaseException {
        String diffJson = diff(source, fromSnapshot, toSnapshot);
        try {
            return objectMapper.readTree(diffJson);
        } catch (IOException e) {
            throw new SnapbaseException("Failed to parse diff JSON: " + e.getMessage(), e);
        }
    }
    
    /**
     * Export snapshot data to a file.
     * 
     * @param source Source file or pattern
     * @param outputFile Output file path (format determined by extension: .csv or .parquet)
     * @param toSnapshot Name of the snapshot to export
     * @return Result message indicating success and export details
     * @throws SnapbaseException if export operation fails
     */
    public String export(String source, String outputFile, String toSnapshot) throws SnapbaseException {
        return export(source, outputFile, toSnapshot, false);
    }
    
    /**
     * Export snapshot data to a file with force option.
     * 
     * @param source Source file or pattern
     * @param outputFile Output file path (format determined by extension: .csv or .parquet)
     * @param toSnapshot Name of the snapshot to export
     * @param force Skip confirmation prompts and overwrite existing files
     * @return Result message indicating success and export details
     * @throws SnapbaseException if export operation fails
     */
    public String export(String source, String outputFile, String toSnapshot, boolean force) throws SnapbaseException {
        checkHandle();
        return nativeExport(nativeHandle, source, outputFile, toSnapshot, force);
    }
    
    /**
     * Export snapshot data to a file asynchronously.
     * 
     * @param source Source file or pattern
     * @param outputFile Output file path (format determined by extension: .csv or .parquet)
     * @param toSnapshot Name of the snapshot to export
     * @param force Skip confirmation prompts and overwrite existing files
     * @return CompletableFuture that completes with the result message
     */
    public CompletableFuture<String> exportAsync(String source, String outputFile, String toSnapshot, boolean force) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return export(source, outputFile, toSnapshot, force);
            } catch (SnapbaseException e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }
    
    /**
     * Close the workspace and free native resources.
     */
    @Override
    public void close() {
        if (nativeHandle != 0) {
            nativeClose(nativeHandle);
            nativeHandle = 0;
        }
        allocator.close();
        executor.shutdown();
    }
    
    private void checkHandle() throws SnapbaseException {
        if (nativeHandle == 0) {
            throw new SnapbaseException("Workspace has been closed");
        }
    }
    
    private static void loadNativeLibrary() {
        try {
            // Try to load from system library path first
            System.loadLibrary("snapbase_java");
            logger.info("Loaded native library from system path");
        } catch (UnsatisfiedLinkError e) {
            try {
                // Try to load from JAR resources
                loadNativeLibraryFromJar();
            } catch (Exception e2) {
                try {
                    // Fall back to build location
                    String osName = System.getProperty("os.name").toLowerCase();
                    String libExtension;
                    if (osName.contains("win")) {
                        libExtension = ".dll";
                    } else if (osName.contains("mac")) {
                        libExtension = ".dylib";
                    } else {
                        libExtension = ".so";
                    }
                    
                    String libPath = System.getProperty("user.dir") + "/../target/release/libsnapbase_java" + libExtension;
                    System.load(libPath);
                    logger.info("Loaded native library from build location: {}", libPath);
                } catch (UnsatisfiedLinkError e3) {
                    logger.error("Failed to load native library from all locations", e3);
                    throw new RuntimeException("Failed to load snapbase native library. " +
                        "Make sure the JAR includes the native library or build it with: cargo build --release --features jni", e3);
                }
            }
        }
    }
    
    private static void loadNativeLibraryFromJar() throws Exception {
        String osName = System.getProperty("os.name").toLowerCase();
        String libName;
        if (osName.contains("win")) {
            libName = "libsnapbase_java.dll";
        } else if (osName.contains("mac")) {
            libName = "libsnapbase_java.dylib";
        } else {
            libName = "libsnapbase_java.so";
        }
        
        // Extract from JAR to temp file and load
        try (var input = SnapbaseWorkspace.class.getResourceAsStream("/" + libName)) {
            if (input == null) {
                throw new RuntimeException("Native library not found in JAR: " + libName);
            }
            
            // Create temp file
            java.io.File tempFile = java.io.File.createTempFile("libsnapbase_java", 
                osName.contains("win") ? ".dll" : (osName.contains("mac") ? ".dylib" : ".so"));
            tempFile.deleteOnExit();
            
            // Copy from JAR to temp file
            try (var output = new java.io.FileOutputStream(tempFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = input.read(buffer)) != -1) {
                    output.write(buffer, 0, bytesRead);
                }
            }
            
            // Load the temp file
            System.load(tempFile.getAbsolutePath());
            logger.info("Loaded native library from JAR: {}", tempFile.getAbsolutePath());
        }
    }
    
    // Native method declarations
    private static native long nativeCreateWorkspace(String workspacePath) throws SnapbaseException;
    private static native void nativeInit(long handle) throws SnapbaseException;
    private static native String nativeCreateSnapshot(long handle, String filePath, String name) throws SnapbaseException;
    private static native String nativeStatus(long handle, String filePath, String baseline) throws SnapbaseException;
    private static native void nativeQueryArrow(long handle, String source, String sql, long arrayPtr, long schemaPtr) throws SnapbaseException;
    private static native String nativeGetPath(long handle) throws SnapbaseException;
    private static native List<String> nativeListSnapshots(long handle) throws SnapbaseException;
    private static native List<String> nativeListSnapshotsForSource(long handle, String sourcePath) throws SnapbaseException;
    private static native boolean nativeSnapshotExists(long handle, String name) throws SnapbaseException;
    private static native String nativeStats(long handle) throws SnapbaseException;
    private static native String nativeDiff(long handle, String source, String fromSnapshot, String toSnapshot) throws SnapbaseException;
    private static native String nativeExport(long handle, String source, String outputFile, String toSnapshot, boolean force) throws SnapbaseException;
    private static native void nativeClose(long handle);
}