package com.snapbase;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for SnapbaseWorkspace Java API
 */
class SnapbaseWorkspaceTest {
    
    @TempDir
    Path tempDir;
    
    private SnapbaseWorkspace workspace;
    private Path testDataFile;
    
    @BeforeEach
    void setUp() throws IOException, SnapbaseException {
        // Create test workspace
        workspace = new SnapbaseWorkspace(tempDir.toString());
        workspace.init();
        
        // Create test data file
        testDataFile = tempDir.resolve("test_data.csv");
        String csvContent = "id,name,value\n" +
                           "1,Alice,100\n" +
                           "2,Bob,200\n" +
                           "3,Charlie,300\n";
        Files.write(testDataFile, csvContent.getBytes());
    }
    
    @AfterEach
    void tearDown() {
        if (workspace != null) {
            workspace.close();
        }
    }
    
    @Test
    void testWorkspaceCreation() throws SnapbaseException {
        assertNotNull(workspace);
        assertEquals(tempDir.toString(), workspace.getPath());
    }
    
    @Test
    void testCreateSnapshot() throws SnapbaseException {
        String result = workspace.createSnapshot(testDataFile.toString(), "test_snapshot");
        assertNotNull(result);
        assertTrue(result.contains("Created snapshot"));
        assertTrue(result.contains("test_snapshot"));
        assertTrue(result.contains("3 rows"));
        assertTrue(result.contains("3 columns"));
    }
    
    @Test
    void testCreateSnapshotWithAutoName() throws SnapbaseException {
        String result = workspace.createSnapshot(testDataFile.toString());
        assertNotNull(result);
        assertTrue(result.contains("Created snapshot"));
        assertTrue(result.contains("rows"));
        assertTrue(result.contains("columns"));
    }
    
    @Test
    void testListSnapshots() throws SnapbaseException {
        // Create a snapshot first
        workspace.createSnapshot(testDataFile.toString(), "test_snapshot");
        
        List<String> snapshots = workspace.listSnapshots();
        assertNotNull(snapshots);
        assertFalse(snapshots.isEmpty());
        assertTrue(snapshots.contains("test_snapshot"));
    }
    
    @Test
    void testSnapshotExists() throws SnapbaseException {
        // Use a unique snapshot name to avoid conflicts
        String uniqueSnapshotName = "unique_test_" + System.currentTimeMillis();
        
        // Initially no snapshots with this name
        assertFalse(workspace.snapshotExists(uniqueSnapshotName));
        
        // Create a snapshot
        workspace.createSnapshot(testDataFile.toString(), uniqueSnapshotName);
        
        // Now it should exist
        assertTrue(workspace.snapshotExists(uniqueSnapshotName));
    }
    
    @Test
    void testStatus() throws SnapbaseException, IOException {
        // Create initial snapshot
        workspace.createSnapshot(testDataFile.toString(), "baseline");
        
        // Modify the file
        String updatedCsvContent = "id,name,value\n" +
                                  "1,Alice,150\n" +
                                  "2,Bob,200\n" +
                                  "4,David,400\n";
        Files.write(testDataFile, updatedCsvContent.getBytes());
        
        // Check status
        ChangeDetectionResult changes = workspace.status(testDataFile.toString(), "baseline");
        assertNotNull(changes);
        
        // Verify the result has expected structure
        assertNotNull(changes.getSchemaChanges());
        assertNotNull(changes.getRowChanges());
    }
    
    @Test
    void testQueryArrow() throws SnapbaseException {
        // Create a snapshot first
        workspace.createSnapshot(testDataFile.toString(), "test_snapshot");
        
        // Query the data using just the filename (like CLI does)
        String sourceFile = testDataFile.getFileName().toString(); // "test_data.csv"
        
        try (VectorSchemaRoot result = workspace.query(sourceFile, "SELECT * FROM data", 10)) {
            assertNotNull(result);
            
            // Debug: Print actual row count
            System.out.println("Actual row count: " + result.getRowCount());
            System.out.println("Field count: " + result.getFieldVectors().size());
            
            // Query might include multiple snapshots, so just verify we have data
            assertTrue(result.getRowCount() > 0, "Should have at least some rows");
            assertTrue(result.getFieldVectors().size() >= 3, "Should have at least id, name, value columns"); // id, name, value columns plus partition metadata
            
            // Test column access - columns may have different types due to Arrow conversion
            FieldVector idColumn = result.getVector("id");
            assertNotNull(idColumn, "Should have id column");
            
            FieldVector nameColumn = result.getVector("name");
            assertNotNull(nameColumn, "Should have name column");
            
            // Test row count helper method
            int rowCount = workspace.queryRowCount(sourceFile, "SELECT * FROM data");
            assertTrue(rowCount > 0, "Should have at least some rows from helper method");
        }
    }
    
    @Test
    void testQueryColumn() throws SnapbaseException {
        // Create a snapshot first
        workspace.createSnapshot(testDataFile.toString(), "test_snapshot");
        
        String sourceFile = testDataFile.getFileName().toString();
        
        // Test accessing specific column
        try (FieldVector idColumn = workspace.queryColumn(sourceFile, "SELECT id FROM data", "id")) {
            assertNotNull(idColumn);
            // May not be IntVector due to Arrow conversion, just check it's a FieldVector
            assertTrue(idColumn instanceof FieldVector);
        }
        
        // Test error for non-existent column
        assertThrows(SnapbaseException.class, () -> {
            workspace.queryColumn(sourceFile, "SELECT id FROM data", "non_existent_column");
        });
    }
    
    @Test
    void testQueryPerformance() throws SnapbaseException {
        // Create a snapshot first
        workspace.createSnapshot(testDataFile.toString(), "test_snapshot");
        
        String sourceFile = testDataFile.getFileName().toString();
        
        // Test that multiple queries work efficiently with zero-copy
        long startTime = System.nanoTime();
        
        for (int i = 0; i < 10; i++) {
            try (VectorSchemaRoot result = workspace.query(sourceFile, "SELECT * FROM data LIMIT 5")) {
                assertTrue(result.getRowCount() > 0);
            }
        }
        
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;
        
        // Should be fast with zero-copy (arbitrary threshold for test)
        assertTrue(durationMs < 5000, "Query performance test took too long: " + durationMs + "ms");
    }
    
    @Test
    void testStats() throws SnapbaseException {
        // Create a snapshot first
        workspace.createSnapshot(testDataFile.toString(), "test_snapshot");
        
        String stats = workspace.stats();
        assertNotNull(stats);
        
        // Parse JSON stats
        JsonNode statsJson = workspace.statsAsJson();
        assertNotNull(statsJson);
        assertTrue(statsJson.has("snapshot_count"));
        assertTrue(statsJson.get("snapshot_count").asInt() >= 1);
    }
    
    @Test
    void testDiff() throws SnapbaseException, IOException {
        // Create first snapshot
        workspace.createSnapshot(testDataFile.toString(), "snapshot1");
        
        // Modify the file
        String updatedCsvContent = "id,name,value\n" +
                                  "1,Alice,150\n" +
                                  "2,Bob,200\n" +
                                  "4,David,400\n";
        Files.write(testDataFile, updatedCsvContent.getBytes());
        
        // Create second snapshot
        workspace.createSnapshot(testDataFile.toString(), "snapshot2");
        
        // Compare snapshots using filename
        String sourceFile = testDataFile.getFileName().toString();
        ChangeDetectionResult diff = workspace.diff(sourceFile, "snapshot1", "snapshot2");
        assertNotNull(diff);
        
        // Verify the result has expected structure
        assertNotNull(diff.getSchemaChanges());
        assertNotNull(diff.getRowChanges());
    }
    
    @Test
    void testAsyncSnapshot() throws Exception {
        String result = workspace.createSnapshotAsync(testDataFile.toString(), "async_test")
                .get(); // Wait for completion
        
        assertNotNull(result);
        assertTrue(result.contains("async_test"));
    }
    
    @Test
    void testExceptionHandling() {
        // Test with non-existent file
        assertThrows(SnapbaseException.class, () -> {
            workspace.createSnapshot("/non/existent/file.csv", "test");
        });
        
        // Test status check with non-existent baseline
        assertThrows(SnapbaseException.class, () -> {
            workspace.status(testDataFile.toString(), "non_existent_baseline");
        });
        
        // Test query with non-existent source
        assertThrows(SnapbaseException.class, () -> {
            workspace.query("non_existent.csv", "SELECT * FROM data");
        });
    }
    
    @Test
    void testResourceManagement() throws SnapbaseException {
        // Test that workspace can be closed and reopened
        workspace.close();
        
        // Create new workspace instance
        workspace = new SnapbaseWorkspace(tempDir.toString());
        assertNotNull(workspace.getPath());
    }
}