package com.snapbase;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test specifically for snapshot name validation functionality
 */
class SnapshotValidationTest {
    
    @TempDir
    Path tempDir;
    
    private SnapbaseWorkspace workspace;
    
    // Utility method to generate unique snapshot names to avoid conflicts
    private String uniqueSnapshotName(String baseName) {
        String testId = java.util.UUID.randomUUID().toString().substring(0, 8);
        return baseName + "_" + testId;
    }
    
    @BeforeEach
    void setUp() throws SnapbaseException {
        workspace = new SnapbaseWorkspace(tempDir.toString());
        workspace.init();
    }
    
    @AfterEach
    void tearDown() {
        if (workspace != null) {
            workspace.close();
        }
    }
    
    @Test
    @DisplayName("Test duplicate snapshot name validation works correctly")
    void testDuplicateSnapshotNameValidation() throws IOException, SnapbaseException {
        // Create test data file
        Path testFile = tempDir.resolve("test.csv");
        String testData = "id,name\n1,Alice\n2,Bob\n";
        Files.write(testFile, testData.getBytes(StandardCharsets.UTF_8));
        
        // Use unique name to avoid conflicts with other tests
        String snapshotName = uniqueSnapshotName("validation_test");
        
        // Create first snapshot - should succeed
        String result1 = workspace.createSnapshot("test.csv", snapshotName);
        assertNotNull(result1);
        assertTrue(result1.contains(snapshotName));
        assertTrue(workspace.snapshotExists(snapshotName));
        
        // Attempt to create second snapshot with same name - should fail
        SnapbaseException exception = assertThrows(SnapbaseException.class, () -> {
            workspace.createSnapshot("test.csv", snapshotName);
        });
        
        assertTrue(exception.getMessage().contains("already exists"));
        assertTrue(exception.getMessage().contains(snapshotName));
        System.out.println("✅ Snapshot validation test completed successfully");
    }
    
    @Test
    @DisplayName("Test unique snapshot names work correctly")
    void testUniqueSnapshotNamesWork() throws IOException, SnapbaseException {
        // Create test data file
        Path testFile = tempDir.resolve("test.csv");
        String testData = "id,name\n1,Alice\n2,Bob\n";
        Files.write(testFile, testData.getBytes(StandardCharsets.UTF_8));
        
        // Create multiple snapshots with unique names - all should succeed
        String snapshot1 = uniqueSnapshotName("unique_test_1");
        String snapshot2 = uniqueSnapshotName("unique_test_2");
        String snapshot3 = uniqueSnapshotName("unique_test_3");
        
        String result1 = workspace.createSnapshot("test.csv", snapshot1);
        assertNotNull(result1);
        assertTrue(workspace.snapshotExists(snapshot1));
        
        String result2 = workspace.createSnapshot("test.csv", snapshot2);
        assertNotNull(result2);
        assertTrue(workspace.snapshotExists(snapshot2));
        
        String result3 = workspace.createSnapshot("test.csv", snapshot3);
        assertNotNull(result3);
        assertTrue(workspace.snapshotExists(snapshot3));
        
        System.out.println("✅ Unique snapshot names test completed successfully");
    }
}