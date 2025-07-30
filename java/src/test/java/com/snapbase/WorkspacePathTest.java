package com.snapbase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Tests for workspace path resolution - ensuring Java bindings work correctly
 * when explicit workspace paths are provided, independent of current working directory.
 */
public class WorkspacePathTest {
    
    private String originalUserDir;
    
    @BeforeEach
    void saveCurrentDirectory() {
        originalUserDir = System.getProperty("user.dir");
    }
    
    @AfterEach
    void restoreCurrentDirectory() {
        System.setProperty("user.dir", originalUserDir);
    }
    
    private String normalizePathForComparison(String path) {
        try {
            return Paths.get(path).toRealPath().toString();
        } catch (IOException e) {
            return Paths.get(path).toAbsolutePath().normalize().toString();
        }
    }
    
    @Test
    void testWorkspaceWithExplicitSubdirectoryPath(@TempDir Path tempDir) throws Exception {
        // Change to temp directory
        System.setProperty("user.dir", tempDir.toString());
        
        Path workspacePath = tempDir.resolve("myproject");
        
        try (SnapbaseWorkspace workspace = new SnapbaseWorkspace(workspacePath.toString())) {
            // Verify workspace path is correct
            String actualPath = normalizePathForComparison(workspace.getPath());
            String expectedPath = normalizePathForComparison(workspacePath.toString());
            assertEquals(expectedPath, actualPath);
            
            // Initialize should create directory structure
            workspace.init();
            assertTrue(Files.exists(workspacePath));
            assertTrue(Files.exists(workspacePath.resolve(".snapbase")));
            
            // Test snapshot creation in correct location
            Path testCsv = workspacePath.resolve("test.csv");
            Files.writeString(testCsv, "id,name\n1,test\n");
            String result = workspace.createSnapshot("test.csv", "subdirectory_test");
            assertTrue(result.contains("Created snapshot 'subdirectory_test'"));
        }
    }
    
    @Test
    void testSnapshotCreationWithExplicitWorkspacePathAndCwdChange(@TempDir Path tempDir) throws Exception {
        // Setup directories (simulate demo structure)
        Path scriptDir = tempDir.resolve("script_location");
        Path workspaceDir = scriptDir.resolve("my_workspace");
        Files.createDirectories(scriptDir);
        
        // Change to script directory (where demo script runs from)
        System.setProperty("user.dir", scriptDir.toString());
        
        try (SnapbaseWorkspace workspace = new SnapbaseWorkspace("my_workspace")) {
            workspace.init();
            
            // Verify workspace was created in correct location
            String actualPath = normalizePathForComparison(workspace.getPath());
            String expectedPath = normalizePathForComparison(workspaceDir.toString());
            assertEquals(expectedPath, actualPath);
            assertTrue(Files.exists(workspaceDir.resolve(".snapbase")));
            
            // Create a test CSV file in the workspace directory
            Path testCsv = workspaceDir.resolve("test_data.csv");
            Files.writeString(testCsv, "id,name,value\n1,test,100\n2,demo,200\n");
            
            // Create snapshot (the key test - should go to workspace dir)
            String result = workspace.createSnapshot("test_data.csv", "test_snapshot");
            assertTrue(result.contains("Created snapshot 'test_snapshot'"));
            
            // Verify snapshot was created in workspace directory, NOT current directory
            Path workspaceSnapbase = workspaceDir.resolve(".snapbase");
            Path scriptSnapbase = scriptDir.resolve(".snapbase");
            
            // Should exist in workspace
            assertTrue(Files.exists(workspaceSnapbase), "Snapshot should be in workspace directory");
            
            // Should NOT exist in script directory OR should be empty
            if (Files.exists(scriptSnapbase)) {
                // If it exists, it should be empty (no snapshot data)
                boolean hasSnapshots = Files.walk(scriptSnapbase)
                    .anyMatch(path -> path.toString().contains("snapshot_name="));
                assertFalse(hasSnapshots, "Should not find snapshots in script directory");
            }
            
            // Verify snapshot exists in correct location
            boolean snapshotExists = Files.walk(workspaceSnapbase)
                .anyMatch(path -> path.toString().contains("snapshot_name=test_snapshot"));
            assertTrue(snapshotExists, "Snapshot should exist in workspace .snapbase directory");
            
            // Test querying works from the workspace
            try (var result_vsr = workspace.query(
                "SELECT COUNT(*) as count FROM test_data_csv WHERE snapshot_name = 'test_snapshot'")) {
                assertTrue(result_vsr.getRowCount() > 0, "Should be able to query snapshot from workspace");
            }
        }
    }
    
    @Test
    void testWorkspaceWithRelativePath(@TempDir Path tempDir) throws Exception {
        System.setProperty("user.dir", tempDir.toString());
        
        try (SnapbaseWorkspace workspace = new SnapbaseWorkspace("./subproject")) {
            String actualPath = normalizePathForComparison(workspace.getPath());
            String expectedPath = normalizePathForComparison(tempDir.resolve("subproject").toString());
            assertEquals(expectedPath, actualPath);
        }
    }
    
    @Test
    void testWorkspaceWithAbsolutePath(@TempDir Path tempDir) throws Exception {
        Path projectPath = tempDir.resolve("absolute_project");
        
        try (SnapbaseWorkspace workspace = new SnapbaseWorkspace(projectPath.toString())) {
            String actualPath = normalizePathForComparison(workspace.getPath());
            String expectedPath = normalizePathForComparison(projectPath.toString());
            assertEquals(expectedPath, actualPath);
        }
    }
    
    @Test
    void testWorkspaceIgnoresExistingParentWorkspace(@TempDir Path tempDir) throws Exception {
        System.setProperty("user.dir", tempDir.toString());
        
        // Create a workspace in the current directory
        try (SnapbaseWorkspace parentWorkspace = new SnapbaseWorkspace(tempDir.toString())) {
            parentWorkspace.init();
            assertTrue(Files.exists(tempDir.resolve(".snapbase")));
        }
        
        // Now create a workspace in a subdirectory - should NOT use parent
        String childDir = "child_project";
        try (SnapbaseWorkspace childWorkspace = new SnapbaseWorkspace(childDir)) {
            String actualPath = normalizePathForComparison(childWorkspace.getPath());
            String expectedPath = normalizePathForComparison(tempDir.resolve(childDir).toString());
            String parentPath = normalizePathForComparison(tempDir.toString());
            
            // This is the key test - should be child path, not parent path
            assertEquals(expectedPath, actualPath);
            assertNotEquals(parentPath, actualPath);
            
            // Initialize child workspace
            childWorkspace.init();
            assertTrue(Files.exists(tempDir.resolve(childDir).resolve(".snapbase")));
        }
    }
    
    @Test
    void testMultipleWorkspacesInSubdirectories(@TempDir Path tempDir) throws Exception {
        System.setProperty("user.dir", tempDir.toString());
        
        // Create workspaces in different subdirs
        try (SnapbaseWorkspace ws1 = new SnapbaseWorkspace("project1");
             SnapbaseWorkspace ws2 = new SnapbaseWorkspace("project2");
             SnapbaseWorkspace ws3 = new SnapbaseWorkspace("nested/project3")) {
            
            // Verify paths are correct
            String path1 = normalizePathForComparison(ws1.getPath());
            String path2 = normalizePathForComparison(ws2.getPath());
            String path3 = normalizePathForComparison(ws3.getPath());
            
            String expected1 = normalizePathForComparison(tempDir.resolve("project1").toString());
            String expected2 = normalizePathForComparison(tempDir.resolve("project2").toString());
            String expected3 = normalizePathForComparison(tempDir.resolve("nested/project3").toString());
            
            assertEquals(expected1, path1);
            assertEquals(expected2, path2);
            assertEquals(expected3, path3);
            
            // All should be different
            assertNotEquals(path1, path2);
            assertNotEquals(path2, path3);
            assertNotEquals(path1, path3);
        }
    }
    
    @Test 
    void testBugRegressionExplicitPathNotTraversingUp(@TempDir Path tempDir) throws Exception {
        System.setProperty("user.dir", tempDir.toString());
        
        // Create existing workspace in current directory
        try (SnapbaseWorkspace existingWs = new SnapbaseWorkspace(tempDir.toString())) {
            existingWs.init();
            String existingPath = normalizePathForComparison(existingWs.getPath());
            
            // Create workspace with explicit subdirectory - should NOT find the existing one
            try (SnapbaseWorkspace newWs = new SnapbaseWorkspace("subproject")) {
                String newPath = normalizePathForComparison(newWs.getPath());
                String expectedNewPath = normalizePathForComparison(tempDir.resolve("subproject").toString());
                
                // Key assertion: new workspace should be in subdirectory, not existing location
                assertNotEquals(existingPath, newPath);
                assertEquals(expectedNewPath, newPath);
                assertEquals(normalizePathForComparison(tempDir.toString()), existingPath);
            }
        }
    }
    
    @Test
    void testBugRegressionBasicFunctionalityWorks(@TempDir Path tempDir) throws Exception {
        System.setProperty("user.dir", tempDir.toString());
        
        // This is the exact use case that was failing
        try (SnapbaseWorkspace workspace = new SnapbaseWorkspace("myworkspace")) {
            // Should NOT return empty string or current directory
            String path = workspace.getPath();
            assertNotNull(path);
            assertFalse(path.isEmpty(), "Should not return empty string");
            
            String normalizedPath = normalizePathForComparison(path);
            String tempDirPath = normalizePathForComparison(tempDir.toString());
            String expectedPath = normalizePathForComparison(tempDir.resolve("myworkspace").toString());
            
            assertNotEquals(tempDirPath, normalizedPath, "Should not return current directory");
            assertEquals(expectedPath, normalizedPath, "Should be subdirectory");
        }
    }
}