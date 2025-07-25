package com.snapbase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Tests for configuration context resolution in Java API.
 * 
 * These tests verify that when a workspace is created with an explicit path,
 * it uses the configuration from that workspace directory, not the current
 * directory or global configuration.
 */
public class ConfigContextTest {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Path originalWorkingDir;
    
    @BeforeEach
    void setUp() {
        originalWorkingDir = Paths.get("").toAbsolutePath();
    }
    
    @AfterEach 
    void tearDown() {
        // Restore original working directory
        System.setProperty("user.dir", originalWorkingDir.toString());
    }
    
    @Test
    void testWorkspaceUsesOwnConfigNotCurrentDirectory(@TempDir Path tempDir) throws Exception {
        // Setup: Create parent directory with one config
        Path parentDir = tempDir.resolve("parent");
        Files.createDirectories(parentDir);
        
        Path parentConfig = parentDir.resolve("snapbase.toml");
        Files.writeString(parentConfig, 
            "[storage]\n" +
            "backend = \"local\"\n" + 
            "path = \"parent_snapbase\"\n");
        
        // Setup: Create child directory with different config
        Path childDir = parentDir.resolve("project1");
        Files.createDirectories(childDir);
        
        Path childConfig = childDir.resolve("snapbase.toml");
        Files.writeString(childConfig,
            "[storage]\n" +
            "backend = \"local\"\n" +
            "path = \"child_snapbase\"\n");
        
        // Change working directory to parent (simulate script running from parent)
        System.setProperty("user.dir", parentDir.toString());
        
        // Create workspace pointing to child directory
        try (SnapbaseWorkspace workspace = new SnapbaseWorkspace(childDir)) {
            
            // Get config resolution information
            String configInfoJson = workspace.getConfigInfo();
            JsonNode configInfo = objectMapper.readTree(configInfoJson);
            
            System.out.println("Config info: " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(configInfo));
            
            // Verify workspace config is being used
            assertEquals("workspace", configInfo.get("config_source").asText(),
                "Should use workspace config, not parent directory config");
                
            assertTrue(configInfo.get("config_path").asText().contains("project1"),
                "Config path should reference child directory: " + configInfo.get("config_path").asText());
                
            assertEquals(childConfig.toString(), configInfo.get("config_path").asText(),
                "Should use exact child config file path");
                
            assertEquals(childDir.toString(), configInfo.get("workspace_path").asText(),
                "Workspace path should be child directory");
        }
    }
    
    @Test
    void testWorkspaceConfigIsolation(@TempDir Path tempDir) throws Exception {
        // Create two separate workspace directories
        Path ws1Dir = tempDir.resolve("workspace1");
        Path ws2Dir = tempDir.resolve("workspace2");
        Files.createDirectories(ws1Dir);
        Files.createDirectories(ws2Dir);
        
        // Different configs for each workspace
        Files.writeString(ws1Dir.resolve("snapbase.toml"),
            "[storage]\nbackend = \"local\"\npath = \"ws1_storage\"\n");
            
        Files.writeString(ws2Dir.resolve("snapbase.toml"),
            "[storage]\nbackend = \"local\"\npath = \"ws2_storage\"\n");
        
        // Change to neutral directory
        System.setProperty("user.dir", tempDir.toString());
        
        try (SnapbaseWorkspace ws1 = new SnapbaseWorkspace(ws1Dir);
             SnapbaseWorkspace ws2 = new SnapbaseWorkspace(ws2Dir)) {
            
            String config1Json = ws1.getConfigInfo();
            String config2Json = ws2.getConfigInfo();
            
            JsonNode config1 = objectMapper.readTree(config1Json);
            JsonNode config2 = objectMapper.readTree(config2Json);
            
            // Each workspace should use its own config
            assertNotEquals(config1.get("config_path").asText(), config2.get("config_path").asText(),
                "Workspaces should use different config files");
                
            assertTrue(config1.get("config_path").asText().contains("workspace1"),
                "WS1 should use workspace1 config");
                
            assertTrue(config2.get("config_path").asText().contains("workspace2"),
                "WS2 should use workspace2 config");
        }
    }
    
    @Test
    void testWorkspaceOperationsUseWorkspaceConfig(@TempDir Path tempDir) throws Exception {
        // Create workspace directory with specific config
        Path workspaceDir = tempDir.resolve("operation_test");
        Files.createDirectories(workspaceDir);
        
        Path configFile = workspaceDir.resolve("snapbase.toml");
        Files.writeString(configFile,
            "[storage]\n" +
            "backend = \"local\"\n" +
            "path = \"operation_storage\"\n" +
            "\n" +
            "[snapshot]\n" +
            "default_name_pattern = \"java_test_{seq}\"\n");
        
        // Create test data file
        Path testData = workspaceDir.resolve("test.csv");
        Files.writeString(testData, "id,name,value\n1,test,100\n2,demo,200\n");
        
        // Run from different directory to test context
        System.setProperty("user.dir", tempDir.toString());
        
        try (SnapbaseWorkspace workspace = new SnapbaseWorkspace(workspaceDir)) {
            
            // Verify config is correct
            String configInfoJson = workspace.getConfigInfo();
            JsonNode configInfo = objectMapper.readTree(configInfoJson);
            
            assertEquals("workspace", configInfo.get("config_source").asText());
            assertTrue(configInfo.get("config_path").asText().contains("operation_test"));
            
            // Initialize workspace
            workspace.init();
            
            // Test snapshot creation - should use workspace config context
            String result = workspace.createSnapshot(testData.toString(), "test_snapshot");
            assertTrue(result.contains("test_snapshot"), "Should create snapshot with given name");
            
            // Verify snapshot exists
            assertTrue(workspace.snapshotExists("test_snapshot"), "Snapshot should exist in workspace context");
            
            // Verify workspace path is correct
            String workspacePath = workspace.getPath();
            assertTrue(workspacePath.contains("operation_test"), 
                "Workspace path should reference correct directory: " + workspacePath);
        }
    }
    
    @Test 
    void testRelativeWorkspacePathConfigResolution(@TempDir Path tempDir) throws Exception {
        // Create workspace subdirectory
        Path subDir = tempDir.resolve("relative_test");
        Files.createDirectories(subDir);
        
        Path configFile = subDir.resolve("snapbase.toml");
        Files.writeString(configFile,
            "[storage]\nbackend = \"local\"\npath = \"relative_storage\"\n");
        
        // Change to parent directory
        System.setProperty("user.dir", tempDir.toString());
        
        // Test both relative and absolute paths
        try (SnapbaseWorkspace workspaceRel = new SnapbaseWorkspace(Paths.get("relative_test"));
             SnapbaseWorkspace workspaceAbs = new SnapbaseWorkspace(subDir)) {
            
            String configRelJson = workspaceRel.getConfigInfo();
            String configAbsJson = workspaceAbs.getConfigInfo();
            
            JsonNode configRel = objectMapper.readTree(configRelJson);
            JsonNode configAbs = objectMapper.readTree(configAbsJson);
            
            // Both should use workspace config
            assertEquals("workspace", configRel.get("config_source").asText());
            assertEquals("workspace", configAbs.get("config_source").asText());
            
            // Both should resolve to the same config file
            Path relConfigPath = Paths.get(configRel.get("config_path").asText()).toAbsolutePath();
            Path absConfigPath = Paths.get(configAbs.get("config_path").asText()).toAbsolutePath();
            
            assertEquals(relConfigPath, absConfigPath, 
                "Relative and absolute paths should resolve to same config file");
        }
    }
}