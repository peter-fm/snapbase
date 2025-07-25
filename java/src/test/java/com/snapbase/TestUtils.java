package com.snapbase;

/**
 * Utility class for test methods to generate unique snapshot names
 * and avoid conflicts between test runs.
 */
public class TestUtils {
    
    /**
     * Generate a unique snapshot name based on a base name and UUID.
     * This ensures tests don't conflict with each other due to duplicate snapshot names.
     * 
     * @param baseName The base name for the snapshot (e.g., "test", "baseline")
     * @return A unique snapshot name in format: baseName_randomId
     */
    public static String uniqueSnapshotName(String baseName) {
        String testId = java.util.UUID.randomUUID().toString().substring(0, 8);
        return baseName + "_" + testId;
    }
    
    /**
     * Generate a unique snapshot name with test method context.
     * Includes the test method name for better debugging.
     * 
     * @param baseName The base name for the snapshot
     * @param testMethodName The name of the test method
     * @return A unique snapshot name in format: baseName_methodName_randomId
     */
    public static String uniqueSnapshotName(String baseName, String testMethodName) {
        String testId = java.util.UUID.randomUUID().toString().substring(0, 8);
        return baseName + "_" + testMethodName + "_" + testId;
    }
}