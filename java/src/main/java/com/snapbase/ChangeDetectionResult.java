package com.snapbase;

/**
 * Result of change detection between two data snapshots.
 * Contains both schema-level and row-level changes.
 */
public class ChangeDetectionResult {
    private final SchemaChanges schemaChanges;
    private final RowChanges rowChanges;
    
    public ChangeDetectionResult(SchemaChanges schemaChanges, RowChanges rowChanges) {
        this.schemaChanges = schemaChanges;
        this.rowChanges = rowChanges;
    }
    
    public SchemaChanges getSchemaChanges() {
        return schemaChanges;
    }
    
    public RowChanges getRowChanges() {
        return rowChanges;
    }
    
    @Override
    public String toString() {
        return "ChangeDetectionResult{" +
                "schemaChanges=" + schemaChanges +
                ", rowChanges=" + rowChanges +
                '}';
    }
}