package com.snapbase;

import java.util.Map;

/**
 * Represents a row modification.
 */
public class RowModification {
    private final long rowIndex;
    private final Map<String, CellChange> changes;
    
    public RowModification(long rowIndex, Map<String, CellChange> changes) {
        this.rowIndex = rowIndex;
        this.changes = changes;
    }
    
    public long getRowIndex() {
        return rowIndex;
    }
    
    public Map<String, CellChange> getChanges() {
        return changes;
    }
    
    @Override
    public String toString() {
        return "RowModification{" +
                "rowIndex=" + rowIndex +
                ", changes=" + changes +
                '}';
    }
}