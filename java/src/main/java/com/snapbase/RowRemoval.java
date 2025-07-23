package com.snapbase;

import java.util.Map;

/**
 * Represents a row removal.
 */
public class RowRemoval {
    private final long rowIndex;
    private final Map<String, String> data;
    
    public RowRemoval(long rowIndex, Map<String, String> data) {
        this.rowIndex = rowIndex;
        this.data = data;
    }
    
    public long getRowIndex() {
        return rowIndex;
    }
    
    public Map<String, String> getData() {
        return data;
    }
    
    @Override
    public String toString() {
        return "RowRemoval{" +
                "rowIndex=" + rowIndex +
                ", data=" + data +
                '}';
    }
}