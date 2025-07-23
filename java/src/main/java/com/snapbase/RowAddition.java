package com.snapbase;

import java.util.Map;

/**
 * Represents a row addition.
 */
public class RowAddition {
    private final long rowIndex;
    private final Map<String, String> data;
    
    public RowAddition(long rowIndex, Map<String, String> data) {
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
        return "RowAddition{" +
                "rowIndex=" + rowIndex +
                ", data=" + data +
                '}';
    }
}