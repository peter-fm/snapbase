package com.snapbase;

/**
 * Represents a cell-level change.
 */
public class CellChange {
    private final String before;
    private final String after;
    
    public CellChange(String before, String after) {
        this.before = before;
        this.after = after;
    }
    
    public String getBefore() {
        return before;
    }
    
    public String getAfter() {
        return after;
    }
    
    @Override
    public String toString() {
        return "CellChange{" +
                "before='" + before + '\'' +
                ", after='" + after + '\'' +
                '}';
    }
}