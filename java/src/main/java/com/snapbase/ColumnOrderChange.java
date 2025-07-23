package com.snapbase;

import java.util.List;

/**
 * Represents a change in column order.
 */
public class ColumnOrderChange {
    private final List<String> before;
    private final List<String> after;
    
    public ColumnOrderChange(List<String> before, List<String> after) {
        this.before = before;
        this.after = after;
    }
    
    public List<String> getBefore() {
        return before;
    }
    
    public List<String> getAfter() {
        return after;
    }
    
    @Override
    public String toString() {
        return "ColumnOrderChange{" +
                "before=" + before +
                ", after=" + after +
                '}';
    }
}