package com.snapbase;

/**
 * Represents a column rename.
 */
public class ColumnRename {
    private final String from;
    private final String to;
    
    public ColumnRename(String from, String to) {
        this.from = from;
        this.to = to;
    }
    
    public String getFrom() {
        return from;
    }
    
    public String getTo() {
        return to;
    }
    
    @Override
    public String toString() {
        return "ColumnRename{" +
                "from='" + from + '\'' +
                ", to='" + to + '\'' +
                '}';
    }
}