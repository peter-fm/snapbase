package com.snapbase;

/**
 * Represents a column type change.
 */
public class TypeChange {
    private final String column;
    private final String from;
    private final String to;
    
    public TypeChange(String column, String from, String to) {
        this.column = column;
        this.from = from;
        this.to = to;
    }
    
    public String getColumn() {
        return column;
    }
    
    public String getFrom() {
        return from;
    }
    
    public String getTo() {
        return to;
    }
    
    @Override
    public String toString() {
        return "TypeChange{" +
                "column='" + column + '\'' +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                '}';
    }
}