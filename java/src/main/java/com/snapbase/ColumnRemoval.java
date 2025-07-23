package com.snapbase;

/**
 * Represents a column removal.
 */
public class ColumnRemoval {
    private final String name;
    private final String dataType;
    private final int position;
    private final boolean nullable;
    
    public ColumnRemoval(String name, String dataType, int position, boolean nullable) {
        this.name = name;
        this.dataType = dataType;
        this.position = position;
        this.nullable = nullable;
    }
    
    public String getName() {
        return name;
    }
    
    public String getDataType() {
        return dataType;
    }
    
    public int getPosition() {
        return position;
    }
    
    public boolean isNullable() {
        return nullable;
    }
    
    @Override
    public String toString() {
        return "ColumnRemoval{" +
                "name='" + name + '\'' +
                ", dataType='" + dataType + '\'' +
                ", position=" + position +
                ", nullable=" + nullable +
                '}';
    }
}