package com.snapbase;

/**
 * Represents a column addition.
 */
public class ColumnAddition {
    private final String name;
    private final String dataType;
    private final int position;
    private final boolean nullable;
    private final String defaultValue;
    
    public ColumnAddition(String name, String dataType, int position, boolean nullable, String defaultValue) {
        this.name = name;
        this.dataType = dataType;
        this.position = position;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
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
    
    public String getDefaultValue() {
        return defaultValue;
    }
    
    @Override
    public String toString() {
        return "ColumnAddition{" +
                "name='" + name + '\'' +
                ", dataType='" + dataType + '\'' +
                ", position=" + position +
                ", nullable=" + nullable +
                ", defaultValue='" + defaultValue + '\'' +
                '}';
    }
}