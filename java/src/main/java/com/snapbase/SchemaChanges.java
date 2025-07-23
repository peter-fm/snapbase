package com.snapbase;

import java.util.List;

/**
 * Schema-level changes between two data snapshots.
 */
public class SchemaChanges {
    private final ColumnOrderChange columnOrder;
    private final List<ColumnAddition> columnsAdded;
    private final List<ColumnRemoval> columnsRemoved;
    private final List<ColumnRename> columnsRenamed;
    private final List<TypeChange> typeChanges;
    
    public SchemaChanges(
            ColumnOrderChange columnOrder,
            List<ColumnAddition> columnsAdded,
            List<ColumnRemoval> columnsRemoved,
            List<ColumnRename> columnsRenamed,
            List<TypeChange> typeChanges) {
        this.columnOrder = columnOrder;
        this.columnsAdded = columnsAdded;
        this.columnsRemoved = columnsRemoved;
        this.columnsRenamed = columnsRenamed;
        this.typeChanges = typeChanges;
    }
    
    public ColumnOrderChange getColumnOrder() {
        return columnOrder;
    }
    
    public List<ColumnAddition> getColumnsAdded() {
        return columnsAdded;
    }
    
    public List<ColumnRemoval> getColumnsRemoved() {
        return columnsRemoved;
    }
    
    public List<ColumnRename> getColumnsRenamed() {
        return columnsRenamed;
    }
    
    public List<TypeChange> getTypeChanges() {
        return typeChanges;
    }
    
    /**
     * Check if there are any schema changes.
     */
    public boolean hasChanges() {
        return columnOrder != null
                || !columnsAdded.isEmpty()
                || !columnsRemoved.isEmpty()
                || !columnsRenamed.isEmpty()
                || !typeChanges.isEmpty();
    }
    
    @Override
    public String toString() {
        return "SchemaChanges{" +
                "columnOrder=" + columnOrder +
                ", columnsAdded=" + columnsAdded +
                ", columnsRemoved=" + columnsRemoved +
                ", columnsRenamed=" + columnsRenamed +
                ", typeChanges=" + typeChanges +
                '}';
    }
}