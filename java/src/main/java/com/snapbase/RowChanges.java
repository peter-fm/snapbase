package com.snapbase;

import java.util.List;

/**
 * Row-level changes between two data snapshots.
 */
public class RowChanges {
    private final List<RowModification> modified;
    private final List<RowAddition> added;
    private final List<RowRemoval> removed;
    
    public RowChanges(
            List<RowModification> modified,
            List<RowAddition> added,
            List<RowRemoval> removed) {
        this.modified = modified;
        this.added = added;
        this.removed = removed;
    }
    
    public List<RowModification> getModified() {
        return modified;
    }
    
    public List<RowAddition> getAdded() {
        return added;
    }
    
    public List<RowRemoval> getRemoved() {
        return removed;
    }
    
    /**
     * Check if there are any row changes.
     */
    public boolean hasChanges() {
        return !modified.isEmpty() || !added.isEmpty() || !removed.isEmpty();
    }
    
    /**
     * Get total number of changed rows.
     */
    public int getTotalChanges() {
        return modified.size() + added.size() + removed.size();
    }
    
    @Override
    public String toString() {
        return "RowChanges{" +
                "modified=" + modified +
                ", added=" + added +
                ", removed=" + removed +
                '}';
    }
}