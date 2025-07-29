package com.snapbase.demos.compliance;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Audit logger for tracking all data modifications and system activities
 * for compliance reporting and regulatory requirements.
 */
public class AuditLogger {
    
    private final List<AuditEntry> auditEntries;
    private final DateTimeFormatter timestampFormat;
    
    public AuditLogger() {
        this.auditEntries = new ArrayList<>();
        this.timestampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    }
    
    /**
     * Log an audit event
     */
    public void log(String user, String action, String details) {
        AuditEntry entry = new AuditEntry(
            LocalDateTime.now(),
            user,
            action,
            details
        );
        auditEntries.add(entry);
        
        // Print to console for demo visibility
        System.out.println("   [AUDIT] " + entry.getTimestamp().format(timestampFormat) + 
                          " | " + user + " | " + action + " | " + details);
    }
    
    /**
     * Export audit log to CSV file for regulatory compliance
     */
    public void exportToFile(Path filePath) throws IOException {
        try (CSVPrinter csvPrinter = new CSVPrinter(
                Files.newBufferedWriter(filePath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING),
                CSVFormat.DEFAULT.withHeader("timestamp", "user", "action", "details"))) {
            
            for (AuditEntry entry : auditEntries) {
                csvPrinter.printRecord(
                    entry.getTimestamp().format(timestampFormat),
                    entry.getUser(),
                    entry.getAction(),
                    entry.getDetails()
                );
            }
        }
    }
    
    /**
     * Get all audit entries for programmatic access
     */
    public List<AuditEntry> getAllEntries() {
        return new ArrayList<>(auditEntries);
    }
    
    /**
     * Get audit entries for a specific user
     */
    public List<AuditEntry> getEntriesForUser(String user) {
        return auditEntries.stream()
            .filter(entry -> entry.getUser().equals(user))
            .collect(java.util.stream.Collectors.toList());
    }
    
    /**
     * Get audit entries containing specific keywords
     */
    public List<AuditEntry> searchEntries(String keyword) {
        String lowerKeyword = keyword.toLowerCase();
        return auditEntries.stream()
            .filter(entry -> 
                entry.getAction().toLowerCase().contains(lowerKeyword) ||
                entry.getDetails().toLowerCase().contains(lowerKeyword))
            .collect(java.util.stream.Collectors.toList());
    }
    
    /**
     * Clear all audit entries
     */
    public void clear() {
        auditEntries.clear();
    }
    
    /**
     * Get audit statistics
     */
    public AuditStatistics getStatistics() {
        return new AuditStatistics(auditEntries);
    }
    
    /**
     * Individual audit entry
     */
    public static class AuditEntry {
        private final LocalDateTime timestamp;
        private final String user;
        private final String action;
        private final String details;
        
        public AuditEntry(LocalDateTime timestamp, String user, String action, String details) {
            this.timestamp = timestamp;
            this.user = user;
            this.action = action;
            this.details = details;
        }
        
        public LocalDateTime getTimestamp() {
            return timestamp;
        }
        
        public String getUser() {
            return user;
        }
        
        public String getAction() {
            return action;
        }
        
        public String getDetails() {
            return details;
        }
        
        @Override
        public String toString() {
            return "AuditEntry{" +
                    "timestamp=" + timestamp +
                    ", user='" + user + '\'' +
                    ", action='" + action + '\'' +
                    ", details='" + details + '\'' +
                    '}';
        }
    }
    
    /**
     * Audit statistics for reporting
     */
    public static class AuditStatistics {
        private final int totalEntries;
        private final long uniqueUsers;
        private final long sensitiveOperations;
        private final LocalDateTime firstEntry;
        private final LocalDateTime lastEntry;
        
        public AuditStatistics(List<AuditEntry> entries) {
            this.totalEntries = entries.size();
            this.uniqueUsers = entries.stream()
                .map(AuditEntry::getUser)
                .distinct()
                .count();
            this.sensitiveOperations = entries.stream()
                .filter(entry -> entry.getAction().toUpperCase().contains("SENSITIVE"))
                .count();
            this.firstEntry = entries.stream()
                .map(AuditEntry::getTimestamp)
                .min(LocalDateTime::compareTo)
                .orElse(null);
            this.lastEntry = entries.stream()
                .map(AuditEntry::getTimestamp)
                .max(LocalDateTime::compareTo)
                .orElse(null);
        }
        
        public int getTotalEntries() {
            return totalEntries;
        }
        
        public long getUniqueUsers() {
            return uniqueUsers;
        }
        
        public long getSensitiveOperations() {
            return sensitiveOperations;
        }
        
        public LocalDateTime getFirstEntry() {
            return firstEntry;
        }
        
        public LocalDateTime getLastEntry() {
            return lastEntry;
        }
        
        @Override
        public String toString() {
            return "AuditStatistics{" +
                    "totalEntries=" + totalEntries +
                    ", uniqueUsers=" + uniqueUsers +
                    ", sensitiveOperations=" + sensitiveOperations +
                    ", firstEntry=" + firstEntry +
                    ", lastEntry=" + lastEntry +
                    '}';
        }
    }
}