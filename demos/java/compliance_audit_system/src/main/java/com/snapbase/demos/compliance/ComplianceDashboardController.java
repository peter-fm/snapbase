package com.snapbase.demos.compliance;

import com.snapbase.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.FieldVector;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Web controller for the Compliance Audit Dashboard
 */
@Controller
public class ComplianceDashboardController {
    
    private static final String WORKSPACE_PATH = System.getProperty("user.dir") + "/compliance_workspace";
    private static final String CUSTOMER_DATA_FILE = "customer_data.csv";
    
    private SnapbaseWorkspace workspace;
    private boolean workspaceInitialized = false;
    private String workspaceError = null;
    
    public ComplianceDashboardController() {
        // Don't initialize workspace in constructor to avoid Spring Boot startup issues
        System.out.println("DEBUG: ComplianceDashboardController constructor called");
    }
    
    private synchronized SnapbaseWorkspace getWorkspace() {
        System.out.println("DEBUG: getWorkspace() called, workspaceInitialized=" + workspaceInitialized);
        if (!workspaceInitialized) {
            try {
                System.out.println("DEBUG: Attempting to create SnapbaseWorkspace with path: " + WORKSPACE_PATH);
                this.workspace = new SnapbaseWorkspace(WORKSPACE_PATH);
                this.workspaceError = null;
                System.out.println("DEBUG: SnapbaseWorkspace created successfully");
            } catch (Exception e) {
                System.err.println("DEBUG: Failed to initialize workspace: " + e.getMessage());
                e.printStackTrace();
                this.workspace = null;
                this.workspaceError = e.getMessage();
            }
            workspaceInitialized = true;
        }
        System.out.println("DEBUG: Returning workspace: " + (workspace != null ? "not null" : "null"));
        return workspace;
    }
    
    private boolean isWorkspaceAvailable() {
        return getWorkspace() != null;
    }
    
    @GetMapping("/")
    public String dashboard(Model model) {
        System.out.println("DEBUG: dashboard() method called");
        try {
            // Check if demo data exists first
            System.out.println("DEBUG: Checking if data exists at: " + WORKSPACE_PATH + "/" + CUSTOMER_DATA_FILE);
            boolean dataExists = Files.exists(Paths.get(WORKSPACE_PATH, CUSTOMER_DATA_FILE));
            System.out.println("DEBUG: dataExists = " + dataExists);
            model.addAttribute("dataExists", dataExists);
            
            if (!isWorkspaceAvailable()) {
                // Workspace failed to initialize due to Arrow issues
                model.addAttribute("totalSnapshots", 0);
                model.addAttribute("snapshots", Collections.emptyList());
                model.addAttribute("workspaceStats", "{}");
                model.addAttribute("totalRecords", 0);
                model.addAttribute("error", "Workspace initialization failed. This may be due to missing JVM arguments for Apache Arrow support. Try running the dashboard with the provided script: ./run-dashboard.sh");
            } else if (dataExists) {
                // Get basic workspace statistics only if data exists and workspace is available
                try {
                    List<String> snapshots = getWorkspace().listSnapshots();
                    model.addAttribute("totalSnapshots", snapshots.size());
                    model.addAttribute("snapshots", snapshots);
                    
                    // Get workspace stats if available
                    try {
                        String statsJson = getWorkspace().stats();
                        model.addAttribute("workspaceStats", statsJson);
                    } catch (Exception e) {
                        model.addAttribute("workspaceStats", "{}");
                    }
                    
                    if (!snapshots.isEmpty()) {
                        // Skip query operations for now to avoid Arrow errors
                        model.addAttribute("totalRecords", "Available");
                    }
                } catch (Exception e) {
                    model.addAttribute("error", "Failed to load workspace data: " + e.getMessage());
                }
            } else {
                // No data exists yet - set default values
                model.addAttribute("totalSnapshots", 0);
                model.addAttribute("snapshots", Collections.emptyList());
                model.addAttribute("workspaceStats", "{}");
                model.addAttribute("totalRecords", 0);
            }
            
        } catch (Exception e) {
            System.err.println("DEBUG: Exception in dashboard() method: " + e.getMessage());
            e.printStackTrace();
            model.addAttribute("error", "Failed to load dashboard: " + e.getMessage());
        }
        
        System.out.println("DEBUG: dashboard() method returning 'dashboard'");
        return "dashboard";
    }
    
    @GetMapping("/snapshots")
    public String snapshots(Model model) {
        System.out.println("DEBUG: snapshots() method called");
        try {
            // Check if demo data exists first
            boolean dataExists = Files.exists(Paths.get(WORKSPACE_PATH, CUSTOMER_DATA_FILE));
            System.out.println("DEBUG: dataExists = " + dataExists);
            
            if (!isWorkspaceAvailable()) {
                System.out.println("DEBUG: Workspace not available in snapshots()");
                model.addAttribute("snapshots", Collections.emptyList());
                model.addAttribute("error", "Workspace not available. This may be due to missing JVM arguments for Apache Arrow support.");
            } else if (dataExists) {
                System.out.println("DEBUG: Getting snapshots list");
                List<String> snapshots = getWorkspace().listSnapshots();
                System.out.println("DEBUG: Found snapshots: " + snapshots);
                List<SnapshotInfo> snapshotInfos = new ArrayList<>();
                
                for (String snapshot : snapshots) {
                    System.out.println("DEBUG: Processing snapshot: " + snapshot);
                    // Skip queries for now to avoid Arrow errors, just show snapshot names
                    snapshotInfos.add(new SnapshotInfo(snapshot, null));
                }
                
                System.out.println("DEBUG: Created " + snapshotInfos.size() + " SnapshotInfo objects");
                model.addAttribute("snapshots", snapshotInfos);
            } else {
                // No data exists yet
                System.out.println("DEBUG: No data exists, setting empty snapshots");
                model.addAttribute("snapshots", Collections.emptyList());
                model.addAttribute("message", "No snapshots available. Run the CLI demo first to generate sample data.");
            }
            
        } catch (Exception e) {
            System.err.println("DEBUG: Exception in snapshots() method: " + e.getMessage());
            e.printStackTrace();
            model.addAttribute("error", "Failed to load snapshots: " + e.getMessage());
        }
        
        System.out.println("DEBUG: snapshots() method returning 'snapshots'");
        return "snapshots";
    }
    
    @GetMapping("/changes")
    public String changes(@RequestParam(required = false) String from,
                         @RequestParam(required = false) String to,
                         Model model) {
        System.out.println("DEBUG: changes() method called with from=" + from + ", to=" + to);
        try {
            if (!isWorkspaceAvailable()) {
                System.out.println("DEBUG: Workspace not available in changes()");
                model.addAttribute("availableSnapshots", Collections.emptyList());
                model.addAttribute("error", "Workspace not available. Please ensure JVM arguments for Apache Arrow are configured.");
                return "changes";
            }
            
            List<String> snapshots = getWorkspace().listSnapshots();
            System.out.println("DEBUG: Available snapshots: " + snapshots);
            model.addAttribute("availableSnapshots", snapshots);
            
            if (from != null && to != null && !from.equals(to)) {
                System.out.println("DEBUG: Attempting to compare snapshots: " + from + " -> " + to);
                try {
                    ChangeDetectionResult changes = getWorkspace().diff(CUSTOMER_DATA_FILE, from, to);
                    System.out.println("DEBUG: diff() succeeded");
                    
                    model.addAttribute("fromSnapshot", from);
                    model.addAttribute("toSnapshot", to);
                    model.addAttribute("changes", changes);
                    
                    // Extract sensitive changes
                    List<SensitiveChange> sensitiveChanges = extractSensitiveChanges(changes);
                    System.out.println("DEBUG: Found " + sensitiveChanges.size() + " sensitive changes");
                    model.addAttribute("sensitiveChanges", sensitiveChanges);
                    
                } catch (Exception e) {
                    System.err.println("DEBUG: Exception in diff(): " + e.getMessage());
                    e.printStackTrace();
                    model.addAttribute("error", "Failed to compare snapshots: " + e.getMessage());
                }
            } else if (from != null) {
                System.out.println("DEBUG: Only 'from' parameter provided, no comparison");
            }
            
        } catch (Exception e) {
            System.err.println("DEBUG: Exception in changes() method: " + e.getMessage());
            e.printStackTrace();
            model.addAttribute("error", "Failed to load changes page: " + e.getMessage());
        }
        
        System.out.println("DEBUG: changes() method returning 'changes'");
        return "changes";
    }
    
    @GetMapping("/audit")
    public String audit(Model model) {
        System.out.println("DEBUG: audit() method called");
        
        Path auditLogPath = Paths.get(WORKSPACE_PATH, "audit_exports", "audit_log.csv");
        Path summaryPath = Paths.get(WORKSPACE_PATH, "audit_exports", "compliance_summary.txt");
        
        boolean auditLogExists = Files.exists(auditLogPath);
        boolean summaryExists = Files.exists(summaryPath);
        
        System.out.println("DEBUG: Audit log exists: " + auditLogExists + " at " + auditLogPath);
        System.out.println("DEBUG: Summary exists: " + summaryExists + " at " + summaryPath);
        
        model.addAttribute("auditLogExists", auditLogExists);
        model.addAttribute("summaryExists", summaryExists);
        model.addAttribute("auditEntries", Collections.emptyList());
        
        if (auditLogExists || summaryExists) {
            model.addAttribute("message", "Audit files are available for download. Generated by the CLI demo.");
        } else {
            model.addAttribute("message", "Run the CLI demo first to generate audit files: ./run-demo.sh");
        }
        
        System.out.println("DEBUG: audit() method returning 'audit'");
        return "audit";
    }
    
    @GetMapping("/query")
    public String query(Model model) {
        model.addAttribute("sampleQueries", getSampleQueries());
        return "query";
    }
    
    @PostMapping("/api/query")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> executeQuery(@RequestBody Map<String, String> request) {
        String sql = request.get("sql");
        
        if (sql == null || sql.trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "SQL query is required"));
        }
        
        if (!isWorkspaceAvailable()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Workspace not available. Please ensure JVM arguments for Apache Arrow are configured."));
        }
        
        try (VectorSchemaRoot result = getWorkspace().query(CUSTOMER_DATA_FILE, sql)) {
            Map<String, Object> response = new HashMap<>();
            response.put("rowCount", result.getRowCount());
            response.put("columns", result.getSchema().getFields().stream()
                .map(field -> field.getName())
                .collect(Collectors.toList()));
            
            // Convert first 100 rows to JSON for display
            List<Map<String, Object>> rows = new ArrayList<>();
            int maxRows = Math.min(result.getRowCount(), 100);
            
            for (int i = 0; i < maxRows; i++) {
                Map<String, Object> row = new HashMap<>();
                for (FieldVector vector : result.getFieldVectors()) {
                    String columnName = vector.getField().getName();
                    Object value = vector.isNull(i) ? null : vector.getObject(i);
                    
                    // Mask sensitive data
                    if (isSensitiveField(columnName)) {
                        value = maskSensitiveValue(columnName, value);
                    }
                    
                    row.put(columnName, value);
                }
                rows.add(row);
            }
            
            response.put("data", rows);
            response.put("truncated", result.getRowCount() > 100);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", "Query failed: " + e.getMessage()));
        }
    }
    
    @PostMapping("/api/run-demo")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> runDemo() {
        try {
            ComplianceAuditDemo demo = new ComplianceAuditDemo();
            
            // Run demo in a separate thread to avoid blocking
            new Thread(() -> {
                try {
                    demo.runDemo();
                } catch (Exception e) {
                    System.err.println("Demo execution failed: " + e.getMessage());
                }
            }).start();
            
            return ResponseEntity.ok(Map.of(
                "message", "Demo started successfully! Check the console output for progress.",
                "status", "running"
            ));
            
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to start demo: " + e.getMessage(),
                "status", "failed"
            ));
        }
    }
    
    private List<SensitiveChange> extractSensitiveChanges(ChangeDetectionResult changes) {
        List<SensitiveChange> sensitiveChanges = new ArrayList<>();
        Set<String> sensitiveFields = Set.of("ssn", "email", "phone", "date_of_birth", "address");
        
        for (RowModification modification : changes.getRowChanges().getModified()) {
            for (Map.Entry<String, CellChange> entry : modification.getChanges().entrySet()) {
                String column = entry.getKey();
                if (sensitiveFields.contains(column.toLowerCase())) {
                    CellChange cellChange = entry.getValue();
                    sensitiveChanges.add(new SensitiveChange(
                        (int) modification.getRowIndex(),
                        column,
                        maskSensitiveValue(column, cellChange.getBefore()),
                        maskSensitiveValue(column, cellChange.getAfter())
                    ));
                }
            }
        }
        
        return sensitiveChanges;
    }
    
    @GetMapping("/api/export/audit-log")
    public ResponseEntity<Resource> downloadAuditLog() {
        try {
            Path auditLogPath = Paths.get(WORKSPACE_PATH, "audit_exports", "audit_log.csv");
            
            if (!Files.exists(auditLogPath)) {
                return ResponseEntity.notFound().build();
            }
            
            Resource resource = new FileSystemResource(auditLogPath.toFile());
            
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=audit_log.csv");
            headers.add(HttpHeaders.CONTENT_TYPE, "text/csv");
            
            return ResponseEntity.ok()
                    .headers(headers)
                    .body(resource);
                    
        } catch (Exception e) {
            System.err.println("DEBUG: Failed to download audit log: " + e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
    
    @GetMapping("/api/export/compliance-summary")
    public ResponseEntity<Resource> downloadComplianceSummary() {
        try {
            Path summaryPath = Paths.get(WORKSPACE_PATH, "audit_exports", "compliance_summary.txt");
            
            if (!Files.exists(summaryPath)) {
                return ResponseEntity.notFound().build();
            }
            
            Resource resource = new FileSystemResource(summaryPath.toFile());
            
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=compliance_summary.txt");
            headers.add(HttpHeaders.CONTENT_TYPE, "text/plain");
            
            return ResponseEntity.ok()
                    .headers(headers)
                    .body(resource);
                    
        } catch (Exception e) {
            System.err.println("DEBUG: Failed to download compliance summary: " + e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
    
    @ExceptionHandler(Exception.class)
    public ModelAndView handleException(Exception e) {
        System.err.println("DEBUG: Global exception handler caught: " + e.getMessage());
        e.printStackTrace();
        
        ModelAndView mav = new ModelAndView("dashboard");
        mav.addObject("error", "Application error: " + e.getMessage());
        mav.addObject("dataExists", false);
        mav.addObject("totalSnapshots", 0);
        mav.addObject("snapshots", Collections.emptyList());
        mav.addObject("workspaceStats", "{}");
        mav.addObject("totalRecords", 0);
        return mav;
    }
    
    private boolean isSensitiveField(String columnName) {
        Set<String> sensitiveFields = Set.of("ssn", "email", "phone", "date_of_birth", "address");
        return sensitiveFields.contains(columnName.toLowerCase());
    }
    
    private Object maskSensitiveValue(String columnName, Object value) {
        if (value == null) return null;
        String strValue = value.toString();
        
        switch (columnName.toLowerCase()) {
            case "ssn":
                return strValue.length() >= 4 ? "***-**-" + strValue.substring(strValue.length() - 4) : "***-**-****";
            case "email":
                int atIndex = strValue.indexOf('@');
                return atIndex > 0 ? strValue.substring(0, 2) + "***" + strValue.substring(atIndex) : "***@***.com";
            case "phone":
                return strValue.length() >= 4 ? "***-***-" + strValue.substring(strValue.length() - 4) : "***-***-****";
            default:
                return strValue.length() > 10 ? strValue.substring(0, 10) + "..." : strValue;
        }
    }
    
    private List<String> getSampleQueries() {
        return Arrays.asList(
            "SELECT COUNT(*) FROM data",
            "SELECT snapshot_name, COUNT(*) as record_count FROM data GROUP BY snapshot_name",
            "SELECT * FROM data WHERE credit_score > 750 LIMIT 10",
            "SELECT AVG(annual_income) as avg_income FROM data GROUP BY snapshot_name",
            "SELECT DISTINCT snapshot_name FROM data ORDER BY snapshot_name"
        );
    }
    
    // Helper classes for the web interface
    public static class SnapshotInfo {
        private final String name;
        private final Long recordCount;
        
        public SnapshotInfo(String name, Long recordCount) {
            this.name = name;
            this.recordCount = recordCount;
        }
        
        public String getName() { return name; }
        public Long getRecordCount() { return recordCount; }
    }
    
    public static class SensitiveChange {
        private final int rowIndex;
        private final String column;
        private final Object oldValue;
        private final Object newValue;
        
        public SensitiveChange(int rowIndex, String column, Object oldValue, Object newValue) {
            this.rowIndex = rowIndex;
            this.column = column;
            this.oldValue = oldValue;
            this.newValue = newValue;
        }
        
        public int getRowIndex() { return rowIndex; }
        public String getColumn() { return column; }
        public Object getOldValue() { return oldValue; }
        public Object getNewValue() { return newValue; }
    }
}