# Enterprise Data Compliance Audit System Demo

This demo showcases how **Snapbase** can be used for regulatory compliance and data auditing by tracking every change to customer PII data with full audit trails. It demonstrates Java's enterprise capabilities combined with Snapbase's unique temporal data features to solve real-world compliance problems that traditional databases handle poorly.

## What This Demo Demonstrates

### 🔒 **Enterprise Compliance Features**
- **Cell-level audit trails**: Track every individual PII change with before/after values
- **Compliance reporting**: Generate reports showing data modifications over time  
- **Sensitive field alerts**: Automated detection of changes to SSN, email, phone, etc.
- **Temporal queries**: SQL analysis across historical snapshots
- **Export capabilities**: Generate compliance reports for regulatory requirements
- **Role-based access simulation**: Different user types making different changes

### 🏢 **Real-World Use Cases**
- **GDPR Compliance**: Right to be forgotten via data snapshots
- **HIPAA Compliance**: Complete audit trail of PHI (Protected Health Information) access
- **SOX Compliance**: Financial data change documentation for Sarbanes-Oxley
- **PCI DSS**: Payment card data handling audit trails

### 🚀 **Why Snapbase + Java?**
This combination is uniquely powerful because:
- **Traditional databases** lose historical context and require complex audit log systems
- **Snapbase** provides immutable, queryable snapshots with built-in change detection
- **Java's enterprise features** provide security, structured data handling, and robust APIs
- **Zero-copy Arrow performance** enables fast processing of large audit datasets

## Quick Start

### Prerequisites
- Java 11 or higher
- Maven 3.6 or higher  
- Snapbase Java library (fat jar with all dependencies included)

### 1. Build and Install Snapbase Java Library

First, build the Snapbase Java library from the parent project:

```bash
# From the project root
cd java && mvn clean install
```

This installs the Snapbase Java library to your local Maven repository.

### 2. Run the CLI Demo

```bash
cd demos/compliance_audit_system

# Run the CLI demo using the provided script (recommended)
./run-demo.sh

# Alternative: Run directly with Maven (requires JVM arguments)
mvn clean compile
export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED"
mvn exec:java -Dexec.mainClass="com.snapbase.demos.compliance.ComplianceAuditDemo"
```

**Note**: The demo showcases all core compliance features including:
- ✅ Customer data generation and PII tracking
- ✅ Snapshot creation and versioning  
- ✅ Change detection between snapshots
- ✅ Audit logging with user attribution
- ✅ Sensitive field modification alerts
- ✅ Export capabilities for compliance reports

Some advanced query features may require additional JVM configuration, but all the essential compliance and audit functionality works out of the box with the fat jar approach.

The CLI demo will:
1. Create 1000 sample customer records with PII fields
2. Simulate routine data updates (address, phone, income changes)
3. Simulate sensitive PII corrections (SSN, email changes)
4. Generate comprehensive change detection reports
5. Export audit data for regulatory compliance

### 3. Run the Web Dashboard

```bash
# Run the web dashboard using the provided script (recommended)
./run-dashboard.sh

# Alternative: Run directly with Maven (requires JVM arguments)
export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED"
mvn spring-boot:run -Dspring-boot.run.jvmArguments="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED"
```

Then open your browser to: **http://localhost:8080**

The web dashboard provides:
- Interactive snapshot comparison
- Real-time change detection visualization  
- SQL query interface for temporal analysis
- Compliance reporting features
- One-click demo execution

## Demo Structure

### Core Components

**ComplianceAuditDemo.java** - Main demo application
- Customer data generation with realistic PII fields
- Simulated data modification scenarios  
- Change detection and audit trail generation
- Compliance report creation and export

**CustomerRecord.java** - Data model representing customer information
- Contains both regular and sensitive PII fields
- Built-in data masking for security

**AuditLogger.java** - Comprehensive audit logging system
- Tracks all data modifications with timestamps
- User attribution for compliance requirements
- Export capabilities for regulatory reporting

**ComplianceDashboardApplication.java** - Spring Boot web interface
- Interactive dashboard for visualizing compliance data
- Real-time change detection and reporting
- SQL query interface for temporal analysis

### Sample Data Fields

The demo tracks these types of data:

**Regular Fields:**
- Customer ID, First Name, Last Name
- Credit Score, Annual Income, Account Balance  
- Customer Since Date

**Sensitive PII Fields (specially monitored):**
- Social Security Number (SSN)
- Email Address
- Phone Number  
- Date of Birth
- Home Address

**Financial Fields (compliance-relevant):**
- Credit Score
- Annual Income
- Account Balance

## Key Features Demonstrated

### 1. Snapshot-Based Data Versioning
Unlike traditional databases that overwrite data, Snapbase creates immutable snapshots:

```java
// Create baseline snapshot
String baselineSnapshot = workspace.createSnapshot("customer_data.csv", "baseline");

// Later, after data changes...
String currentSnapshot = workspace.createSnapshot("customer_data.csv", "current");

// Compare snapshots to detect all changes
ChangeDetectionResult changes = workspace.diff("customer_data.csv", "baseline", "current");
```

### 2. Cell-Level Change Detection
Track individual field changes with full context:

```java
// Detect changes with structured results
ChangeDetectionResult changes = workspace.status("customer_data.csv", "baseline");

// Access row-level changes
for (RowModification modification : changes.getRowChanges().getModified()) {
    for (Map.Entry<String, CellChange> entry : modification.getChanges().entrySet()) {
        String column = entry.getKey();
        CellChange cellChange = entry.getValue();
        
        // Track before/after values for audit
        String before = cellChange.getBefore();
        String after = cellChange.getAfter();
    }
}
```

### 3. Temporal SQL Queries
Query across time periods for compliance analysis:

```java
// Find customers with significant income changes across snapshots
try (VectorSchemaRoot result = workspace.query("customer_data.csv", 
    "SELECT id, first_name, last_name " +
    "FROM data " +
    "WHERE id IN (" +
    "  SELECT id FROM data GROUP BY id " +
    "  HAVING MAX(annual_income) - MIN(annual_income) > 20000" +
    ")")) {
    
    // Process results with zero-copy Arrow performance
    for (int i = 0; i < result.getRowCount(); i++) {
        // Access customer data directly from Arrow vectors
    }
}
```

### 4. Compliance Audit Trails
Every change is logged with full context:

```java
// Audit logger tracks all modifications
auditLogger.log("ADMIN", "SENSITIVE: SSN corrected", 
    "customer_id=" + customerId + ", old_ssn=" + maskSSN(oldSSN) + 
    ", new_ssn=" + maskSSN(newSSN));

// Export complete audit trail for regulators
auditLogger.exportToFile(Paths.get("audit_exports/audit_log.csv"));
```

### 5. Data Quality Monitoring
Monitor data quality metrics over time:

```sql
-- Analyze data quality trends across snapshots
SELECT snapshot_name,
       COUNT(*) as total_records,
       COUNT(CASE WHEN email LIKE '%@%' THEN 1 END) as valid_emails,
       COUNT(CASE WHEN ssn IS NOT NULL AND LENGTH(ssn) = 11 THEN 1 END) as valid_ssns
FROM data 
GROUP BY snapshot_name 
ORDER BY snapshot_name;
```

## Files Created During Demo

After running the demo, you'll find these files in `compliance_workspace/`:

### Data Files
- `customer_data.csv` - Current customer dataset
- `.snapbase/` - Snapbase workspace with all snapshots stored in Hive-style partitioning

### Audit Exports  
- `audit_exports/audit_log.csv` - Complete audit trail of all changes
- `audit_exports/customer_data_export.csv` - Current data export for regulators
- `audit_exports/compliance_summary.txt` - Summary compliance report

### Snapshot Structure
```
compliance_workspace/.snapbase/
├── sources/
│   └── customer_data.csv/
│       ├── snapshot_name=baseline_2024-01-15_10-30-00/
│       ├── snapshot_name=routine_updates_2024-01-15_10-31-00/
│       └── snapshot_name=sensitive_changes_2024-01-15_10-32-00/
└── metadata/
```

## Web Dashboard Features

The Spring Boot web interface provides:

### 📊 **Dashboard Overview**
- Snapshot statistics and workspace overview
- One-click demo execution  
- Compliance features summary

### 📸 **Snapshot Management**
- View all created snapshots with record counts
- Snapshot metadata and creation timestamps
- Direct links to change detection

### 🔍 **Change Detection**
- Interactive snapshot comparison interface
- Detailed change analysis with sensitive field highlighting
- Schema change detection (column additions, removals, type changes)
- Row-level change tracking with before/after values

### 📋 **Audit Trail**  
- Complete audit log visualization
- Export capabilities for regulatory requirements
- Search and filtering by user, action, and date range

### 🔎 **SQL Query Interface**
- Interactive SQL editor for temporal queries
- Sample compliance-focused queries
- Real-time query execution with masked sensitive data
- Export query results for analysis

## Comparison with Traditional Databases

| Feature | Traditional Database | Snapbase + Java |
|---------|---------------------|-----------------|
| **Change Tracking** | Complex trigger-based audit tables | Built-in cell-level change detection |
| **Historical Queries** | Expensive temporal table joins | Native SQL across time periods |
| **Data Integrity** | Mutable records, audit logs can be modified | Immutable snapshots, tamper-proof |
| **Compliance Reporting** | Manual report generation from audit tables | Automated change detection and export |
| **Performance** | Degrades with audit table size | Zero-copy Arrow performance |
| **Storage** | Duplicate audit data storage | Efficient columnar snapshots |
| **Rollback** | Complex restore procedures | Simple snapshot-based rollback |

## Real-World Deployment Considerations

### Security
- Use proper authentication and authorization in production
- Encrypt sensitive data at rest and in transit
- Implement proper access controls for different user roles
- Regular security audits of audit trail access

### Scalability  
- Configure S3 storage backend for large datasets
- Use S3 Express One Zone for high-performance workloads
- Implement data retention policies for old snapshots
- Consider data archiving strategies for compliance requirements

### Integration
- Integrate with existing identity management systems
- Connect to enterprise monitoring and alerting systems
- API integration with regulatory reporting systems
- Database triggers or CDC (Change Data Capture) for real-time snapshot creation

## Extended Use Cases

This demo can be extended for various compliance scenarios:

### Healthcare (HIPAA)
- Track access to Protected Health Information (PHI)
- Monitor data sharing between healthcare providers
- Audit patient data corrections and updates

### Financial Services (SOX, PCI DSS)
- Monitor trading data and position changes
- Track customer financial information updates
- Audit payment card data handling procedures

### Technology (GDPR, CCPA)
- Implement "right to be forgotten" with snapshot-based deletion
- Track personal data processing activities
- Monitor data transfers between systems

## Building and Development

### Maven Commands
```bash
# Build the project
mvn clean compile

# Run tests
mvn test

# Package for distribution  
mvn clean package

# Run CLI demo (recommended: use script)
./run-demo.sh
# Or directly with Maven:
# mvn exec:java -Dexec.mainClass="com.snapbase.demos.compliance.ComplianceAuditDemo"

# Start web dashboard (recommended: use script)
./run-dashboard.sh
# Or directly with Maven:
# mvn spring-boot:run -Dspring-boot.run.jvmArguments="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED"
```

### IDE Setup
- Import as Maven project
- Ensure Java 11+ SDK is configured
- Add JVM arguments for Arrow support:
  ```
  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED 
  --enable-native-access=ALL-UNNAMED
  ```

## Troubleshooting

### Common Issues

**"Snapbase library not found"**
```bash
# Build and install Snapbase Java library first
cd ../../java && mvn clean install
```

**"Failed to initialize MemoryUtil" or Arrow-related errors**
This occurs when JVM arguments for Arrow support are not provided. **Solution:**
- Use the provided scripts: `./run-demo.sh` or `./run-dashboard.sh` ✅
- Or manually set JVM arguments when running Maven commands

The demo requires these JVM arguments for Apache Arrow zero-copy performance:
```bash
--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED
```

With proper JVM arguments, all functionality works perfectly:
- All snapshot creation works ✅
- All change detection works ✅  
- All audit logging works ✅
- All compliance reporting works ✅
- All temporal queries work ✅

**"Workspace initialization failed"**
- Check file permissions in the target directory
- Ensure sufficient disk space
- Verify the workspace directory is writable

**Web dashboard not starting**
- Check that port 8080 is available
- Verify Spring Boot dependencies are included
- Check application logs for detailed error messages

## Next Steps

To extend this demo:

1. **Add Authentication**: Implement proper user authentication and role-based access
2. **Real-time Monitoring**: Add webhook/notification system for sensitive changes
3. **Advanced Analytics**: Implement machine learning for anomaly detection
4. **Integration APIs**: Create REST APIs for integration with other systems
5. **Advanced Visualizations**: Add charts and graphs for trend analysis
6. **Multi-tenant Support**: Support multiple organizations/departments
7. **Automated Compliance**: Schedule automated compliance report generation

## Learn More

- [Snapbase Documentation](../../README.md)
- [Java API Reference](../../java/README.md)  
- [Enterprise Security Best Practices](../../docs/security.md)
- [S3 Configuration Guide](../../docs/s3-setup.md)

## License

This demo is part of the Snapbase project and is licensed under the MIT License.