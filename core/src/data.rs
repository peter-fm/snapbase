//! Data processing utilities using DuckDB

use crate::error::Result;
use crate::hash::ColumnInfo;
use crate::sql;
use blake3;
use duckdb::Connection;
use std::collections::HashMap;
use std::path::Path;

// Type alias for complex return types
type DataChangeResult = (Vec<(Vec<String>, bool, bool)>, Vec<crate::hash::ColumnInfo>);

fn get_duckdb_install_instructions() -> String {
    if cfg!(target_os = "windows") {
        r#"  Windows:
    Download and install the DuckDB library (not the CLI):
    
    curl -L -o duckdb.zip https://github.com/duckdb/duckdb/releases/latest/download/libduckdb-windows-amd64.zip
    7z x duckdb.zip
    mkdir "C:\Program Files\DuckDB\lib"
    mkdir "C:\Program Files\DuckDB\include"
    copy duckdb.dll "C:\Program Files\DuckDB\lib\"
    copy duckdb.lib "C:\Program Files\DuckDB\lib\"
    copy duckdb.h "C:\Program Files\DuckDB\include\"
    
    Then add C:\Program Files\DuckDB\lib to your PATH environment variable."#.to_string()
    } else if cfg!(target_os = "macos") {
        r#"  macOS:
    Unbundled builds are not available for macOS due to cross-compilation complexity.
    Please use the bundled version or build from source:
    
    # Use bundled version (recommended):
    curl -L -o snapbase "https://github.com/peter-fm/snapbase/releases/latest/download/snapbase-macos-arm64-bundled"
    
    # Or build from source:
    cargo build --release --features bundled"#.to_string()
    } else {
        r#"  Linux:
    Install the DuckDB library (libduckdb):
    
    # Manual installation (recommended):
    # Linux:
    wget https://github.com/duckdb/duckdb/releases/latest/download/libduckdb-linux-amd64.zip
    unzip libduckdb-linux-amd64.zip
    sudo cp libduckdb.so /usr/local/lib/
    sudo cp duckdb.h /usr/local/include/
    sudo ldconfig
    
    # Package manager (if available):
    # Ubuntu/Debian: sudo apt update && sudo apt install libduckdb-dev
    # Fedora: sudo dnf install duckdb-devel
    
    # macOS:
    brew install duckdb
    
    # Windows:
    # Download from https://duckdb.org/docs/installation/
    # Extract to C:\\Program Files\\DuckDB\\
    # Note: Package manager versions may be outdated"#.to_string()
    }
}

/// Data processor for various file formats
pub struct DataProcessor {
    pub connection: Connection,
    cached_columns: Option<Vec<ColumnInfo>>,
    streaming_query: Option<String>,
    database_type: Option<DatabaseType>,
    database_name: Option<String>,
}

/// Database type for identifier quoting
#[derive(Debug, Clone)]
pub enum DatabaseType {
    MySQL,
    PostgreSQL,
    SQLite,
    DuckDB,
}

impl DataProcessor {
    /// Create a new data processor with default settings
    pub fn new() -> Result<Self> {
        Self::new_with_config()
    }

    /// Detect database type from connection string
    fn detect_database_type(connection_string: &str) -> Option<DatabaseType> {
        let connection_upper = connection_string.to_uppercase();
        
        if connection_upper.contains("MYSQL") || connection_upper.contains("TYPE MYSQL") {
            Some(DatabaseType::MySQL)
        } else if connection_upper.contains("POSTGRES") || connection_upper.contains("TYPE POSTGRES") {
            Some(DatabaseType::PostgreSQL)
        } else if connection_upper.contains("SQLITE") || connection_upper.contains("TYPE SQLITE") {
            Some(DatabaseType::SQLite)
        } else {
            // Default to DuckDB
            Some(DatabaseType::DuckDB)
        }
    }

    /// Extract database name from MySQL connection string
    fn extract_database_name(connection_string: &str) -> Option<String> {
        // Look for database= pattern in MySQL connection strings
        if let Some(start) = connection_string.find("database=") {
            let after_db = &connection_string[start + 9..]; // Skip "database="
            // Find the end (space, semicolon, or end of string)
            let end = after_db.find(&[' ', ';', '\'', '"'][..]).unwrap_or(after_db.len());
            Some(after_db[..end].to_string())
        } else {
            None
        }
    }

    /// Transform SQL query to use fully qualified table names for MySQL
    fn transform_sql_for_mysql(&self, sql: &str) -> String {
        if let (Some(DatabaseType::MySQL), Some(ref db_name)) = (&self.database_type, &self.database_name) {
            // For MySQL connections, transform unqualified table references to fully qualified ones
            // This prevents DuckDB's inconsistent identifier transformation
            
            // Simple pattern matching for common SQL patterns
            // This handles basic cases like "FROM table_name" -> "FROM db_name.table_name"
            use regex::Regex;
            
            // Match FROM clauses with unqualified table names
            let from_pattern = Regex::new(r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
            let transformed = from_pattern.replace_all(sql, |caps: &regex::Captures| {
                let table_name = &caps[1];
                // Only transform if it's not already qualified (doesn't contain a dot)
                if table_name.contains('.') {
                    caps[0].to_string() // Already qualified, leave as is
                } else {
                    format!("FROM {}.{}", db_name, table_name)
                }
            });
            
            transformed.to_string()
        } else {
            // For non-MySQL connections, return SQL unchanged
            sql.to_string()
        }
    }


    /// Create a new data processor with workspace configuration
    pub fn new_with_workspace(workspace: &crate::workspace::SnapbaseWorkspace) -> Result<Self> {
        let connection = crate::query_engine::create_configured_connection(workspace)?;
        
        Ok(Self { 
            connection, 
            cached_columns: None,
            streaming_query: None,
            database_type: None,
            database_name: None,
        })
    }

    /// Create a new data processor with custom configuration
    pub fn new_with_config() -> Result<Self> {
        let connection = match Connection::open_in_memory() {
            Ok(conn) => conn,
            Err(e) => {
                // Check if this is a DuckDB library loading error
                let error_msg = e.to_string();
                if error_msg.contains("libduckdb") || error_msg.contains("duckdb.dll") || error_msg.contains("cannot open shared object") {
                    let install_instructions = get_duckdb_install_instructions();
                    eprintln!("❌ DuckDB library not found!");
                    eprintln!();
                    eprintln!("This version of snapbase requires DuckDB to be installed on your system.");
                    eprintln!();
                    eprintln!("📦 Install DuckDB:");
                    eprintln!("{install_instructions}");
                    eprintln!();
                    eprintln!("💡 Alternatively, download the bundled version that includes DuckDB:");
                    eprintln!("   Visit: https://github.com/peter-fm/snapbase/releases/latest");
                    eprintln!();
                    eprintln!("   For your platform, download the file ending with '-bundled' instead.");
                    eprintln!();
                    eprintln!("Original error: {error_msg}");
                    std::process::exit(1);
                }
                return Err(e.into());
            }
        };
        
        // Optimize DuckDB for large datasets and performance
        connection.execute("SET memory_limit='8GB'", [])?;  // Increased from 4GB
        // DuckDB auto-detects optimal thread count by default, no need to set explicitly
        connection.execute("SET enable_progress_bar=false", [])?; // Disable for performance
        connection.execute("SET preserve_insertion_order=false", [])?; // Allow reordering for performance
        connection.execute("SET enable_object_cache=true", [])?; // Enable object caching
        // Configure platform-appropriate temp directory
        let temp_dir = if cfg!(target_os = "windows") {
            std::env::var("TEMP").or_else(|_| std::env::var("TMP")).unwrap_or_else(|_| "C:\\temp".to_string())
        } else {
            "/tmp".to_string()
        };
        connection.execute(&format!("SET temp_directory='{temp_dir}'"), [])?;
        connection.execute("SET max_memory='8GB'", [])?; // Set max memory usage
        connection.execute("SET force_compression='auto'", [])?; // Enable compression for temp data
        
        Ok(Self { 
            connection, 
            cached_columns: None,
            streaming_query: None,
            database_type: None,
            database_name: None,
        })
    }

    /// Load data from file and return basic info
    pub fn load_file(&mut self, file_path: &Path) -> Result<DataInfo> {
        // Check if this is a SQL file
        if sql::is_sql_file(file_path) {
            return self.load_sql_file(file_path);
        }
        
        // Validate file exists and is readable
        if !file_path.exists() {
            return Err(crate::error::SnapbaseError::invalid_input(
                format!("File not found: {}", file_path.display())
            ));
        }

        if !file_path.is_file() && !file_path.is_dir() {
            return Err(crate::error::SnapbaseError::invalid_input(
                format!("Path is neither a file nor a directory: {}", file_path.display())
            ));
        }

        let path_str = file_path.to_string_lossy();
        
        // Create a view of the file with proper error handling
        let create_view_sql = format!(
            "CREATE OR REPLACE VIEW data_view AS SELECT * FROM '{path_str}'"
        );
        
        self.connection.execute(&create_view_sql, [])
            .map_err(|e| self.convert_duckdb_error(e, file_path))?;
        
        // Get row count with error handling
        let row_count: u64 = self.connection
            .prepare("SELECT COUNT(*) FROM data_view")
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to prepare row count query: {e}")
            ))?
            .query_row([], |row| row.get(0))
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to get row count: {e}")
            ))?;
        
        // Get column information
        let columns = self.get_column_info()?;
        
        Ok(DataInfo {
            source: file_path.to_path_buf(),
            row_count,
            columns,
        })
    }

    /// Load data from SQL file with database connection
    pub fn load_sql_file(&mut self, file_path: &Path) -> Result<DataInfo> {
        // Load environment variables
        sql::load_env_file()?;
        
        // Parse the SQL file
        let sql_file = sql::parse_sql_file(file_path)?;
        
        // Substitute environment variables in the connection string
        let connection_string = sql::substitute_env_vars(&sql_file.connection_string)?;
        
        // Execute the connection string to attach the database (if provided)
        if !connection_string.is_empty() {
            // Detect database type from connection string
            self.database_type = Self::detect_database_type(&connection_string);
            
            // Extract database name for MySQL connections
            if matches!(self.database_type, Some(DatabaseType::MySQL)) {
                self.database_name = Self::extract_database_name(&connection_string);
            }
            
            self.connection.execute(&connection_string, [])
                .map_err(|e| crate::error::SnapbaseError::data_processing(
                    format!("Failed to execute connection string '{connection_string}': {e}")
                ))?;
        }
        
        // Parse the content again to get setup statements and the SELECT query
        let content = std::fs::read_to_string(file_path)
            .map_err(|e| crate::error::SnapbaseError::invalid_input(
                format!("Failed to read SQL file '{}': {}", file_path.display(), e)
            ))?;
        
        // Split by semicolons to get individual statements
        let statements: Vec<&str> = content.split(';').collect();
        let mut setup_statements = Vec::new();
        let mut select_query = String::new();
        
        for statement in statements {
            let trimmed = statement.trim();
            
            // Skip empty statements
            if trimmed.is_empty() {
                continue;
            }
            
            // Remove any comment lines from the statement
            let cleaned_statement = trimmed.lines()
                .filter(|line| !line.trim().starts_with("--") && !line.trim().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_string();
            
            if cleaned_statement.is_empty() {
                continue;
            }
            
            // Check if this is a SELECT query or CTE (Common Table Expression)
            let upper_statement = cleaned_statement.to_uppercase();
            if (upper_statement.starts_with("SELECT") || upper_statement.starts_with("WITH")) && 
               !upper_statement.contains("CREATE TABLE") {
                select_query = cleaned_statement;
            } else {
                setup_statements.push(cleaned_statement);
            }
        }
        
        // Execute setup statements first
        for statement in setup_statements {
            if !statement.is_empty() {
                self.connection.execute(&statement, [])
                    .map_err(|e| crate::error::SnapbaseError::data_processing(
                        format!("Failed to execute setup statement '{statement}': {e}")
                    ))?;
            }
        }
        
        // For SQL queries, use streaming approach to handle large datasets efficiently
        if select_query.trim().is_empty() {
            return Err(crate::error::SnapbaseError::invalid_input(
                format!("No SELECT query found in SQL file '{}'", file_path.display())
            ));
        }
        
        // Transform the select query to use fully qualified table names for MySQL
        let transformed_query = self.transform_sql_for_mysql(select_query.trim());
        
        // First, get the row count and column info without materializing all data
        let count_query = format!("SELECT COUNT(*) FROM ({})", transformed_query);
        let row_count: u64 = self.connection
            .prepare(&count_query)
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to prepare row count query: {e}")
            ))?
            .query_row([], |row| row.get(0))
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to get row count: {e}")
            ))?;
        
        // Get column information by creating a temporary view with LIMIT 0
        let temp_view_sql = format!(
            "CREATE OR REPLACE VIEW temp_schema_view AS SELECT * FROM ({}) AS query_result LIMIT 0",
            transformed_query
        );
        
        self.connection.execute(&temp_view_sql, [])
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to create temporary view for schema: {e}")
            ))?;
        
        // Get column information from the temporary view and cache it
        let columns = self.get_column_info_from_view("schema_temp_view")?;
        
        // Cache the columns for streaming queries since we won't have data_view
        self.cached_columns = Some(columns.clone());
        
        // Clean up temporary view
        self.connection.execute("DROP VIEW IF EXISTS schema_temp_view", [])
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to drop temporary view: {e}")
            ))?;
        
        // Store the transformed SELECT query for streaming use
        self.streaming_query = Some(transformed_query);
        
        Ok(DataInfo {
            source: file_path.to_path_buf(),
            row_count,
            columns,
        })
    }

    /// Convert DuckDB errors to appropriate SnapbaseError types
    fn convert_duckdb_error(&self, error: duckdb::Error, file_path: &Path) -> crate::error::SnapbaseError {
        let error_msg = error.to_string();
        
        // Detect common file format issues
        if error_msg.contains("CSV Error") || 
           error_msg.contains("Could not convert") ||
           error_msg.contains("Invalid CSV") ||
           error_msg.contains("Unterminated quoted field") {
            crate::error::SnapbaseError::invalid_input(
                format!("Malformed CSV file '{}': {}", file_path.display(), error_msg)
            )
        } else if error_msg.contains("JSON") || error_msg.contains("Malformed JSON") {
            crate::error::SnapbaseError::invalid_input(
                format!("Malformed JSON file '{}': {}", file_path.display(), error_msg)
            )
        } else if error_msg.contains("No files found") || error_msg.contains("does not exist") {
            crate::error::SnapbaseError::invalid_input(
                format!("File not found: {}", file_path.display())
            )
        } else if error_msg.contains("Permission denied") {
            crate::error::SnapbaseError::invalid_input(
                format!("Permission denied accessing file: {}", file_path.display())
            )
        } else if error_msg.contains("UTF-8") || error_msg.contains("encoding") {
            crate::error::SnapbaseError::invalid_input(
                format!("File encoding error '{}': {}", file_path.display(), error_msg)
            )
        } else {
            // For other DuckDB errors, pass through the original error message
            crate::error::SnapbaseError::DuckDb(error)
        }
    }

    /// Get column information from the current view (cached to avoid repeated calls)
    fn get_column_info(&mut self) -> Result<Vec<ColumnInfo>> {
        self.get_column_info_from_view("data_view")
    }

    /// Get column information from a specific view (cached to avoid repeated calls)
    fn get_column_info_from_view(&mut self, view_name: &str) -> Result<Vec<ColumnInfo>> {
        // Return cached columns if available (for data_view only)
        if view_name == "data_view" {
            if let Some(ref columns) = self.cached_columns {
                return Ok(columns.clone());
            }
        }

        // First, get column names in their original order using DESCRIBE
        let describe_sql = format!("DESCRIBE {view_name}");
        let mut stmt = self.connection.prepare(&describe_sql)
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to prepare describe query for '{view_name}': {e}")
            ))?;
            
        let rows = stmt.query_map([], |row| {
            Ok(ColumnInfo {
                name: row.get::<_, String>(0)?,           // column_name
                data_type: row.get::<_, String>(1)?,      // column_type
                nullable: true, // DESCRIBE doesn't provide nullable info, default to true
            })
        }).map_err(|e| crate::error::SnapbaseError::data_processing(
            format!("Failed to query column info: {e}")
        ))?;
        
        let mut columns = Vec::new();
        for row in rows {
            columns.push(row.map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to process column info row: {e}")
            ))?);
        }
        
        // Cache the columns for future calls (only for data_view)
        if view_name == "data_view" {
            self.cached_columns = Some(columns.clone());
        }
        
        Ok(columns)
    }

    /// Extract all data as rows of strings
    pub fn extract_all_data(&mut self) -> Result<Vec<Vec<String>>> {
        self.extract_data_chunked_with_progress(None)
    }

    /// Extract data in chunks with progress reporting for better memory efficiency
    pub fn extract_data_chunked_with_progress(
        &mut self,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<Vec<String>>> {
        // Check if this is a streaming SQL query
        if let Some(ref query) = self.streaming_query.clone() {
            return self.extract_streaming_data_with_progress(query, progress_callback);
        }

        // Regular file-based extraction
        self.extract_regular_data_with_progress(progress_callback)
    }

    /// Extract data from regular files (CSV, Parquet, etc.) with chunking
    fn extract_regular_data_with_progress(
        &mut self,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<Vec<String>>> {
        // First, get column information to determine the number of columns safely
        let columns = self.get_column_info()?;
        let column_count = columns.len();
        
        if column_count == 0 {
            return Ok(Vec::new()); // No columns, return empty data
        }

        // Get total row count for progress reporting
        let total_rows: u64 = self.connection
            .prepare("SELECT COUNT(*) FROM data_view")?
            .query_row([], |row| row.get(0))?;

        if total_rows == 0 {
            return Ok(Vec::new());
        }

        let mut all_data = Vec::with_capacity(total_rows as usize); // Pre-allocate the entire vector to prevent reallocations
        let mut processed_rows = 0u64;

        // No chunking needed - use streaming approach

        // Execute the full query once and stream through results (no LIMIT/OFFSET)
        let mut stmt = self.connection.prepare("SELECT * FROM data_view")
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to prepare streaming data query: {e}")
            ))?;

        let rows = stmt.query_map([], |row| {
            let mut string_row = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let value: String = match row.get_ref(i)? {
                    duckdb::types::ValueRef::Null => String::new(),
                    duckdb::types::ValueRef::Boolean(b) => if b { "true".to_string() } else { "false".to_string() },
                    duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Int(i) => i.to_string(),
                    duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Float(f) => f.to_string(),
                    duckdb::types::ValueRef::Double(f) => f.to_string(),
                    duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                    duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                    duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                    duckdb::types::ValueRef::Date32(d) => {
                        // Convert days since epoch to proper date format
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let date = epoch + chrono::Duration::days(d as i64);
                        date.format("%Y-%m-%d").to_string()
                    },
                    duckdb::types::ValueRef::Time64(unit, t) => {
                        // Convert microseconds since midnight to HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let total_seconds = t / 1_000_000;
                                let hours = total_seconds / 3600;
                                let minutes = (total_seconds % 3600) / 60;
                                let seconds = total_seconds % 60;
                                let microseconds = t % 1_000_000;
                                if microseconds > 0 {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                                } else {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}")
                                }
                            }
                            _ => format!("{t:?}"), // Fallback for other time units
                        }
                    },
                    duckdb::types::ValueRef::Timestamp(unit, ts) => {
                        // Convert microseconds since Unix epoch to YYYY-MM-DD HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let seconds = ts / 1_000_000;
                                let microseconds = ts % 1_000_000;
                                let datetime = chrono::DateTime::from_timestamp(seconds, (microseconds * 1000) as u32)
                                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                                if microseconds > 0 {
                                    datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                                } else {
                                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                                }
                            }
                            _ => format!("{ts:?}"), // Fallback for other time units
                        }
                    },
                    _ => "<unknown>".to_string(),
                };
                string_row.push(value);
            }
            Ok(string_row)
        }).map_err(|e| crate::error::SnapbaseError::data_processing(
            format!("Failed to stream data query: {e}")
        ))?;

        for row_result in rows {
            let row = row_result.map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to process streaming row: {e}")
            ))?;
            
            all_data.push(row);
            processed_rows += 1;

            // Report progress at regular intervals for better performance
            if let Some(callback) = progress_callback {
                if processed_rows % 50000 == 0 || processed_rows >= total_rows {
                    callback(processed_rows, total_rows);
                }
            }
        }
        
        Ok(all_data)
    }

    /// Extract data from streaming SQL queries with chunking for memory efficiency
    fn extract_streaming_data_with_progress(
        &mut self,
        query: &str,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<Vec<String>>> {
        // Get column information
        let columns = self.get_column_info()?;
        let column_count = columns.len();
        
        if column_count == 0 {
            return Ok(Vec::new());
        }

        // Get total row count without materializing data
        let count_query = format!("SELECT COUNT(*) FROM ({query})");
        let total_rows: u64 = self.connection
            .prepare(&count_query)?
            .query_row([], |row| row.get(0))?;

        if total_rows == 0 {
            return Ok(Vec::new());
        }

        let mut all_data = Vec::with_capacity(total_rows as usize); // Pre-allocate the entire vector to prevent reallocations

        // For better performance with large datasets, execute the full query once 
        // and stream through the result set instead of using LIMIT/OFFSET
        let mut stmt = self.connection.prepare(query)
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to prepare streaming query: {e}")
            ))?;

        let rows = stmt.query_map([], |row| {
            let mut string_row = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let value: String = match row.get_ref(i)? {
                    duckdb::types::ValueRef::Null => String::new(),
                    duckdb::types::ValueRef::Boolean(b) => if b { "true".to_string() } else { "false".to_string() },
                    duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Int(i) => i.to_string(),
                    duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Float(f) => f.to_string(),
                    duckdb::types::ValueRef::Double(f) => f.to_string(),
                    duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                    duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                    duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                    duckdb::types::ValueRef::Date32(d) => {
                        // Convert days since epoch to proper date format
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let date = epoch + chrono::Duration::days(d as i64);
                        date.format("%Y-%m-%d").to_string()
                    },
                    duckdb::types::ValueRef::Time64(unit, t) => {
                        // Convert microseconds since midnight to HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let total_seconds = t / 1_000_000;
                                let hours = total_seconds / 3600;
                                let minutes = (total_seconds % 3600) / 60;
                                let seconds = total_seconds % 60;
                                let microseconds = t % 1_000_000;
                                if microseconds > 0 {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                                } else {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}")
                                }
                            }
                            _ => format!("{t:?}"), // Fallback for other time units
                        }
                    },
                    duckdb::types::ValueRef::Timestamp(unit, ts) => {
                        // Convert microseconds since Unix epoch to YYYY-MM-DD HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let seconds = ts / 1_000_000;
                                let microseconds = ts % 1_000_000;
                                let datetime = chrono::DateTime::from_timestamp(seconds, (microseconds * 1000) as u32)
                                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                                if microseconds > 0 {
                                    datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                                } else {
                                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                                }
                            }
                            _ => format!("{ts:?}"), // Fallback for other time units
                        }
                    },
                    _ => "<unknown>".to_string(),
                };
                string_row.push(value);
            }
            Ok(string_row)
        }).map_err(|e| crate::error::SnapbaseError::data_processing(
            format!("Failed to stream query data: {e}")
        ))?;

        let mut processed_rows = 0u64;
        let update_frequency = 50_000; // Report progress every 50K rows for smooth updates

        for row_result in rows {
            let row = row_result.map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to process streaming row: {e}")
            ))?;
            
            all_data.push(row);
            processed_rows += 1;

            // Report progress at regular intervals
            if let Some(callback) = progress_callback {
                if processed_rows % update_frequency == 0 || processed_rows >= total_rows {
                    callback(processed_rows, total_rows);
                }
            }
        }
        
        Ok(all_data)
    }

    /// Export current data to Parquet file using DuckDB COPY
    pub fn export_to_parquet(&mut self, parquet_path: &Path) -> Result<()> {
        self.export_to_parquet_with_flags(parquet_path, None)
    }

    /// Export current data to Parquet file with optional change detection flags
    pub fn export_to_parquet_with_flags(
        &mut self, 
        parquet_path: &Path, 
        baseline_data: Option<&BaselineData>
    ) -> Result<()> {
        // Get current data and schema
        let current_schema = self.get_column_info()?;
        let current_data = self.extract_all_data()?;
        
        // Compute flags using existing change detection system
        let (flag_data, final_schema) = if let Some(baseline) = baseline_data {
            self.compute_flags_with_change_detection(&baseline.schema, &baseline.data, &current_schema, &current_data)?
        } else {
            // No baseline - all rows are considered "added" (first snapshot)
            let flag_data: Vec<(Vec<String>, bool, bool)> = current_data.into_iter()
                .map(|row| (row, true, false)) // (data, added, modified)
                .collect();
            let mut final_schema = current_schema.clone();
            final_schema.extend(vec![
                crate::hash::ColumnInfo { name: "__snapbase_added".to_string(), data_type: "BOOLEAN".to_string(), nullable: false },
                crate::hash::ColumnInfo { name: "__snapbase_modified".to_string(), data_type: "BOOLEAN".to_string(), nullable: false },
            ]);
            (flag_data, final_schema)
        };
        
        // Create a temporary table with the flag data
        self.create_temp_table_with_flags(&final_schema, &flag_data)?;
        
        // Export to Parquet using DuckDB's COPY command
        let copy_sql = format!(
            "COPY (SELECT * FROM temp_flag_data) TO '{}' (FORMAT parquet)",
            parquet_path.to_string_lossy()
        );
        
        self.connection.execute(&copy_sql, [])?;
        
        Ok(())
    }

    /// Compute flags using the existing change detection system
    fn compute_flags_with_change_detection(
        &self,
        baseline_schema: &[crate::hash::ColumnInfo],
        baseline_data: &[Vec<String>],
        current_schema: &[crate::hash::ColumnInfo],
        current_data: &[Vec<String>],
    ) -> Result<DataChangeResult> {
        // Use the existing change detection system
        let changes = crate::change_detection::ChangeDetector::detect_changes(
            baseline_schema,
            baseline_data,
            current_schema,
            current_data,
        )?;
        
        
        // Create flag mappings for current data rows
        let mut current_row_flags: std::collections::HashMap<usize, (bool, bool)> = std::collections::HashMap::new();
        
        // Mark added rows (row_index refers to current data position)
        for addition in &changes.row_changes.added {
            current_row_flags.insert(addition.row_index as usize, (true, false)); // (added, modified)
        }
        
        // Mark modified rows (row_index refers to current data position)
        for modification in &changes.row_changes.modified {
            // Check if this is a "total change" (all non-ID fields changed) - treat as addition
            let total_fields_changed = modification.changes.len();
            let total_fields = current_schema.len();
            
            if total_fields_changed >= total_fields * 2 / 3 {
                // If most fields changed, treat as addition (assume this is a new row)
                current_row_flags.insert(modification.row_index as usize, (true, false)); // (added, modified)
            } else {
                current_row_flags.insert(modification.row_index as usize, (false, true)); // (added, modified)
            }
        }
        
        // Create result data with flags - only include current data (no removed rows)
        let mut flag_data = Vec::new();
        
        // Add current data rows with their flags
        for (index, row) in current_data.iter().enumerate() {
            let flags = current_row_flags.get(&index).unwrap_or(&(false, false));
            flag_data.push((row.clone(), flags.0, flags.1));
        }
        
        // Note: Removed rows are NOT added to snapshots - this creates true snapshots
        
        // Create final schema with flag columns (no removed column)
        let mut final_schema = current_schema.to_vec();
        final_schema.extend(vec![
            crate::hash::ColumnInfo { name: "__snapbase_added".to_string(), data_type: "BOOLEAN".to_string(), nullable: false },
            crate::hash::ColumnInfo { name: "__snapbase_modified".to_string(), data_type: "BOOLEAN".to_string(), nullable: false },
        ]);
        
        Ok((flag_data, final_schema))
    }

    /// Create a temporary table with flag data
    fn create_temp_table_with_flags(
        &mut self,
        schema: &[crate::hash::ColumnInfo],
        flag_data: &[(Vec<String>, bool, bool)],
    ) -> Result<()> {
        // Drop existing temp table
        self.connection.execute("DROP TABLE IF EXISTS temp_flag_data", [])?;
        
        // Create table schema
        let mut create_sql = String::from("CREATE TABLE temp_flag_data (");
        for (i, col) in schema.iter().enumerate() {
            if i > 0 {
                create_sql.push_str(", ");
            }
            create_sql.push_str(&format!("{} {}", col.name, col.data_type));
        }
        create_sql.push(')');
        
        self.connection.execute(&create_sql, [])?;
        
        // Insert data using DuckDB parameterized queries (proper way)
        if !flag_data.is_empty() {
            // Build placeholders for parameterized query
            let placeholders: Vec<String> = (0..schema.len()).map(|_| "?".to_string()).collect();
            let insert_sql = format!(
                "INSERT INTO temp_flag_data VALUES ({})",
                placeholders.join(", ")
            );
            
            let mut stmt = self.connection.prepare(&insert_sql)?;
            
            for (row_data, added, modified) in flag_data {
                // Build parameter slice - DuckDB will handle type conversion automatically
                let mut params: Vec<&dyn duckdb::ToSql> = Vec::new();
                
                // Add original data columns as string references
                for value in row_data.iter() {
                    params.push(value);
                }
                
                // Add flag columns (only added and modified)
                params.push(added); 
                params.push(modified);
                
                stmt.execute(&params[..])?;
            }
        }
        
        Ok(())
    }


    /// Load baseline data into a temporary table for comparison
    pub fn load_baseline_data(&mut self, baseline: &BaselineData) -> Result<()> {
        // Drop existing baseline table if it exists
        self.connection.execute("DROP TABLE IF EXISTS baseline_data", [])?;
        
        // Create baseline table with same structure as current data
        let mut create_sql = String::from("CREATE TABLE baseline_data (");
        for (i, col) in baseline.schema.iter().enumerate() {
            if i > 0 {
                create_sql.push_str(", ");
            }
            create_sql.push_str(&format!("{} {}", col.name, col.data_type));
        }
        create_sql.push(')');
        
        self.connection.execute(&create_sql, [])?;
        
        // Insert baseline data using parameterized queries
        if !baseline.data.is_empty() {
            // Build placeholders for parameterized query
            let num_cols = baseline.schema.len();
            let placeholders: Vec<String> = (0..num_cols).map(|_| "?".to_string()).collect();
            let insert_sql = format!(
                "INSERT INTO baseline_data VALUES ({})",
                placeholders.join(", ")
            );
            
            let mut stmt = self.connection.prepare(&insert_sql)?;
            
            for row in &baseline.data {
                // Build parameter slice - DuckDB will handle type conversion automatically
                let params: Vec<&dyn duckdb::ToSql> = row.iter().map(|v| v as &dyn duckdb::ToSql).collect();
                stmt.execute(&params[..])?;
            }
        }
        
        Ok(())
    }

    /// Stream data row by row with callback for memory efficiency
    pub fn stream_data_with_progress<F>(
        &mut self,
        mut row_callback: F,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<u64>
    where
        F: FnMut(Vec<String>) -> Result<()>,
    {
        // Get column information
        let columns = self.get_column_info()?;
        let column_count = columns.len();
        
        if column_count == 0 {
            return Ok(0);
        }

        // Get total row count
        let total_rows = if let Some(ref query) = self.streaming_query.clone() {
            let count_query = format!("SELECT COUNT(*) FROM ({query})");
            self.connection
                .prepare(&count_query)?
                .query_row([], |row| row.get(0))?
        } else {
            self.connection
                .prepare("SELECT COUNT(*) FROM data_view")?
                .query_row([], |row| row.get(0))?
        };

        if total_rows == 0 {
            return Ok(0);
        }

        let mut processed_rows = 0u64;

        if let Some(ref query) = self.streaming_query.clone() {
            // Stream SQL query results
            let mut stmt = self.connection.prepare(query)?;
            
            let rows = stmt.query_map([], |row| {
                let mut string_row = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let value: String = match row.get_ref(i)? {
                        duckdb::types::ValueRef::Null => String::new(),
                        duckdb::types::ValueRef::Boolean(b) => if b { "true".to_string() } else { "false".to_string() },
                        duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                        duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                        duckdb::types::ValueRef::Int(i) => i.to_string(),
                        duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                        duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                        duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                        duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                        duckdb::types::ValueRef::UInt(i) => i.to_string(),
                        duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                        duckdb::types::ValueRef::Float(f) => f.to_string(),
                        duckdb::types::ValueRef::Double(f) => f.to_string(),
                        duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                        duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                        duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                        duckdb::types::ValueRef::Date32(d) => {
                        // Convert days since epoch to proper date format
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let date = epoch + chrono::Duration::days(d as i64);
                        date.format("%Y-%m-%d").to_string()
                    },
                        duckdb::types::ValueRef::Time64(unit, t) => {
                        // Convert microseconds since midnight to HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let total_seconds = t / 1_000_000;
                                let hours = total_seconds / 3600;
                                let minutes = (total_seconds % 3600) / 60;
                                let seconds = total_seconds % 60;
                                let microseconds = t % 1_000_000;
                                if microseconds > 0 {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                                } else {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}")
                                }
                            }
                            _ => format!("{t:?}"), // Fallback for other time units
                        }
                    },
                        duckdb::types::ValueRef::Timestamp(unit, ts) => {
                        // Convert microseconds since Unix epoch to YYYY-MM-DD HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let seconds = ts / 1_000_000;
                                let microseconds = ts % 1_000_000;
                                let datetime = chrono::DateTime::from_timestamp(seconds, (microseconds * 1000) as u32)
                                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                                if microseconds > 0 {
                                    datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                                } else {
                                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                                }
                            }
                            _ => format!("{ts:?}"), // Fallback for other time units
                        }
                    },
                        _ => "<unknown>".to_string(),
                    };
                    string_row.push(value);
                }
                Ok(string_row)
            })?;
            
            for row_result in rows {
                let row = row_result?;
                row_callback(row)?;
                processed_rows += 1;
                
                // Report progress
                if let Some(callback) = progress_callback {
                    if processed_rows % 50000 == 0 || processed_rows >= total_rows {
                        callback(processed_rows, total_rows);
                    }
                }
            }
        } else {
            // Stream from data_view for regular files
            let mut stmt = self.connection.prepare("SELECT * FROM data_view")?;
            
            let rows = stmt.query_map([], |row| {
                let mut string_row = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let value: String = match row.get_ref(i)? {
                        duckdb::types::ValueRef::Null => String::new(),
                        duckdb::types::ValueRef::Boolean(b) => if b { "true".to_string() } else { "false".to_string() },
                        duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                        duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                        duckdb::types::ValueRef::Int(i) => i.to_string(),
                        duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                        duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                        duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                        duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                        duckdb::types::ValueRef::UInt(i) => i.to_string(),
                        duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                        duckdb::types::ValueRef::Float(f) => f.to_string(),
                        duckdb::types::ValueRef::Double(f) => f.to_string(),
                        duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                        duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                        duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                        duckdb::types::ValueRef::Date32(d) => {
                        // Convert days since epoch to proper date format
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let date = epoch + chrono::Duration::days(d as i64);
                        date.format("%Y-%m-%d").to_string()
                    },
                        duckdb::types::ValueRef::Time64(unit, t) => {
                        // Convert microseconds since midnight to HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let total_seconds = t / 1_000_000;
                                let hours = total_seconds / 3600;
                                let minutes = (total_seconds % 3600) / 60;
                                let seconds = total_seconds % 60;
                                let microseconds = t % 1_000_000;
                                if microseconds > 0 {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                                } else {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}")
                                }
                            }
                            _ => format!("{t:?}"), // Fallback for other time units
                        }
                    },
                        duckdb::types::ValueRef::Timestamp(unit, ts) => {
                        // Convert microseconds since Unix epoch to YYYY-MM-DD HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let seconds = ts / 1_000_000;
                                let microseconds = ts % 1_000_000;
                                let datetime = chrono::DateTime::from_timestamp(seconds, (microseconds * 1000) as u32)
                                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                                if microseconds > 0 {
                                    datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                                } else {
                                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                                }
                            }
                            _ => format!("{ts:?}"), // Fallback for other time units
                        }
                    },
                        _ => "<unknown>".to_string(),
                    };
                    string_row.push(value);
                }
                Ok(string_row)
            })?;
            
            for row_result in rows {
                let row = row_result?;
                row_callback(row)?;
                processed_rows += 1;
                
                // Report progress
                if let Some(callback) = progress_callback {
                    if processed_rows % 50000 == 0 || processed_rows >= total_rows {
                        callback(processed_rows, total_rows);
                    }
                }
            }
        }
        
        Ok(processed_rows)
    }

    /// Extract data by columns (optimized to use single query instead of N queries)
    pub fn extract_column_data(&mut self) -> Result<HashMap<String, Vec<String>>> {
        let columns = self.get_column_info()?;
        if columns.is_empty() {
            return Ok(HashMap::new());
        }
        
        // Initialize column data vectors
        let mut column_data: HashMap<String, Vec<String>> = columns
            .iter()
            .map(|col| (col.name.clone(), Vec::new()))
            .collect();
        
        // Single query to get all columns at once (much more efficient than N queries)
        let mut stmt = self.connection.prepare("SELECT * FROM data_view")
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to prepare column data query: {e}")
            ))?;
        
        let rows = stmt.query_map([], |row| {
            let mut row_values = Vec::with_capacity(columns.len());
            for i in 0..columns.len() {
                let value: String = match row.get_ref(i)? {
                    duckdb::types::ValueRef::Null => String::new(),
                    duckdb::types::ValueRef::Boolean(b) => if b { "true".to_string() } else { "false".to_string() },
                    duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Int(i) => i.to_string(),
                    duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Float(f) => f.to_string(),
                    duckdb::types::ValueRef::Double(f) => f.to_string(),
                    duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                    duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                    duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                    duckdb::types::ValueRef::Date32(d) => {
                        // Convert days since epoch to proper date format
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let date = epoch + chrono::Duration::days(d as i64);
                        date.format("%Y-%m-%d").to_string()
                    },
                    duckdb::types::ValueRef::Time64(unit, t) => {
                        // Convert microseconds since midnight to HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let total_seconds = t / 1_000_000;
                                let hours = total_seconds / 3600;
                                let minutes = (total_seconds % 3600) / 60;
                                let seconds = total_seconds % 60;
                                let microseconds = t % 1_000_000;
                                if microseconds > 0 {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                                } else {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}")
                                }
                            }
                            _ => format!("{t:?}"), // Fallback for other time units
                        }
                    },
                    duckdb::types::ValueRef::Timestamp(unit, ts) => {
                        // Convert microseconds since Unix epoch to YYYY-MM-DD HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let seconds = ts / 1_000_000;
                                let microseconds = ts % 1_000_000;
                                let datetime = chrono::DateTime::from_timestamp(seconds, (microseconds * 1000) as u32)
                                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                                if microseconds > 0 {
                                    datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                                } else {
                                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                                }
                            }
                            _ => format!("{ts:?}"), // Fallback for other time units
                        }
                    },
                    _ => "<unknown>".to_string(),
                };
                row_values.push(value);
            }
            Ok(row_values)
        }).map_err(|e| crate::error::SnapbaseError::data_processing(
            format!("Failed to extract column data: {e}")
        ))?;
        
        // Process each row and distribute values to appropriate columns
        for row_result in rows {
            let row_values = row_result.map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to process column data row: {e}")
            ))?;
            
            // Distribute row values to their respective columns
            for (col_index, value) in row_values.into_iter().enumerate() {
                if let Some(column) = columns.get(col_index) {
                    if let Some(col_vec) = column_data.get_mut(&column.name) {
                        col_vec.push(value);
                    }
                }
            }
        }
        
        Ok(column_data)
    }

    /// Get estimated row count (for progress reporting)
    pub fn estimate_row_count(&mut self, file_path: &Path) -> Result<u64> {
        // For now, just load and count - could be optimized for large files
        self.load_file(file_path)?;
        let count: u64 = self.connection
            .prepare("SELECT COUNT(*) FROM data_view")?
            .query_row([], |row| row.get(0))?;
        Ok(count)
    }



    /// Compute row hashes directly in DuckDB for maximum performance (robust version)
    pub fn compute_row_hashes_sql(&mut self) -> Result<Vec<crate::hash::RowHash>> {
        self.compute_row_hashes_with_progress(None)
    }

    /// Compute row hashes with deterministic ordering and full hash precision
    pub fn compute_row_hashes_with_progress(
        &mut self,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<crate::hash::RowHash>> {
        // Check if this is a streaming SQL query
        if let Some(ref query) = self.streaming_query.clone() {
            return self.compute_streaming_row_hashes_with_progress(query, progress_callback);
        }

        // Regular file-based row hashing
        self.compute_regular_row_hashes_with_progress(progress_callback)
    }

    /// Compute row hashes for regular files with chunking
    fn compute_regular_row_hashes_with_progress(
        &mut self,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<crate::hash::RowHash>> {
        let columns = self.get_column_info()?;
        
        if columns.is_empty() {
            return Ok(Vec::new());
        }

        // Get total row count for progress reporting
        let total_rows: u64 = self.connection
            .prepare("SELECT COUNT(*) FROM data_view")?
            .query_row([], |row| row.get(0))?;

        if total_rows == 0 {
            return Ok(Vec::new());
        }

        let mut all_hashes = Vec::new();
        let mut processed_rows = 0u64;
        let start_time = std::time::Instant::now();
        
        // Use natural file order - no ORDER BY clause needed
        // DuckDB preserves the original row order from CSV files
        let column_list = columns.iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        
        let natural_order_sql = format!(
            "SELECT {column_list} FROM data_view"
        );
        
        let mut stmt = self.connection.prepare(&natural_order_sql)
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to prepare natural order query: {e}")
            ))?;

        let rows = stmt.query_map([], |row| {
            self.extract_row_values_for_hashing(row, &columns)
        }).map_err(|e| crate::error::SnapbaseError::data_processing(
            format!("Failed to create row iterator: {e}")
        ))?;

        // Process each row individually with immediate progress updates
        for (row_index, row_result) in rows.enumerate() {
            let row_values = row_result.map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to process row {row_index}: {e}")
            ))?;
            
            let hash_hex = self.compute_row_hash(&row_values);
            
            all_hashes.push(crate::hash::RowHash {
                row_index: row_index as u64,
                hash: hash_hex,
            });
            
            processed_rows += 1;
            
            self.report_hash_progress(processed_rows, total_rows, start_time, &progress_callback);
        }
        
        // Final newline after progress
        eprintln!();
        
        Ok(all_hashes)
    }

    /// Compute row hashes for streaming SQL queries with chunking for memory efficiency
    fn compute_streaming_row_hashes_with_progress(
        &mut self,
        query: &str,
        progress_callback: Option<&dyn Fn(u64, u64)>,
    ) -> Result<Vec<crate::hash::RowHash>> {
        let columns = self.get_column_info()?;
        
        if columns.is_empty() {
            return Ok(Vec::new());
        }

        // Get total row count without materializing data
        let count_query = format!("SELECT COUNT(*) FROM ({query})");
        let total_rows: u64 = self.connection
            .prepare(&count_query)?
            .query_row([], |row| row.get(0))?;

        if total_rows == 0 {
            return Ok(Vec::new());
        }

        let mut all_hashes = Vec::with_capacity(total_rows as usize); // Pre-allocate to prevent reallocations
        let mut processed_rows = 0u64;
        let start_time = std::time::Instant::now();

        // Execute the full query once and stream through results (no chunking needed)

        let column_list = columns.iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        // Execute the full query once and stream through results (no LIMIT/OFFSET)
        let streaming_sql = format!("SELECT {column_list} FROM ({query})");
        let mut stmt = self.connection.prepare(&streaming_sql)
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to prepare streaming hash query: {e}")
            ))?;

        let rows = stmt.query_map([], |row| {
            self.extract_row_values_for_hashing(row, &columns)
        }).map_err(|e| crate::error::SnapbaseError::data_processing(
            format!("Failed to create streaming row iterator: {e}")
        ))?;

        // Process each row from the single streaming query
        for row_result in rows {
            let row_values = row_result.map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to process streaming row {processed_rows}: {e}")
            ))?;
            
            let hash_hex = self.compute_row_hash(&row_values);
            
            all_hashes.push(crate::hash::RowHash {
                row_index: processed_rows,
                hash: hash_hex,
            });

            processed_rows += 1;
            
            // Report progress at regular intervals for better performance
            if processed_rows % 10000 == 0 || processed_rows >= total_rows {
                self.report_hash_progress(processed_rows, total_rows, start_time, &progress_callback);
            }
        }
        
        // Final newline after progress
        eprintln!();
        
        Ok(all_hashes)
    }

    /// Extract row values for hashing with consistent formatting
    fn extract_row_values_for_hashing(&self, row: &duckdb::Row, columns: &[ColumnInfo]) -> duckdb::Result<Vec<String>> {
        let mut row_values = Vec::new();
        for i in 0..columns.len() {
            let value: String = match row.get_ref(i) {
                Ok(duckdb::types::ValueRef::Null) => String::new(),
                Ok(duckdb::types::ValueRef::Boolean(b)) => b.to_string(),
                Ok(duckdb::types::ValueRef::TinyInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::SmallInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::Int(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::BigInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::HugeInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::UTinyInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::USmallInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::UInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::UBigInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::Float(f)) => {
                    // Use consistent float formatting to avoid precision issues
                    format!("{f:.10}")
                },
                Ok(duckdb::types::ValueRef::Double(f)) => {
                    // Use consistent double formatting to avoid precision issues
                    format!("{f:.15}")
                },
                Ok(duckdb::types::ValueRef::Decimal(d)) => d.to_string(),
                Ok(duckdb::types::ValueRef::Text(s)) => String::from_utf8_lossy(s).to_string(),
                Ok(duckdb::types::ValueRef::Blob(b)) => format!("<blob:{} bytes>", b.len()),
                Ok(duckdb::types::ValueRef::Date32(d)) => {
                    // Convert days since epoch to proper date format
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let date = epoch + chrono::Duration::days(d as i64);
                    date.format("%Y-%m-%d").to_string()
                },
                Ok(duckdb::types::ValueRef::Time64(unit, t)) => {
                    // Convert microseconds since midnight to HH:MM:SS format
                    match unit {
                        duckdb::types::TimeUnit::Microsecond => {
                            let total_seconds = t / 1_000_000;
                            let hours = total_seconds / 3600;
                            let minutes = (total_seconds % 3600) / 60;
                            let seconds = total_seconds % 60;
                            let microseconds = t % 1_000_000;
                            if microseconds > 0 {
                                format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                            } else {
                                format!("{hours:02}:{minutes:02}:{seconds:02}")
                            }
                        }
                        _ => format!("{t:?}"), // Fallback for other time units
                    }
                },
                Ok(duckdb::types::ValueRef::Timestamp(unit, ts)) => {
                    // Convert microseconds since Unix epoch to YYYY-MM-DD HH:MM:SS format
                    match unit {
                        duckdb::types::TimeUnit::Microsecond => {
                            let seconds = ts / 1_000_000;
                            let microseconds = ts % 1_000_000;
                            let datetime = chrono::DateTime::from_timestamp(seconds, (microseconds * 1000) as u32)
                                .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                            if microseconds > 0 {
                                datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                            } else {
                                datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                            }
                        }
                        _ => format!("{ts:?}"), // Fallback for other time units
                    }
                },
                _ => String::new(), // Handle any other types or errors
            };
            row_values.push(value);
        }
        Ok(row_values)
    }

    /// Compute hash for a row's values
    fn compute_row_hash(&self, row_values: &[String]) -> String {
        // Hash the row values using Blake3 with consistent separator
        let row_content = row_values.join("||"); // Use || to avoid conflicts with | in data
        let hash = blake3::hash(row_content.as_bytes());
        
        // Use full Blake3 hash for maximum collision resistance
        hash.to_hex().to_string()
    }

    /// Report progress for hash computation
    fn report_hash_progress(
        &self,
        processed_rows: u64,
        total_rows: u64,
        start_time: std::time::Instant,
        progress_callback: &Option<&dyn Fn(u64, u64)>,
    ) {
        // Real-time progress updates - every 10000 rows for large files, every 1000 for smaller
        let update_frequency = if total_rows > 1_000_000 { 10000 } else { 1000 };
        
        if processed_rows % update_frequency == 0 || processed_rows == total_rows {
            // Always print to stderr for immediate feedback
            let elapsed = start_time.elapsed().as_secs_f64();
            let rate = processed_rows as f64 / elapsed;
            let percent = (processed_rows as f64 / total_rows as f64) * 100.0;
            
            use std::io::Write;
            eprint!("\rProcessed: {processed_rows}/{total_rows} rows ({percent:.1}%) - {rate:.0} rows/sec");
            let _ = std::io::stderr().flush();
            
            // Also call the progress callback if provided
            if let Some(callback) = progress_callback {
                callback(processed_rows, total_rows);
            }
        }
    }

    /// Compute column hashes efficiently - just hash column metadata, not all data
    pub fn compute_column_hashes_sql(&mut self) -> Result<Vec<crate::hash::ColumnHash>> {
        let columns = self.get_column_info()?;
        
        if columns.is_empty() {
            return Ok(Vec::new());
        }

        // Fast column hashing - just hash the column metadata, not all the data
        // This is much more efficient and still provides change detection for schema changes
        DataProcessor::compute_column_metadata_hashes(&columns)
    }

    /// Efficient column hash computation - hash only metadata, not data content
    pub fn compute_column_metadata_hashes(columns: &[ColumnInfo]) -> Result<Vec<crate::hash::ColumnHash>> {
        let mut column_hashes = Vec::new();

        for column in columns {
            // Hash just the column metadata (name + type + nullable flag)
            // This is much faster than hashing all column data
            let metadata_string = format!("{}|{}|{}", 
                column.name, 
                column.data_type, 
                if column.nullable { "nullable" } else { "not_null" }
            );
            
            // Use a simple hash of the metadata
            let hash_hex = format!("{:016x}", 
                blake3::hash(metadata_string.as_bytes()).as_bytes()[0..8]
                    .iter()
                    .fold(0u64, |acc, &b| (acc << 8) | b as u64)
            );

            column_hashes.push(crate::hash::ColumnHash {
                column_name: column.name.clone(),
                column_type: column.data_type.clone(),
                hash: hash_hex,
            });
        }

        // Preserve original column order - don't sort alphabetically
        Ok(column_hashes)
    }



    /// Load data from cloud storage using DuckDB S3 extension
    pub async fn load_cloud_storage_data(
        &mut self,
        data_path: &str,
        workspace: &crate::workspace::SnapbaseWorkspace,
    ) -> Result<Vec<Vec<String>>> {
        // Convert storage path to DuckDB-compatible path (handles both local and S3)
        let duckdb_path = workspace.storage().get_duckdb_path(data_path);
        
        // True snapshots contain only current data - no need to filter removed rows
        let query = format!("SELECT * FROM read_parquet('{duckdb_path}')");
        
        let mut stmt = self.connection.prepare(&query)
            .map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to prepare cloud storage query: {e}")
            ))?;

        let rows = stmt.query_map([], |row| {
            let column_count = row.as_ref().column_count();
            let mut string_row = Vec::with_capacity(column_count);
            
            for i in 0..column_count {
                let value: String = match row.get_ref(i)? {
                    duckdb::types::ValueRef::Null => String::new(),
                    duckdb::types::ValueRef::Boolean(b) => if b { "true".to_string() } else { "false".to_string() },
                    duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Int(i) => i.to_string(),
                    duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Float(f) => f.to_string(),
                    duckdb::types::ValueRef::Double(f) => f.to_string(),
                    duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                    duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                    duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                    duckdb::types::ValueRef::Date32(d) => {
                        // Convert days since epoch to proper date format
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let date = epoch + chrono::Duration::days(d as i64);
                        date.format("%Y-%m-%d").to_string()
                    },
                    duckdb::types::ValueRef::Time64(unit, t) => {
                        // Convert microseconds since midnight to HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let total_seconds = t / 1_000_000;
                                let hours = total_seconds / 3600;
                                let minutes = (total_seconds % 3600) / 60;
                                let seconds = total_seconds % 60;
                                let microseconds = t % 1_000_000;
                                if microseconds > 0 {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                                } else {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}")
                                }
                            }
                            _ => format!("{t:?}"), // Fallback for other time units
                        }
                    },
                    duckdb::types::ValueRef::Timestamp(unit, ts) => {
                        // Convert microseconds since Unix epoch to YYYY-MM-DD HH:MM:SS format
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let seconds = ts / 1_000_000;
                                let microseconds = ts % 1_000_000;
                                let datetime = chrono::DateTime::from_timestamp(seconds, (microseconds * 1000) as u32)
                                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                                if microseconds > 0 {
                                    datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                                } else {
                                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                                }
                            }
                            _ => format!("{ts:?}"), // Fallback for other time units
                        }
                    },
                    _ => "<unknown>".to_string(),
                };
                string_row.push(value);
            }
            Ok(string_row)
        }).map_err(|e| crate::error::SnapbaseError::data_processing(
            format!("Failed to extract cloud storage data: {e}")
        ))?;
        
        let mut all_rows = Vec::new();
        for row in rows {
            all_rows.push(row.map_err(|e| crate::error::SnapbaseError::data_processing(
                format!("Failed to process cloud storage row: {e}")
            ))?);
        }


        Ok(all_rows)
    }

    /// Stream rows for memory-efficient change detection
    /// Returns an async stream of (row_index, row_data) pairs
    pub async fn stream_rows_async<F>(
        &mut self,
        progress_callback: Option<F>
    ) -> Result<Vec<(u64, Vec<String>)>>
    where 
        F: Fn(u64, u64, &str)
    {
        let columns = self.get_column_info()?;
        let column_count = columns.len();
        
        if column_count == 0 {
            return Ok(Vec::new());
        }

        // Get total row count for progress reporting
        let total_rows = if let Some(ref query) = self.streaming_query.clone() {
            let count_query = format!("SELECT COUNT(*) FROM ({query})");
            self.connection
                .prepare(&count_query)?
                .query_row([], |row| row.get(0))?
        } else {
            self.connection
                .prepare("SELECT COUNT(*) FROM data_view")?
                .query_row([], |row| row.get(0))?
        };

        if total_rows == 0 {
            return Ok(Vec::new());
        }

        let mut all_rows = Vec::new();
        let mut processed_rows = 0u64;

        // Stream rows using the same logic as extract_data_chunked_with_progress
        if let Some(ref query) = self.streaming_query.clone() {
            // Stream SQL query results
            let mut stmt = self.connection.prepare(query)?;
            let rows = stmt.query_map([], |row| {
                self.extract_row_values_for_streaming(row, &columns)
            })?;
            
            for row_result in rows {
                let row_values = row_result?;
                all_rows.push((processed_rows, row_values));
                processed_rows += 1;
                
                if processed_rows % 50000 == 0 {
                    if let Some(ref callback) = progress_callback {
                        callback(processed_rows, total_rows, "Streaming rows...");
                    }
                }
            }
        } else {
            // Stream from data_view for regular files
            let mut stmt = self.connection.prepare("SELECT * FROM data_view")?;
            let rows = stmt.query_map([], |row| {
                self.extract_row_values_for_streaming(row, &columns)
            })?;
            
            for row_result in rows {
                let row_values = row_result?;
                all_rows.push((processed_rows, row_values));
                processed_rows += 1;
                
                if processed_rows % 50000 == 0 {
                    if let Some(ref callback) = progress_callback {
                        callback(processed_rows, total_rows, "Streaming rows...");
                    }
                }
            }
        }
        
        if let Some(ref callback) = progress_callback {
            callback(processed_rows, total_rows, "Streaming completed");
        }
        
        Ok(all_rows)
    }
    
    /// Load specific rows by indices (for Phase 3 of streaming change detection)
    /// This only loads the rows that have been identified as changed
    pub async fn load_specific_rows(
        &mut self,
        row_indices: &[u64]
    ) -> Result<HashMap<u64, Vec<String>>> {
        if row_indices.is_empty() {
            return Ok(HashMap::new());
        }
        
        let columns = self.get_column_info()?;
        let column_count = columns.len();
        
        if column_count == 0 {
            return Ok(HashMap::new());
        }
        
        let mut result = HashMap::new();
        
        // Create a temporary table with row numbers for efficient lookups
        // This is more efficient than multiple individual queries
        let indices_list = row_indices.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",");
        
        // Create a temporary table with row numbers to avoid window functions in WHERE clause
        let temp_table = "temp_indexed_data";
        self.connection.execute(&format!("DROP TABLE IF EXISTS {}", temp_table), [])?;
        
        let create_temp_query = if let Some(ref streaming_query) = self.streaming_query.clone() {
            format!(
                "CREATE TABLE {} AS 
                 SELECT ROW_NUMBER() OVER () - 1 as row_num, * FROM ({})",
                temp_table, streaming_query
            )
        } else {
            format!(
                "CREATE TABLE {} AS 
                 SELECT ROW_NUMBER() OVER () - 1 as row_num, * FROM data_view",
                temp_table
            )
        };
        
        self.connection.execute(&create_temp_query, [])?;
        
        // Build column list dynamically to avoid duplicates
        let column_names: Vec<String> = columns.iter().map(|c| format!("\"{}\"", c.name)).collect();
        let columns_str = column_names.join(", ");
        let query = format!(
            "SELECT row_num, {} FROM {} WHERE row_num IN ({})",
            columns_str, temp_table, indices_list
        );
        
        let mut stmt = self.connection.prepare(&query)?;
        
        let rows = stmt.query_map([], |row| {
            // First column is row_num, rest are the actual data
            let row_num: u64 = row.get(0)?;
            let mut row_values = Vec::with_capacity(column_count);
            
            for i in 1..=column_count { // Skip first column (row_num)
                let value: String = match row.get_ref(i)? {
                    duckdb::types::ValueRef::Null => String::new(),
                    duckdb::types::ValueRef::Boolean(b) => if b { "true".to_string() } else { "false".to_string() },
                    duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Int(i) => i.to_string(),
                    duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Float(f) => f.to_string(),
                    duckdb::types::ValueRef::Double(f) => f.to_string(),
                    duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                    duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                    duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                    duckdb::types::ValueRef::Date32(d) => {
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let date = epoch + chrono::Duration::days(d as i64);
                        date.format("%Y-%m-%d").to_string()
                    },
                    duckdb::types::ValueRef::Time64(unit, t) => {
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let total_seconds = t / 1_000_000;
                                let hours = total_seconds / 3600;
                                let minutes = (total_seconds % 3600) / 60;
                                let seconds = total_seconds % 60;
                                let microseconds = t % 1_000_000;
                                if microseconds > 0 {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                                } else {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}")
                                }
                            }
                            _ => format!("{t:?}"),
                        }
                    },
                    duckdb::types::ValueRef::Timestamp(unit, ts) => {
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let seconds = ts / 1_000_000;
                                let microseconds = ts % 1_000_000;
                                let datetime = chrono::DateTime::from_timestamp(seconds, (microseconds * 1000) as u32)
                                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                                if microseconds > 0 {
                                    datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                                } else {
                                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                                }
                            }
                            _ => format!("{ts:?}"),
                        }
                    },
                    _ => "<unknown>".to_string(),
                };
                row_values.push(value);
            }
            
            Ok((row_num, row_values))
        })?;
        
        for row_result in rows {
            let (row_num, row_values) = row_result?;
            result.insert(row_num, row_values);
        }
        
        // Clean up temp table
        self.connection.execute(&format!("DROP TABLE IF EXISTS {}", temp_table), [])?;
        
        Ok(result)
    }
    
    /// Load specific rows from cloud storage (Parquet files) by indices
    pub async fn load_specific_rows_from_storage(
        &mut self,
        data_path: &str,
        workspace: &crate::workspace::SnapbaseWorkspace,
        row_indices: &[u64]
    ) -> Result<HashMap<u64, Vec<String>>> {
        if row_indices.is_empty() {
            return Ok(HashMap::new());
        }
        
        // Convert storage path to DuckDB-compatible path
        let duckdb_path = workspace.storage().get_duckdb_path(data_path);
        
        // Create comma-separated list of indices
        let indices_list = row_indices.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",");
        
        // Create a temporary table with row numbers to avoid window functions in WHERE clause
        let temp_table = "temp_parquet_indexed_data";
        self.connection.execute(&format!("DROP TABLE IF EXISTS {}", temp_table), [])?;
        
        let create_temp_query = format!(
            "CREATE TABLE {} AS 
             SELECT ROW_NUMBER() OVER () - 1 as row_num, * 
             FROM read_parquet('{}')",
            temp_table, duckdb_path
        );
        
        self.connection.execute(&create_temp_query, [])?;
        
        // Now we can use a simple WHERE clause on the temp table
        let query = format!(
            "SELECT row_num, * FROM {} WHERE row_num IN ({})",
            temp_table, indices_list
        );
        
        let mut stmt = self.connection.prepare(&query)?;
        let mut result = HashMap::new();
        
        let rows = stmt.query_map([], |row| {
            let row_num: u64 = row.get(0)?;
            let column_count = row.as_ref().column_count() - 1; // Subtract 1 for row_num column
            let mut string_row = Vec::with_capacity(column_count);
            
            for i in 1..=column_count {
                let value: String = match row.get_ref(i)? {
                    duckdb::types::ValueRef::Null => String::new(),
                    duckdb::types::ValueRef::Boolean(b) => if b { "true".to_string() } else { "false".to_string() },
                    duckdb::types::ValueRef::TinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::SmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Int(i) => i.to_string(),
                    duckdb::types::ValueRef::BigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::HugeInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UTinyInt(i) => i.to_string(),
                    duckdb::types::ValueRef::USmallInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UInt(i) => i.to_string(),
                    duckdb::types::ValueRef::UBigInt(i) => i.to_string(),
                    duckdb::types::ValueRef::Float(f) => f.to_string(),
                    duckdb::types::ValueRef::Double(f) => f.to_string(),
                    duckdb::types::ValueRef::Decimal(d) => d.to_string(),
                    duckdb::types::ValueRef::Text(s) => String::from_utf8_lossy(s).into_owned(),
                    duckdb::types::ValueRef::Blob(b) => format!("<blob:{} bytes>", b.len()),
                    duckdb::types::ValueRef::Date32(d) => {
                        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let date = epoch + chrono::Duration::days(d as i64);
                        date.format("%Y-%m-%d").to_string()
                    },
                    duckdb::types::ValueRef::Time64(unit, t) => {
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let total_seconds = t / 1_000_000;
                                let hours = total_seconds / 3600;
                                let minutes = (total_seconds % 3600) / 60;
                                let seconds = total_seconds % 60;
                                let microseconds = t % 1_000_000;
                                if microseconds > 0 {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                                } else {
                                    format!("{hours:02}:{minutes:02}:{seconds:02}")
                                }
                            }
                            _ => format!("{t:?}"),
                        }
                    },
                    duckdb::types::ValueRef::Timestamp(unit, ts) => {
                        match unit {
                            duckdb::types::TimeUnit::Microsecond => {
                                let seconds = ts / 1_000_000;
                                let microseconds = ts % 1_000_000;
                                let datetime = chrono::DateTime::from_timestamp(seconds, (microseconds * 1000) as u32)
                                    .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                                if microseconds > 0 {
                                    datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                                } else {
                                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                                }
                            }
                            _ => format!("{ts:?}"),
                        }
                    },
                    _ => "<unknown>".to_string(),
                };
                string_row.push(value);
            }
            Ok((row_num, string_row))
        })?;
        
        for row_result in rows {
            let (row_num, string_row) = row_result?;
            result.insert(row_num, string_row);
        }
        
        // Clean up temp table
        self.connection.execute(&format!("DROP TABLE IF EXISTS {}", temp_table), [])?;
        
        Ok(result)
    }
    
    /// Helper method to extract row values for streaming (reused logic)
    fn extract_row_values_for_streaming(&self, row: &duckdb::Row, columns: &[ColumnInfo]) -> duckdb::Result<Vec<String>> {
        let mut row_values = Vec::new();
        for i in 0..columns.len() {
            let value: String = match row.get_ref(i) {
                Ok(duckdb::types::ValueRef::Null) => String::new(),
                Ok(duckdb::types::ValueRef::Boolean(b)) => if b { "true".to_string() } else { "false".to_string() },
                Ok(duckdb::types::ValueRef::TinyInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::SmallInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::Int(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::BigInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::HugeInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::UTinyInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::USmallInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::UInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::UBigInt(i)) => i.to_string(),
                Ok(duckdb::types::ValueRef::Float(f)) => f.to_string(),
                Ok(duckdb::types::ValueRef::Double(f)) => f.to_string(),
                Ok(duckdb::types::ValueRef::Decimal(d)) => d.to_string(),
                Ok(duckdb::types::ValueRef::Text(s)) => String::from_utf8_lossy(s).to_string(),
                Ok(duckdb::types::ValueRef::Blob(b)) => format!("<blob:{} bytes>", b.len()),
                Ok(duckdb::types::ValueRef::Date32(d)) => {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let date = epoch + chrono::Duration::days(d as i64);
                    date.format("%Y-%m-%d").to_string()
                },
                Ok(duckdb::types::ValueRef::Time64(unit, t)) => {
                    match unit {
                        duckdb::types::TimeUnit::Microsecond => {
                            let total_seconds = t / 1_000_000;
                            let hours = total_seconds / 3600;
                            let minutes = (total_seconds % 3600) / 60;
                            let seconds = total_seconds % 60;
                            let microseconds = t % 1_000_000;
                            if microseconds > 0 {
                                format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}")
                            } else {
                                format!("{hours:02}:{minutes:02}:{seconds:02}")
                            }
                        }
                        _ => format!("{t:?}"),
                    }
                },
                Ok(duckdb::types::ValueRef::Timestamp(unit, ts)) => {
                    match unit {
                        duckdb::types::TimeUnit::Microsecond => {
                            let seconds = ts / 1_000_000;
                            let microseconds = ts % 1_000_000;
                            let datetime = chrono::DateTime::from_timestamp(seconds, (microseconds * 1000) as u32)
                                .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                            if microseconds > 0 {
                                datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string()
                            } else {
                                datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                            }
                        }
                        _ => format!("{ts:?}"),
                    }
                },
                _ => String::new(),
            };
            row_values.push(value);
        }
        Ok(row_values)
    }

    /// Check if file format is supported
    pub fn is_supported_format(file_path: &Path) -> bool {
        if let Some(extension) = file_path.extension().and_then(|s| s.to_str()) {
            matches!(extension.to_lowercase().as_str(), 
                     "csv" | "parquet" | "json" | "jsonl" | "tsv" | "sql" | "xlsx" | "xls")
        } else {
            false
        }
    }
}

/// Information about loaded data
#[derive(Debug, Clone)]
pub struct DataInfo {
    pub source: std::path::PathBuf,
    pub row_count: u64,
    pub columns: Vec<ColumnInfo>,
}

/// Baseline data for change detection
#[derive(Debug, Clone)]
pub struct BaselineData {
    pub schema: Vec<ColumnInfo>,
    pub data: Vec<Vec<String>>,
}

impl DataInfo {
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_data_processor_creation() {
        let _processor = DataProcessor::new().unwrap();
        // Just test that we can create a processor
        assert!(true);
    }

    #[test]
    fn test_supported_formats() {
        assert!(DataProcessor::is_supported_format(Path::new("test.csv")));
        assert!(DataProcessor::is_supported_format(Path::new("test.parquet")));
        assert!(DataProcessor::is_supported_format(Path::new("test.json")));
        assert!(DataProcessor::is_supported_format(Path::new("test.sql")));
        assert!(DataProcessor::is_supported_format(Path::new("test.xlsx")));
        assert!(DataProcessor::is_supported_format(Path::new("test.xls")));
        assert!(!DataProcessor::is_supported_format(Path::new("test.txt")));
        assert!(!DataProcessor::is_supported_format(Path::new("test")));
    }

    #[test]
    fn test_csv_loading() {
        let temp_dir = TempDir::new().unwrap();
        let csv_path = temp_dir.path().join("test.csv");
        
        // Create a simple CSV file
        let csv_content = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
        fs::write(&csv_path, csv_content).unwrap();
        
        let mut processor = DataProcessor::new().unwrap();
        let data_info = processor.load_file(&csv_path).unwrap();
        
        assert_eq!(data_info.row_count, 2);
        assert_eq!(data_info.column_count(), 3);
        assert_eq!(data_info.column_names(), vec!["name", "age", "city"]);
    }

    #[test]
    fn test_excel_loading() {
        // Test with the actual test fixtures
        let excel_path = Path::new("tests/fixtures/data/simple.xlsx");
        
        if excel_path.exists() {
            let mut processor = DataProcessor::new().unwrap();
            let data_info = processor.load_file(excel_path).unwrap();
            
            assert_eq!(data_info.row_count, 3);
            assert_eq!(data_info.column_count(), 3);
            assert_eq!(data_info.column_names(), vec!["name", "age", "city"]);
        }
    }

    #[test]
    fn test_database_type_detection() {
        // Test MySQL detection
        let mysql_conn = "ATTACH 'host=localhost user=test database=test' AS test (TYPE mysql)";
        let db_type = DataProcessor::detect_database_type(mysql_conn);
        assert!(matches!(db_type, Some(DatabaseType::MySQL)));
        
        // Test PostgreSQL detection
        let pg_conn = "ATTACH 'host=localhost user=test database=test' AS test (TYPE postgres)";
        let db_type = DataProcessor::detect_database_type(pg_conn);
        assert!(matches!(db_type, Some(DatabaseType::PostgreSQL)));
        
        // Test SQLite detection
        let sqlite_conn = "ATTACH 'database.db' AS test (TYPE sqlite)";
        let db_type = DataProcessor::detect_database_type(sqlite_conn);
        assert!(matches!(db_type, Some(DatabaseType::SQLite)));
        
        // Test default (DuckDB) detection
        let unknown_conn = "ATTACH 'unknown://connection'";
        let db_type = DataProcessor::detect_database_type(unknown_conn);
        assert!(matches!(db_type, Some(DatabaseType::DuckDB)));
    }

}
