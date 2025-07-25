//! Database operations for table discovery and snapshot creation

use crate::config::{DatabaseConfig, DatabaseType};
use crate::error::Result;
use std::fmt;

/// Database table information
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub schema: Option<String>,
    pub full_name: String,
}

/// Database connection handler
pub struct DatabaseConnection {
    pub config: DatabaseConfig,
    pub connection_string: String,
}

impl DatabaseConnection {
    /// Create a new database connection from configuration
    pub fn new(config: DatabaseConfig) -> Result<Self> {
        let connection_string = config.build_connection_string()?;
        Ok(Self {
            config,
            connection_string,
        })
    }

    /// Discover all tables in the database
    pub fn discover_tables(&self) -> Result<Vec<TableInfo>> {
        let discovery_query = match self.config.db_type {
            DatabaseType::Mysql => {
                // For MySQL, we need to filter by the specific database name
                let db_name = self.config.database.as_deref().unwrap_or("unknown");
                format!("SELECT table_name, table_schema FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = '{db_name}'")
            }
            DatabaseType::Postgresql => {
                // For PostgreSQL, we typically want tables in the 'public' schema
                "SELECT table_name, table_schema FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = 'public'".to_string()
            }
            DatabaseType::Sqlite => {
                // For SQLite, there's no schema concept, just list all user tables
                "SELECT name as table_name, NULL as table_schema FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'".to_string()
            }
        };

        // For now, return a placeholder implementation
        // In a real implementation, this would execute the query against the database
        self.execute_discovery_query(&discovery_query)
    }

    /// Execute a discovery query and return table information
    fn execute_discovery_query(&self, query: &str) -> Result<Vec<TableInfo>> {
        use std::io::Write;

        // Create a temporary SQL file that will ATTACH to the database and query tables
        let temp_dir = std::env::temp_dir();
        let sql_file_path = temp_dir.join(format!("snapbase_discovery_{}.sql", std::process::id()));

        let attach_statement = self.create_attach_statement("discovery_db");
        let sql_content = format!("{attach_statement}\nUSE discovery_db;\n{query}");

        // Write the SQL file
        let mut file = std::fs::File::create(&sql_file_path)?;
        file.write_all(sql_content.as_bytes())?;
        file.sync_all()?;

        // Use DuckDB directly to execute the query
        let connection = duckdb::Connection::open_in_memory().map_err(|e| {
            crate::error::SnapbaseError::invalid_input(format!(
                "Failed to open DuckDB connection: {e}"
            ))
        })?;

        // Execute the attach statement
        connection.execute(&attach_statement, []).map_err(|e| {
            crate::error::SnapbaseError::invalid_input(format!(
                "Failed to attach database '{}': {}. Please check your connection configuration.",
                self.config
                    .database
                    .as_ref()
                    .unwrap_or(&"unknown".to_string()),
                e
            ))
        })?;

        // Set the schema
        connection.execute("USE discovery_db", []).map_err(|e| {
            crate::error::SnapbaseError::invalid_input(format!(
                "Failed to use database schema: {e}"
            ))
        })?;

        // Execute the discovery query
        let mut stmt = connection.prepare(query).map_err(|e| {
            crate::error::SnapbaseError::invalid_input(format!(
                "Failed to prepare table discovery query: {e}"
            ))
        })?;

        let rows = stmt
            .query_map([], |row| {
                let table_name: String = row.get(0)?;
                let schema: Option<String> = row.get(1).ok();
                Ok((table_name, schema))
            })
            .map_err(|e| {
                crate::error::SnapbaseError::invalid_input(format!(
                    "Failed to execute table discovery query: {e}"
                ))
            })?;

        // Clean up temporary file
        let _ = std::fs::remove_file(&sql_file_path);

        let mut tables = Vec::new();
        for row_result in rows {
            let (table_name, schema) = row_result.map_err(|e| {
                crate::error::SnapbaseError::invalid_input(format!(
                    "Failed to read table discovery result: {e}"
                ))
            })?;
            let full_name = if let Some(ref schema) = schema {
                format!("{schema}.{table_name}")
            } else {
                table_name.clone()
            };

            tables.push(TableInfo {
                name: table_name,
                schema,
                full_name,
            });
        }

        Ok(tables)
    }

    /// Filter tables based on configuration
    pub fn filter_tables(&self, tables: Vec<TableInfo>) -> Vec<TableInfo> {
        let included_tables = self.config.get_included_tables();
        let excluded_tables = self.config.get_excluded_tables();

        let mut filtered = Vec::new();

        for table in tables {
            let table_name = &table.name;

            // Check if table should be included
            let should_include = if included_tables.contains(&"*".to_string()) {
                true
            } else {
                included_tables.iter().any(|pattern| {
                    if pattern.contains('*') {
                        self.matches_pattern(table_name, pattern)
                    } else {
                        table_name == pattern
                    }
                })
            };

            // Check if table should be excluded
            let should_exclude = excluded_tables.iter().any(|pattern| {
                if pattern.contains('*') {
                    self.matches_pattern(table_name, pattern)
                } else {
                    table_name == pattern
                }
            });

            if should_include && !should_exclude {
                filtered.push(table);
            }
        }

        filtered
    }

    /// Simple pattern matching for table names (supports * wildcard)
    fn matches_pattern(&self, table_name: &str, pattern: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        if let Some(prefix) = pattern.strip_suffix('*') {
            table_name.starts_with(prefix)
        } else if let Some(suffix) = pattern.strip_prefix('*') {
            table_name.ends_with(suffix)
        } else {
            table_name == pattern
        }
    }

    /// Create a SQL query to snapshot a table
    pub fn create_table_snapshot_query(&self, table: &TableInfo) -> String {
        if let Some(schema) = &table.schema {
            format!("SELECT * FROM {}.{}", schema, table.name)
        } else {
            format!("SELECT * FROM {}", table.name)
        }
    }

    /// Create a DuckDB ATTACH statement for this database
    pub fn create_attach_statement(&self, alias: &str) -> String {
        match self.config.db_type {
            DatabaseType::Mysql => {
                format!(
                    "ATTACH '{}' AS {} (TYPE mysql);",
                    self.connection_string, alias
                )
            }
            DatabaseType::Postgresql => {
                format!(
                    "ATTACH '{}' AS {} (TYPE postgres);",
                    self.connection_string, alias
                )
            }
            DatabaseType::Sqlite => {
                format!(
                    "ATTACH '{}' AS {} (TYPE sqlite);",
                    self.connection_string, alias
                )
            }
        }
    }
}

impl fmt::Display for DatabaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseType::Mysql => write!(f, "mysql"),
            DatabaseType::Postgresql => write!(f, "postgresql"),
            DatabaseType::Sqlite => write!(f, "sqlite"),
        }
    }
}

/// Discover tables from a database configuration
pub fn discover_database_tables(config: &DatabaseConfig) -> Result<Vec<TableInfo>> {
    let connection = DatabaseConnection::new(config.clone())?;
    let all_tables = connection.discover_tables()?;
    Ok(connection.filter_tables(all_tables))
}

/// Create a SQL file content for snapshotting a database table
pub fn create_table_snapshot_sql(config: &DatabaseConfig, table: &TableInfo) -> Result<String> {
    let connection = DatabaseConnection::new(config.clone())?;
    let attach_statement = connection.create_attach_statement("db");
    let query = connection.create_table_snapshot_query(table);

    Ok(format!("{attach_statement}\nUSE db;\n{query}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DatabaseType;

    #[test]
    fn test_pattern_matching() {
        let config = DatabaseConfig {
            db_type: DatabaseType::Mysql,
            connection_string: Some("mysql://user:pass@host:3306/db".to_string()),
            host: None,
            port: None,
            database: None,
            username: None,
            password_env: None,
            tables: vec![],
            exclude_tables: vec![],
        };

        let connection = DatabaseConnection::new(config).unwrap();

        assert!(connection.matches_pattern("users", "*"));
        assert!(connection.matches_pattern("user_profiles", "user*"));
        assert!(connection.matches_pattern("temp_table", "*_table"));
        assert!(connection.matches_pattern("users", "users"));
        assert!(!connection.matches_pattern("posts", "user*"));
    }

    #[test]
    fn test_table_filtering() {
        let config = DatabaseConfig {
            db_type: DatabaseType::Mysql,
            connection_string: Some("mysql://user:pass@host:3306/db".to_string()),
            host: None,
            port: None,
            database: None,
            username: None,
            password_env: None,
            tables: vec!["users".to_string(), "posts".to_string()],
            exclude_tables: vec!["temp_*".to_string()],
        };

        let connection = DatabaseConnection::new(config).unwrap();

        let tables = vec![
            TableInfo {
                name: "users".to_string(),
                schema: None,
                full_name: "users".to_string(),
            },
            TableInfo {
                name: "posts".to_string(),
                schema: None,
                full_name: "posts".to_string(),
            },
            TableInfo {
                name: "temp_data".to_string(),
                schema: None,
                full_name: "temp_data".to_string(),
            },
            TableInfo {
                name: "comments".to_string(),
                schema: None,
                full_name: "comments".to_string(),
            },
        ];

        let filtered = connection.filter_tables(tables);

        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().any(|t| t.name == "users"));
        assert!(filtered.iter().any(|t| t.name == "posts"));
        assert!(!filtered.iter().any(|t| t.name == "temp_data"));
        assert!(!filtered.iter().any(|t| t.name == "comments"));
    }
}
