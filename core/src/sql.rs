//! SQL file parsing and database connection handling

use crate::error::Result;
use std::fs;
use std::path::Path;
use std::env;

/// Represents a parsed SQL file with connection information
#[derive(Debug, Clone)]
pub struct SqlFile {
    pub connection_string: String,
    pub query: String,
    pub source_path: std::path::PathBuf,
}

/// Parse a SQL file to extract connection string and query
pub fn parse_sql_file(file_path: &Path) -> Result<SqlFile> {
    let content = fs::read_to_string(file_path)
        .map_err(|e| crate::error::SnapbaseError::invalid_input(
            format!("Failed to read SQL file '{}': {}", file_path.display(), e)
        ))?;
    
    let lines: Vec<&str> = content.lines().collect();
    let mut connection_string = String::new();
    let mut setup_lines = Vec::new();
    let mut query_lines = Vec::new();
    let mut found_connection = false;
    let mut in_select_query = false;
    
    for line in lines {
        let trimmed = line.trim();
        
        // Look for connection string in comments at the top
        if trimmed.starts_with("--") || trimmed.starts_with("//") {
            // Remove comment markers and trim
            let comment_content = if trimmed.starts_with("--") {
                trimmed.strip_prefix("--").unwrap().trim()
            } else {
                trimmed.strip_prefix("//").unwrap().trim()
            };
            
            // Check if this line contains a connection string
            if comment_content.to_uppercase().contains("ATTACH") && 
               (comment_content.contains("mysql") || 
                comment_content.contains("postgres") || 
                comment_content.contains("sqlite") ||
                comment_content.contains("TYPE")) {
                connection_string = comment_content.to_string();
                found_connection = true;
            }
        } else if !trimmed.is_empty() {
            // Check if this is the start of a SELECT query
            if trimmed.to_uppercase().starts_with("SELECT") {
                in_select_query = true;
                query_lines.push(line);
            } else if in_select_query {
                // Continue collecting the SELECT query
                query_lines.push(line);
            } else {
                // This is setup SQL (USE, CREATE, INSERT, etc.)
                setup_lines.push(line);
            }
        }
    }
    
    // Connection string is optional - if not found, we'll use DuckDB's in-memory capabilities
    if !found_connection {
        connection_string = String::new(); // Empty connection string for in-memory use
    }
    
    // Combine setup and query
    let mut full_query = String::new();
    
    // Add setup statements if any
    if !setup_lines.is_empty() {
        full_query.push_str(&setup_lines.join("\n"));
        full_query.push('\n');
    }
    
    // Add the SELECT query
    if query_lines.is_empty() {
        return Err(crate::error::SnapbaseError::invalid_input(
            format!("No SELECT query found in file '{}'", file_path.display())
        ));
    }
    
    full_query.push_str(&query_lines.join("\n"));
    
    // For the view creation, we only want the SELECT query
    let select_query = query_lines.join("\n").trim().to_string();
    
    Ok(SqlFile {
        connection_string,
        query: select_query,
        source_path: file_path.to_path_buf(),
    })
}

/// Substitute environment variables in a connection string
pub fn substitute_env_vars(connection_string: &str) -> Result<String> {
    let mut result = connection_string.to_string();
    
    // Find all environment variable placeholders like {VAR_NAME}
    let mut start = 0;
    while let Some(open_pos) = result[start..].find('{') {
        let open_pos = start + open_pos;
        if let Some(close_pos) = result[open_pos..].find('}') {
            let close_pos = open_pos + close_pos;
            let var_name = &result[open_pos + 1..close_pos];
            
            // Get the environment variable value
            let var_value = env::var(var_name)
                .map_err(|_| crate::error::SnapbaseError::invalid_input(
                    format!("Environment variable '{var_name}' not found. Make sure it's set in your .env file or environment.")
                ))?;
            
            // Replace the placeholder with the actual value
            result.replace_range(open_pos..=close_pos, &var_value);
            start = open_pos + var_value.len();
        } else {
            start = open_pos + 1;
        }
    }
    
    Ok(result)
}

/// Load environment variables from .env file if it exists
pub fn load_env_file() -> Result<()> {
    // Try to load .env file from current directory
    if Path::new(".env").exists() {
        dotenv::dotenv().map_err(|e| crate::error::SnapbaseError::invalid_input(
            format!("Failed to load .env file: {e}")
        ))?;
    }
    
    Ok(())
}

/// Check if a file is a SQL file
pub fn is_sql_file(file_path: &Path) -> bool {
    if let Some(extension) = file_path.extension().and_then(|s| s.to_str()) {
        extension.to_lowercase() == "sql"
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_parse_sql_file() {
        let temp_dir = TempDir::new().unwrap();
        let sql_path = temp_dir.path().join("test.sql");
        
        let sql_content = r#"
-- ATTACH 'host=localhost user=testuser password=testpass database=mydb' AS mydb (TYPE mysql);
-- This is a test query
USE mydb;
SELECT product, quantity
FROM products 
WHERE date > '2025-01-01';
"#;
        
        fs::write(&sql_path, sql_content).unwrap();
        
        let parsed = parse_sql_file(&sql_path).unwrap();
        
        assert!(parsed.connection_string.contains("ATTACH"));
        assert!(parsed.connection_string.contains("mysql"));
        assert!(parsed.query.contains("SELECT"));
        assert!(parsed.query.contains("FROM products"));
    }

    #[test]
    fn test_substitute_env_vars() {
        env::set_var("TEST_USER", "myuser");
        env::set_var("TEST_PASS", "mypass");
        
        let connection_string = "host=localhost user={TEST_USER} password={TEST_PASS} database=mydb";
        let result = substitute_env_vars(connection_string).unwrap();
        
        assert_eq!(result, "host=localhost user=myuser password=mypass database=mydb");
    }

    #[test]
    fn test_is_sql_file() {
        assert!(is_sql_file(Path::new("test.sql")));
        assert!(is_sql_file(Path::new("query.SQL")));
        assert!(!is_sql_file(Path::new("test.csv")));
        assert!(!is_sql_file(Path::new("test")));
    }
}