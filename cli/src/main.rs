//! Main entry point for snapbase CLI

use clap::Parser;
use snapbase_core::duckdb_config;

mod cli;
mod commands;
mod output;
mod progress;

use cli::Cli;
use commands::execute_command;

fn main() {
    // Load environment variables from .env file if present
    if std::path::Path::new(".env").exists() {
        if let Err(e) = dotenv::dotenv() {
            eprintln!("Warning: Failed to load .env file: {e}");
        }
    }

    // Initialize logging
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    // Parse command line arguments
    let cli = Cli::parse();

    // Set up verbose logging if requested
    if cli.verbose {
        log::set_max_level(log::LevelFilter::Debug);
    }

    // Initialize and validate DuckDB configuration
    if let Err(e) = duckdb_config::init_duckdb() {
        eprintln!("{e}");
        std::process::exit(1);
    }

    // Execute the command
    if let Err(e) = execute_command(cli.command, cli.workspace.as_deref()) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
