//! Progress reporting utilities

use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

/// Progress reporter for snapbase operations
#[derive(Debug)]
pub struct ProgressReporter {
    pub schema_pb: Option<ProgressBar>,
    pub rows_pb: Option<ProgressBar>,
    pub columns_pb: Option<ProgressBar>,
    pub archive_pb: Option<ProgressBar>,
    pub upload_pb: Option<ProgressBar>,
    show_progress: bool,
}

impl ProgressReporter {
    /// Create progress reporter for snapshot creation
    pub fn new_for_snapshot() -> Self {
        // Only create the first progress bar (schema analysis)
        let schema_pb = create_spinner("Analyzing schema...");

        Self {
            schema_pb: Some(schema_pb),
            rows_pb: None,
            columns_pb: None,
            archive_pb: None,
            upload_pb: None,
            show_progress: true,
        }
    }

    /// Lazily create rows progress bar when needed (disabled for cleaner output)
    fn ensure_rows_pb(&mut self) {
        // Disabled: progress bar conflicts with text-based progress reporting
        // Text-based progress in data.rs provides cleaner output
    }

    /// Create upload progress bar
    pub fn create_upload_progress(
        &mut self,
        file_size: u64,
        message: &str,
    ) -> Option<&ProgressBar> {
        if self.show_progress {
            self.upload_pb = Some(create_file_progress(file_size, message));
            self.upload_pb.as_ref()
        } else {
            None
        }
    }

    /// Finish schema analysis and prepare for row processing
    pub fn finish_schema(&mut self, message: &str) {
        if let Some(pb) = self.schema_pb.take() {
            pb.finish_with_message(message.to_string());
        }
        // Immediately create the rows progress bar for large datasets
        self.ensure_rows_pb();
    }

    /// Finish row processing
    pub fn finish_rows(&mut self, message: &str) {
        // Simply print the completion message since we're not using progress bars for rows
        println!("  {message}");
    }

    /// Finish upload progress
    pub fn finish_upload(&mut self, message: &str) {
        if let Some(pb) = self.upload_pb.take() {
            pb.finish_with_message(message.to_string());
        }
    }
}

impl Drop for ProgressReporter {
    fn drop(&mut self) {
        // Ensure all progress bars are cleaned up silently
        if let Some(pb) = self.schema_pb.take() {
            pb.finish_and_clear();
        }
        if let Some(pb) = self.rows_pb.take() {
            pb.finish_and_clear();
        }
        if let Some(pb) = self.columns_pb.take() {
            pb.finish_and_clear();
        }
        if let Some(pb) = self.archive_pb.take() {
            pb.finish_and_clear();
        }
        if let Some(pb) = self.upload_pb.take() {
            pb.finish_and_clear();
        }
    }
}

/// Create a spinner progress bar
fn create_spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
            .template("{spinner:.green} {msg}")
            .expect("Invalid progress template"),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

/// Create a simple progress bar for file operations
pub fn create_file_progress(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes:>7}/{total_bytes:7} {msg}")
            .expect("Invalid progress template")
            .progress_chars("#>-"),
    );
    pb.set_message(message.to_string());
    pb
}
