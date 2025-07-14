//! Core types and structures for LogDB.

use std::time::{SystemTime, UNIX_EPOCH};

/// A token representing a piece of information, such as a word or attribute.
pub type Tok = u64;

/// A unique identifier for a document (log entry).
pub type DocId = u64;

/// Tokenization mode for log entries.
#[derive(Debug, Clone, Copy)]
pub enum TokenMode {
    /// Structured field:value parsing only
    Structured,
    /// Full-text with N-grams and pattern extraction
    FullText,
    /// Both structured and full-text
    Mixed,
}

/// Represents a log entry with metadata.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub id: DocId,
    pub content: String,
    pub timestamp: u64,
    pub level: Option<String>,
    pub service: Option<String>,
    pub mode: TokenMode,
}

impl LogEntry {
    pub fn new(content: String, mode: TokenMode) -> Self {
        Self {
            id: 0, // Will be assigned by LogDB
            content,
            timestamp: now_secs(),
            level: None,
            service: None,
            mode,
        }
    }

    pub fn with_metadata(content: String, level: Option<String>, service: Option<String>, mode: TokenMode) -> Self {
        Self {
            id: 0,
            content,
            timestamp: now_secs(),
            level,
            service,
            mode,
        }
    }
}

/// Returns current time in seconds since Unix epoch.
#[inline]
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}