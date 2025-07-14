//! # LogDB
//! 
//! Ultra-fast in-memory log search engine with microsecond query performance.
//! 
//! ## Examples
//! 
//! ```rust
//! use logdb::{LogDB, TokenMode};
//! 
//! let mut db = LogDB::new();
//! 
//! // Index log lines
//! db.upsert_log("ERROR: Database connection timeout", TokenMode::FullText);
//! db.upsert_log("INFO: User login successful", TokenMode::FullText);
//! 
//! // Query in microseconds
//! let results = db.query("contains:ERROR AND contains:database");
//! ```

pub mod logdb;
pub mod ufhg;
pub mod codec;
pub mod config;
pub mod query;
pub mod types;

// Re-export main types
pub use logdb::LogDB;
pub use config::LogConfig;
pub use types::{TokenMode, LogEntry};
pub use query::QueryNode;

// Re-export for advanced usage
pub use ufhg::UFHGHeadquarters;
pub use codec::{Frame, encode_full, encode_diff, decode};