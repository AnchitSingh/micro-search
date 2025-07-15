//! # LogDB
//! 
//! Ultra-fast in-memory log search engine with microsecond query performance.
//! 
//! 
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