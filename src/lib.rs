//! # LogDB
//!
//! Ultra-fast in-memory log search engine with microsecond query performance.
//!
//!
pub mod codec;
pub mod config;
pub mod logdb;
pub mod query;
pub mod types;
pub mod ufhg;

// Re-export main types
pub use config::LogConfig;
pub use logdb::LogDB;
pub use query::QueryNode;
pub use types::{LogEntry, TokenMode};

// Re-export for advanced usage
pub use codec::{decode, encode_diff, encode_full, Frame};
pub use ufhg::UFHGHeadquarters;
