#![deny(clippy::all)]

//! # MicroSearch Library
//!
//! This module serves as the main entry point for the MicroSearch library when used as
//! a native Node.js addon. It exposes the `MicroSearch` struct and its associated
//! methods to JavaScript, allowing Node.js applications to leverage the high-performance
//! search and indexing capabilities of the underlying `LogDB`.

use napi::bindgen_prelude::*;
use napi_derive::napi;

// Import the necessary modules from the crate.
mod config;
mod logdb;
mod ufhg;
mod utils;

// Use the LogDB implementation, which provides the core functionality.
use logdb::LogDB;

/// A high-performance, in-memory search engine exposed as a Node.js addon.
///
/// The `MicroSearch` struct wraps the `LogDB`, providing a simplified interface for
/// creating, updating, and querying documents. This struct is designed to be
/// instantiated and used from JavaScript code.
#[napi]
pub struct MicroSearch {
    /// The underlying `LogDB` instance that handles the actual search and indexing logic.
    inner: LogDB,
}

#[napi]
impl MicroSearch {
    /// Creates a new instance of `MicroSearch`.
    ///
    /// This constructor initializes a new `LogDB` with default settings and wraps it
    /// in a `MicroSearch` struct, making it available for use in a Node.js environment.
    ///
    /// # Returns
    /// A `Result` containing the new `MicroSearch` instance or an error if initialization fails.
    #[napi(constructor)]
    pub fn new() -> Result<Self> {
        Ok(Self {
            inner: LogDB::new(),
        })
    }

    /// Inserts or updates a simple document with the given content.
    ///
    /// This method provides a straightforward way to add content to the search index
    /// without specifying additional metadata like log level or service.
    ///
    /// # Arguments
    /// * `content` - The string content of the document to be indexed.
    ///
    /// # Returns
    /// A `Result` containing the document ID as a string, or an error if the operation fails.
    #[napi]
    pub fn upsert_simple(&mut self, content: String) -> Result<String> {
        let doc_id = self.inner.upsert_simple(&content);
        Ok(doc_id.to_string())
    }

    /// Inserts or updates a log entry with additional metadata.
    ///
    /// This method allows for the indexing of structured log data, including log level
    /// and service name, which can be used for more advanced filtering and querying.
    ///
    /// # Arguments
    /// * `content` - The main content of the log entry.
    /// * `level` - An optional string specifying the log level (e.g., "INFO", "ERROR").
    /// * `service` - An optional string specifying the service name.
    ///
    /// # Returns
    /// A `Result` containing the document ID as a string, or an error if the operation fails.
    #[napi]
    pub fn upsert_log(
        &mut self,
        content: String,
        level: Option<String>,
        service: Option<String>,
    ) -> Result<String> {
        let doc_id = self.inner.upsert_log(&content, level, service);
        Ok(doc_id.to_string())
    }

    /// Executes a search query and returns a list of matching document IDs.
    ///
    /// # Arguments
    /// * `query` - The search query string.
    ///
    /// # Returns
    /// A `Result` containing a vector of document IDs as strings, or an error if the query fails.
    #[napi]
    pub fn query(&self, query: String) -> Result<Vec<String>> {
        let results = self.inner.query(&query);
        Ok(results.into_iter().map(|id| id.to_string()).collect())
    }

    /// Executes a search query and returns the full content of matching documents.
    ///
    /// # Arguments
    /// * `query` - The search query string.
    ///
    /// # Returns
    /// A `Result` containing a vector of document content strings, or an error if the query fails.
    #[napi]
    pub fn query_content(&self, query: String) -> Result<Vec<String>> {
        Ok(self.inner.query_content(&query))
    }
}
