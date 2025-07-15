#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi_derive::napi;

mod config;
mod ufhg;
mod logdb;
mod utils;
use logdb::LogDB;

#[napi]
pub struct MicroSearch {
    inner: LogDB,
}

#[napi]
impl MicroSearch {
    #[napi(constructor)]
    pub fn new() -> Result<Self> {
        Ok(Self {
            inner: LogDB::new(),
        })
    }

    #[napi]
    pub fn upsert_simple(&mut self, content: String) -> Result<String> {
        let doc_id = self.inner.upsert_simple(&content);
        Ok(doc_id.to_string())
    }

    #[napi]
    pub fn upsert_log(&mut self, content: String, level: Option<String>, service: Option<String>) -> Result<String> {
        let doc_id = self.inner.upsert_log(&content, level, service);
        Ok(doc_id.to_string())
    }

    #[napi]
    pub fn query(&self, query: String) -> Result<Vec<String>> {
        let results = self.inner.query(&query);
        Ok(results.into_iter().map(|id| id.to_string()).collect())
    }

    #[napi]
    pub fn query_content(&self, query: String) -> Result<Vec<String>> {
        Ok(self.inner.query_content(&query))
    }
}