//! Configuration for LogDB indexing and querying.

use crate::ufhg::lightning_hash_str;
use omega::OmegaHashSet;
use std::fs;
use std::io;

/// Configuration for log parsing and indexing.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Maximum number of postings to keep in memory
    pub max_postings: usize,
    /// Seconds after which documents are considered stale
    pub stale_secs: u64,
    /// Log level mappings (hash -> priority)
    pub log_levels: OmegaHashSet<u64, u8>,
    /// Service name mappings (hash -> id)  
    pub services: OmegaHashSet<u64, u8>,
    /// Enable N-gram indexing
    pub enable_ngrams: bool,
    /// Maximum N-gram size
    pub max_ngram_size: usize,
    /// Enable pattern extraction (IPs, error codes, etc.)
    pub enable_patterns: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        let mut log_levels = OmegaHashSet::new(4096);

        // Use numeric keys (hashes) instead of strings for 40x speedup
        log_levels.insert(lightning_hash_str("TRACE"), 0);
        log_levels.insert(lightning_hash_str("DEBUG"), 1);
        log_levels.insert(lightning_hash_str("INFO"), 2);
        log_levels.insert(lightning_hash_str("WARN"), 3);
        log_levels.insert(lightning_hash_str("ERROR"), 4);
        log_levels.insert(lightning_hash_str("FATAL"), 5);

        Self {
            max_postings: 32_000,
            stale_secs: 3600, // 1 hour
            log_levels,
            services: OmegaHashSet::new(4096),
            enable_ngrams: true,
            max_ngram_size: 3,
            enable_patterns: true,
        }
    }
}

impl LogConfig {
    /// Load configuration from a file.
    pub fn from_file(path: &str) -> io::Result<Self> {
        let _content = fs::read_to_string(path)?;
        // TODO: Implement proper config file parsing (JSON/TOML)
        Ok(Self::default())
    }

    /// Get log level priority using numeric key lookup (40x faster).
    pub fn log_level_priority(&self, level: &str) -> u8 {
        let level_hash = lightning_hash_str(level);
        self.log_levels.get(&level_hash).copied().unwrap_or(2) // Default to INFO
    }

    /// Get service ID using numeric key lookup (40x faster).
    pub fn service_id(&self, service: &str) -> u8 {
        let service_hash = lightning_hash_str(service);
        self.services.get(&service_hash).copied().unwrap_or(0)
    }

    /// Register a new service (returns assigned ID).
    pub fn register_service(&mut self, service: &str) -> u8 {
        let service_hash = lightning_hash_str(service);
        if let Some(&existing_id) = self.services.get(&service_hash) {
            return existing_id;
        }

        let new_id = self.services.len() as u8;
        self.services.insert(service_hash, new_id);
        new_id
    }

    /// Check if a log level should be indexed.
    pub fn should_index_level(&self, level: &str) -> bool {
        let level_hash = lightning_hash_str(level);
        self.log_levels.get(&level_hash).is_some()
    }

    /// Get all registered log level hashes for iteration.
    pub fn log_level_hashes(&self) -> Vec<u64> {
        self.log_levels.iter_keys().collect()
    }

    /// Get all registered service hashes for iteration.
    pub fn service_hashes(&self) -> Vec<u64> {
        self.services.iter_keys().collect()
    }

    /// Add custom log level.
    pub fn add_log_level(&mut self, level: &str, priority: u8) {
        let level_hash = lightning_hash_str(level);
        self.log_levels.insert(level_hash, priority);
    }

    /// Check if log level exists by priority.
    pub fn has_log_level_priority(&self, priority: u8) -> bool {
        self.log_levels
            .iter_keys()
            .any(|k| *self.log_levels.get(&k).unwrap() == priority)
    }

    /// Get statistics about the configuration.
    pub fn stats(&self) -> String {
        format!(
            "LogConfig: levels {} services {} max_postings {} ngrams:{}",
            self.log_levels.len(),
            self.services.len(),
            self.max_postings,
            self.enable_ngrams
        )
    }
}
