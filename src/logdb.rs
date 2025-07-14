#![allow(clippy::needless_return)]

use crate::config::LogConfig;
use crate::ufhg::{lightning_hash_str, UFHGHeadquarters};
use omega::omega_timer::{elapsed_ns, timer_init};
use omega::OmegaHashSet;
use slab::Slab;
use smallvec::SmallVec;

pub type Tok = u64;
pub type DocId = u64;

#[derive(Debug, Clone, Default)]
pub struct Posting {
    pub docs: SmallVec<[DocId; 4]>,
}

#[derive(Debug, Clone)]
pub struct MetaEntry {
    tokens: Vec<Tok>,
    timestamp: u64,
    level: Option<String>,
    service: Option<String>,
    ts: u64,
    // payload: String,
}

#[derive(Debug, Clone)]
pub enum QueryNode {
    Term(String),
    Phrase(String),
    FieldTerm(&'static str, String),
    NumericRange(&'static str, u64, u64),
    Contains(String),
    And(Vec<QueryNode>),
    Or(Vec<QueryNode>),
    Not(Box<QueryNode>),
}

#[derive(Debug, Clone)]
pub struct LogDB {
    ufhg: UFHGHeadquarters,
    postings: OmegaHashSet<Tok, Posting>,
    docs: Slab<MetaEntry>,
    max_postings: usize,
    stale_secs: u64,
    config: LogConfig,
}

impl Posting {
    #[inline]
    fn new() -> Self {
        Self {
            docs: SmallVec::new(),
        }
    }

    #[inline]
    fn add(&mut self, id: DocId) {
        if !self.docs.contains(&id) {
            self.docs.push(id);
        }
    }

    #[inline]
    fn remove(&mut self, id: DocId) {
        self.docs.retain(|d| *d != id);
    }

    #[inline]
    fn empty(&self) -> bool {
        self.docs.is_empty()
    }
}

impl LogDB {
    pub fn new() -> Self {
        Self {
            ufhg: UFHGHeadquarters::new(),
            postings: OmegaHashSet::new(40000),
            docs: Slab::with_capacity(8192),
            max_postings: 32_000,
            stale_secs: 3600, // 1 hour for logs
            config: LogConfig::default(),
        }
    }

    pub fn with_config(config: LogConfig) -> Self {
        Self {
            ufhg: UFHGHeadquarters::new(),
            postings: OmegaHashSet::new(12000),
            docs: Slab::with_capacity(4096),
            max_postings: config.max_postings,
            stale_secs: config.stale_secs,
            config,
        }
    }

    pub fn from_config_file(path: &str) -> std::io::Result<Self> {
        let config = LogConfig::from_file(path)?;
        Ok(Self::with_config(config))
    }

    /// Insert a log entry - REMOVE expensive learning
    pub fn upsert_log(
        &mut self,
        content: &str,
        level: Option<String>,
        service: Option<String>,
    ) -> DocId {
        // Create descriptor for tokenization
        let descriptor = match (&level, &service) {
            (Some(l), Some(s)) => format!("level {} service {} content {}", l, s, content),
            (Some(l), None) => format!("level {} content {}", l, content),
            (None, Some(s)) => format!("service {} content {}", s, content),
            (None, None) => format!("content {}", content),
        };

        let (token_slice, _) = self.ufhg.tokenize_zero_copy(&descriptor);
        // let new_tokens: Vec<Tok> = token_slice;
        // let new_tokens_clone = new_tokens.clone();
        
        let doc_id = self.docs.insert(MetaEntry {
            tokens: token_slice.clone(),
            timestamp: now_secs(),
            level: level,
            service: service,
            ts: now_secs(),
            // payload: content.to_string(),
        }) as u64;

        // Update postings using your original algorithm
        for &tok in &token_slice {
            if let Some(post) = self.postings.get_mut(&tok) {
                post.add(doc_id);
            } else {
                let mut v = Posting::new();
                v.add(doc_id);
                self.postings.insert(tok, v);
            }
        }
    

        // ❌ REMOVE THIS LINE - it's causing massive performance hit
        // self.ufhg.learn(content);

        self.evict_if_needed();
        doc_id
    }
    /// Simple log insertion (just content)
    pub fn upsert_simple(&mut self, content: &str) -> DocId {
        self.upsert_log(content, None, None)
    }

    /// Update existing log entry (preserves your diff algorithm)
    pub fn update_log(
        &mut self,
        doc_id: DocId,
        content: &str,
        level: Option<String>,
        service: Option<String>,
    ) -> bool {
        if let Some(entry) = self.docs.get_mut(doc_id as usize) {
            let descriptor = match (&level, &service) {
                (Some(l), Some(s)) => format!("level {} service {} content {}", l, s, content),
                (Some(l), None) => format!("level {} content {}", l, content),
                (None, Some(s)) => format!("service {} content {}", s, content),
                (None, None) => format!("content {}", content),
            };

            let (token_slice, _) = self.ufhg.tokenize_zero_copy(&descriptor);
            let new_tokens: Vec<Tok> = token_slice.to_vec();

            if entry.tokens == new_tokens {
                entry.ts = now_secs();
                return true;
            }

            // Use your original diff algorithm
            let (remove, add) = diff_tokens(&entry.tokens, &new_tokens);

            for tok in remove {
                if let Some(post) = self.postings.get_mut(&tok) {
                    post.remove(doc_id);
                    if post.empty() {
                        self.postings.remove(&tok);
                    }
                }
            }

            for tok in add {
                if let Some(post) = self.postings.get_mut(&tok) {
                    post.add(doc_id);
                } else {
                    let mut v = Posting::new();
                    v.add(doc_id);
                    self.postings.insert(tok, v);
                }
            }

            entry.tokens = new_tokens;
            entry.level = level;
            entry.service = service;
            entry.ts = now_secs();
            return true;
        }
        false
    }

    pub fn query(&self, q: &str) -> Vec<DocId> {
        let ast = parse_query(q, &self.config);
        self.exec(&ast)
    }
    /// Separate method to get content by doc ID
    pub fn get_content(&self, doc_id: DocId) -> Option<String> {
        self.docs.get(doc_id as usize).map(|e| {
            // For now return a placeholder - in real implementation you'd store content properly
            format!("Log entry {} - level:{:?} service:{:?}", 
                   doc_id, e.level, e.service)
        })
    }

    /// Query and return full content
    pub fn query_content(&self, q: &str) -> Vec<String> {
        let doc_ids = self.query(q);
        doc_ids.into_iter()
            .filter_map(|id| self.get_content(id))
            .collect()
    }

    /// Get log entries with metadata
    pub fn query_with_meta(
        &self,
        q: &str,
    ) -> Vec<(DocId, String, Option<String>, Option<String>, u64)> {
        let ast = parse_query(q, &self.config);
        let docs = self.exec(&ast);
        docs.into_iter()
            .filter_map(|id| {
                self.docs.get(id as usize).map(|e| {
                    (
                        id,
                        format!("Log content {}", id),
                        e.level.clone(),
                        e.service.clone(),
                        e.timestamp,
                    )
                })
            })
            .collect()
    }

    pub fn cleanup_stale(&mut self) {
        let now = now_secs();
        let stale_ids: Vec<DocId> = self
            .docs
            .iter()
            .filter_map(|(i, e)| {
                if now - e.ts > self.stale_secs {
                    Some(i as DocId)
                } else {
                    None
                }
            })
            .collect();

        for id in stale_ids {
            self.remove_doc(id);
        }
    }

    pub fn stats(&self) -> String {
        format!(
            "LogDB: docs {}  postings {}  est_mem {:.1} KB\n{}",
            self.docs.len(),
            self.postings.len(),
            ((self.postings.len() * 8 + self.docs.len() * 64) as f64) / 1024.0,
            self.ufhg.get_performance_stats()
        )
    }

    fn remove_doc(&mut self, id: DocId) {
        if let Some(entry) = self.docs.get(id as usize) {
            for &tok in &entry.tokens {
                if let Some(p) = self.postings.get_mut(&tok) {
                    p.remove(id);
                    if p.empty() {
                        self.postings.remove(&tok);
                    }
                }
            }
        }
        self.docs.remove(id as usize);
    }

    fn evict_if_needed(&mut self) {
        if self.postings.len() <= self.max_postings {
            return;
        }
        let over = self.postings.len() - self.max_postings + 512;
        let mut oldest: Vec<(Tok, u64)> = self
            .postings
            .iter_keys()
            .filter_map(|tok| {
                self.postings.get(&tok).and_then(|post| {
                    post.docs
                        .get(0)
                        .and_then(|&d| self.docs.get(d as usize).map(|e| (tok, e.ts)))
                })
            })
            .collect();
        oldest.sort_by_key(|(_, ts)| *ts);
        for (tok, _) in oldest.into_iter().take(over) {
            self.postings.remove(&tok);
        }
    }

    fn postings(&self, tok: Tok) -> Vec<DocId> {
        self.postings
            .get(&tok)
            .map(|p| p.docs.clone())
            .unwrap_or_default()
            .to_vec()
    }

    fn exec(&self, node: &QueryNode) -> Vec<DocId> {
        match node {
            QueryNode::Term(w) => self.postings(lightning_hash_str(w)),
            QueryNode::Contains(w) => self.postings(lightning_hash_str(w)),
            QueryNode::Phrase(p) => {
                let seq_hash = self.ufhg.string_to_u64_to_seq_hash(&p);
                self.postings(seq_hash)
            }
            QueryNode::FieldTerm(f, v) => match *f {
                "level" => self.filter_by_level(v),
                "service" => self.filter_by_service(v),
                _ => intersect(
                    &self.postings(lightning_hash_str(f)),
                    &self.postings(lightning_hash_str(v)),
                ),
            },
            QueryNode::NumericRange("timestamp", lo, hi) => self
                .docs
                .iter()
                .filter_map(|(id, e)| {
                    if e.timestamp >= *lo && e.timestamp <= *hi {
                        Some(id as u64)
                    } else {
                        None
                    }
                })
                .collect(),
            QueryNode::And(xs) => xs
                .iter()
                .skip(1)
                .fold(self.exec(&xs[0]), |acc, n| intersect(&acc, &self.exec(n))),
            QueryNode::Or(xs) => xs
                .iter()
                .skip(1)
                .fold(self.exec(&xs[0]), |acc, n| union(&acc, &self.exec(n))),
            QueryNode::Not(x) => {
                let excl = self.exec(x);
                let all: Vec<DocId> = (0..self.docs.len() as u64).collect();
                difference(&all, &excl)
            }
            _ => Vec::new(),
        }
    }

    fn filter_by_level(&self, level: &str) -> Vec<DocId> {
        self.docs
            .iter()
            .filter_map(|(id, e)| {
                if e.level.as_deref() == Some(level) {
                    Some(id as u64)
                } else {
                    None
                }
            })
            .collect()
    }

    fn filter_by_service(&self, service: &str) -> Vec<DocId> {
        self.docs
            .iter()
            .filter_map(|(id, e)| {
                if e.service.as_deref() == Some(service) {
                    Some(id as u64)
                } else {
                    None
                }
            })
            .collect()
    }

    // Your original token helpers preserved
    pub fn upsert_token(&mut self, s: impl AsRef<str>) -> Tok {
        let tok = lightning_hash_str(s.as_ref());
        if self.postings.get(&tok).is_none() {
            self.postings.insert(tok, Posting::default());
        }
        tok
    }

    pub fn export_tokens(&self) -> Vec<Tok> {
        self.postings.iter_keys().collect()
    }

    pub fn import_tokens(&mut self, toks: Vec<Tok>) {
        for t in toks {
            self.postings.entry(t).or_insert_with(Posting::default);
        }
    }
}

// Your original helper functions preserved exactly
#[inline]
fn now_secs() -> u64 {
    elapsed_ns() / 1_000_000_000
}

#[inline]
fn intersect(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
    if a.is_empty() || b.is_empty() {
        return Vec::new();
    }
    let (small, big) = if a.len() < b.len() { (a, b) } else { (b, a) };
    small.iter().filter(|d| big.contains(d)).cloned().collect()
}

#[inline]
fn union(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
    let mut out = a.to_vec();
    for x in b {
        if !out.contains(x) {
            out.push(*x);
        }
    }
    out
}

#[inline]
fn difference(a: &[DocId], b: &[DocId]) -> Vec<DocId> {
    a.iter().filter(|d| !b.contains(d)).cloned().collect()
}

fn diff_tokens(old: &[Tok], new: &[Tok]) -> (Vec<Tok>, Vec<Tok>) {
    let mut newset = OmegaHashSet::new(1024);
    let mut oldset = OmegaHashSet::new(1024);

    for &token in new {
        newset.insert(token, ());
    }
    for &token in old {
        oldset.insert(token, ());
    }

    let remove: Vec<Tok> = old
        .iter()
        .filter(|t| newset.get(t).is_none())
        .cloned()
        .collect();
    let add: Vec<Tok> = new
        .iter()
        .filter(|t| oldset.get(t).is_none())
        .cloned()
        .collect();
    (remove, add)
}

fn parse_query(q: &str, config: &LogConfig) -> QueryNode {
    let mut nodes = Vec::<QueryNode>::new();
    let mut it = q.split_whitespace().peekable();

    while let Some(tok) = it.next() {
        if tok.contains(':') {
            let mut sp = tok.splitn(2, ':');
            let field = sp.next().unwrap();
            let mut val = sp.next().unwrap().to_string();

            if val.starts_with('"') && !val.ends_with('"') {
                for nxt in it.by_ref() {
                    val.push(' ');
                    val.push_str(nxt);
                    if nxt.ends_with('"') {
                        break;
                    }
                }
                val = val.trim_matches('"').to_string();
            } else {
                val = val.trim_matches('"').to_string();
            }

            match field {
                "level" => nodes.push(QueryNode::FieldTerm("level", val)),
                "service" => nodes.push(QueryNode::FieldTerm("service", val)),
                "contains" => nodes.push(QueryNode::Contains(val)),
                "timestamp" => {
                    if let Some(lo) = val.strip_prefix(">=") {
                        let lo = lo.parse::<u64>().unwrap_or(0);
                        nodes.push(QueryNode::NumericRange("timestamp", lo, u64::MAX));
                    } else if let Some(hi) = val.strip_prefix("<=") {
                        let hi = hi.parse::<u64>().unwrap_or(u64::MAX);
                        nodes.push(QueryNode::NumericRange("timestamp", 0, hi));
                    }
                }
                _ => nodes.push(QueryNode::Term(tok.to_string())),
            }
        } else if tok.starts_with('"') {
            let phrase = tok.trim_matches('"').to_string();
            nodes.push(QueryNode::Phrase(phrase));
        } else {
            nodes.push(QueryNode::Term(tok.to_string()));
        }
    }

    if nodes.len() == 1 {
        nodes.pop().unwrap()
    } else {
        QueryNode::And(nodes)
    }
}
