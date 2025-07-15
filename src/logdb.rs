#![allow(clippy::needless_return)]

use crate::config::LogConfig;
use crate::ufhg::{lightning_hash_str, UFHGHeadquarters};
use omega::omega_timer::elapsed_ns;
use omega::OmegaHashSet;
use smallvec::SmallVec;

pub type Tok = u64;
pub type DocId = u64;

#[derive(Debug, Clone, Default)]
pub struct MetaEntry {
    tokens: Vec<Tok>,
    timestamp: u64,
    level: Option<String>,
    service: Option<String>,
    ts: u64,
    content: String,
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
    docs: OmegaHashSet<DocId, MetaEntry>,
    level_index: OmegaHashSet<Tok, Vec<DocId>>,
    service_index: OmegaHashSet<Tok, Vec<DocId>>,
    next_doc_id: DocId,
    max_postings: usize,
    stale_secs: u64,
    config: LogConfig,
}

#[derive(Debug, Clone)]
pub struct Posting {
    small_docs: SmallVec<[DocId; 4]>,
    large_docs: Option<OmegaHashSet<DocId, ()>>,
}

impl Posting {
    #[inline]
    fn new() -> Self {
        Self {
            small_docs: SmallVec::new(),
            large_docs: None,
        }
    }

    #[inline]
    fn add(&mut self, id: DocId) {
        if let Some(ref mut large) = self.large_docs {
            large.insert(id, ());
        } else if self.small_docs.len() < 128 {
            if !self.small_docs.contains(&id) {
                self.small_docs.push(id);
            }
        } else {
            let mut large = OmegaHashSet::new(512);
            for &doc_id in &self.small_docs {
                large.insert(doc_id, ());
            }
            large.insert(id, ());
            self.large_docs = Some(large);
            self.small_docs.clear();
        }
    }

    #[inline]
    fn remove(&mut self, id: DocId) {
        if let Some(ref mut large) = self.large_docs {
            large.remove(&id);
        } else {
            self.small_docs.retain(|d| *d != id);
        }
    }

    #[inline]
    fn to_set(&self) -> OmegaHashSet<DocId, ()> {
        if let Some(ref large) = self.large_docs {
            large.clone()
        } else {
            let mut set = OmegaHashSet::new(self.small_docs.len().max(8));
            for &id in &self.small_docs {
                set.insert(id, ());
            }
            set
        }
    }

    #[inline]
    fn get_docs(&self) -> Vec<DocId> {
        if let Some(ref large) = self.large_docs {
            large.keys()
        } else {
            self.small_docs.to_vec()
        }
    }

    #[inline]
    fn empty(&self) -> bool {
        if let Some(ref large) = self.large_docs {
            large.is_empty()
        } else {
            self.small_docs.is_empty()
        }
    }

    #[inline]
    fn retain_docs(&mut self, docs: &OmegaHashSet<DocId, MetaEntry>) {
        if let Some(ref mut large) = self.large_docs {
            large.retain(|id, _| docs.get(id).is_some());
        } else {
            self.small_docs.retain(|id| docs.get(id).is_some());
        }
    }
}

impl Default for Posting {
    fn default() -> Self {
        Self::new()
    }
}

impl LogDB {
    pub fn new() -> Self {
        Self {
            ufhg: UFHGHeadquarters::new(),
            postings: OmegaHashSet::new(40000),
            docs: OmegaHashSet::new(50000),
            level_index: OmegaHashSet::new(40000),
            service_index: OmegaHashSet::new(40000),
            next_doc_id: 1,
            max_postings: 32_000,
            stale_secs: 3600,
            config: LogConfig::default(),
        }
    }

    pub fn with_config(config: LogConfig) -> Self {
        Self {
            ufhg: UFHGHeadquarters::new(),
            postings: OmegaHashSet::new(40000),
            docs: OmegaHashSet::new(50000),
            level_index: OmegaHashSet::new(40000),
            service_index: OmegaHashSet::new(40000),
            next_doc_id: 1,
            max_postings: config.max_postings,
            stale_secs: config.stale_secs,
            config,
        }
    }

    pub fn from_config_file(path: &str) -> std::io::Result<Self> {
        let config = LogConfig::from_file(path)?;
        Ok(Self::with_config(config))
    }

    pub fn upsert_log(
        &mut self,
        content: &str,
        level: Option<String>,
        service: Option<String>,
    ) -> DocId {
        let descriptor = match (&level, &service) {
            (Some(l), Some(s)) => format!("level {l} service {s} content {content}"),
            (Some(l), None) => format!("level {l} content {content}"),
            (None, Some(s)) => format!("service {s} content {content}"),
            (None, None) => format!("content {content}"),
        };

        let (_, token_slice_cloned) = self.ufhg.tokenize_zero_copy(&descriptor);
        let timestamp = now_secs();
        let doc_id = self.next_doc_id;
        self.next_doc_id += 1;

        let entry = MetaEntry {
            tokens: token_slice_cloned.clone(),
            timestamp,
            level: level.clone(),
            service: service.clone(),
            ts: timestamp,
            content: content.to_string(),
        };

        self.docs.insert(doc_id, entry);

        // Update postings
        for &tok in &token_slice_cloned {
            self.postings
                .entry(tok)
                .or_insert_with(Posting::new)
                .add(doc_id);
        }

        // Update indexes
        if let Some(ref level_val) = level {
            self.level_index
                .entry(lightning_hash_str(level_val))
                .or_insert_with(Vec::new)
                .push(doc_id);
        }
        if let Some(ref service_val) = service {
            self.service_index
                .entry(lightning_hash_str(service_val))
                .or_insert_with(Vec::new)
                .push(doc_id);
        }

        doc_id
    }

    pub fn upsert_simple(&mut self, content: &str) -> DocId {
        self.upsert_log(content, None, None)
    }

    pub fn query(&self, q: &str) -> Vec<DocId> {
        let ast = parse_query(q, &self.config);
        self.exec(&ast)
    }

    // pub fn get_content(&self, doc_id: &DocId) -> Option<String> {
    //     self.docs.get(doc_id).map(|e| {
    //         format!(
    //             "Log entry {} - level:{:?} service:{:?}",
    //             doc_id, e.level, e.service
    //         )
    //     })
    // }

    pub fn get_content(&self, doc_id: &DocId) -> Option<String> {
        self.docs.get(doc_id).map(|e| e.content.clone())
    }

    pub fn query_content(&self, q: &str) -> Vec<String> {
        let doc_ids = self.query(q);
        doc_ids
            .into_iter()
            .filter_map(|id| self.get_content(&id))
            .collect()
    }

    pub fn query_with_meta(
        &self,
        q: &str,
    ) -> Vec<(DocId, String, Option<String>, Option<String>, u64)> {
        let ast = parse_query(q, &self.config);
        let docs = self.exec(&ast);
        docs.into_iter()
            .filter_map(|id| {
                self.docs.get(&id).map(|e| {
                    (
                        id,
                        e.content.clone(),
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
        let stale_secs = self.stale_secs;

        // Remove stale docs in-place
        self.docs.retain(|_id, entry| now - entry.ts <= stale_secs);

        // Clean up postings for removed docs
        self.postings.retain(|_tok, posting| {
            posting.retain_docs(&self.docs);
            !posting.empty()
        });

        // Rebuild indexes after cleanup
        self.rebuild_indexes();
    }

    pub fn rebuild_indexes(&mut self) {
        self.level_index = self
            .docs
            .create_index_for(|entry| entry.level.as_ref().map(|s| lightning_hash_str(s.as_str())));
        self.service_index = self.docs.create_index_for(|entry| {
            entry
                .service
                .as_ref()
                .map(|s| lightning_hash_str(s.as_str()))
        });
    }

    fn exec(&self, node: &QueryNode) -> Vec<DocId> {
        match node {
            QueryNode::Term(w) | QueryNode::Contains(w) => {
                let hash = lightning_hash_str(w);
                self.postings
                    .get(&hash)
                    .map(|p| p.get_docs())
                    .unwrap_or_default()
            }

            QueryNode::Phrase(p) => {
                let seq_hash = self.ufhg.string_to_u64_to_seq_hash(p);
                self.postings
                    .get(&seq_hash)
                    .map(|p| p.get_docs())
                    .unwrap_or_default()
            }

            QueryNode::FieldTerm(f, v) => match *f {
                "level" => self.filter_by_level(v),
                "service" => self.filter_by_service(v),
                _ => {
                    let field_set = self.get_term_set(&lightning_hash_str(f));
                    let value_set = self.get_term_set(&lightning_hash_str(v));
                    field_set.intersect_with(&value_set).keys()
                }
            },

            QueryNode::And(children) => {
                if children.is_empty() {
                    return Vec::new();
                }

                let mut result_set = self.exec_to_set(&children[0]);
                for child in &children[1..] {
                    let other_set = self.exec_to_set(child);
                    result_set = result_set.intersect_with(&other_set);
                    if result_set.is_empty() {
                        break;
                    }
                }
                result_set.keys()
            }

            QueryNode::Or(children) => {
                if children.is_empty() {
                    return Vec::new();
                }

                let mut result_set = self.exec_to_set(&children[0]);
                for child in &children[1..] {
                    let other_set = self.exec_to_set(child);
                    result_set = result_set.union_with(&other_set);
                }
                result_set.keys()
            }

            QueryNode::Not(child) => {
                let exclude_set = self.exec_to_set(child);
                let all_docs_set = self.create_all_docs_set();
                all_docs_set.fast_difference(&exclude_set).keys()
            }

            _ => Vec::new(),
        }
    }

    fn exec_to_set(&self, node: &QueryNode) -> OmegaHashSet<DocId, ()> {
        let docs = self.exec(node);
        let mut set = OmegaHashSet::new(docs.len().max(8));
        for id in docs {
            set.insert(id, ());
        }
        set
    }

    fn get_term_set(&self, tok: &Tok) -> OmegaHashSet<DocId, ()> {
        self.postings
            .get(tok)
            .map(|p| p.to_set())
            .unwrap_or_else(|| OmegaHashSet::new(1))
    }

    fn create_all_docs_set(&self) -> OmegaHashSet<DocId, ()> {
        let mut set = OmegaHashSet::new(self.docs.len());
        for id in self.docs.iter_keys() {
            set.insert(id, ());
        }
        set
    }

    fn filter_by_level(&self, level: &str) -> Vec<DocId> {
        self.level_index
            .get(&lightning_hash_str(level))
            .cloned()
            .unwrap_or_default()
    }

    fn filter_by_service(&self, service: &str) -> Vec<DocId> {
        self.service_index
            .get(&lightning_hash_str(service))
            .cloned()
            .unwrap_or_default()
    }

    pub fn upsert_token(&mut self, s: impl AsRef<str>) -> Tok {
        let tok = lightning_hash_str(s.as_ref());
        self.postings.entry(tok).or_insert_with(Posting::default);
        tok
    }

    pub fn export_tokens(&self) -> Vec<Tok> {
        self.postings.keys()
    }

    pub fn import_tokens(&mut self, toks: Vec<Tok>) {
        for t in toks {
            self.postings.entry(t).or_insert_with(Posting::default);
        }
    }
}

#[inline]
fn now_secs() -> u64 {
    elapsed_ns() / 1_000_000_000
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
