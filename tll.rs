#![allow(clippy::needless_return)]

use crate::config::LogConfig;
use crate::ufhg::{lightning_hash_str, UFHGHeadquarters};
use omega::omega_timer::{elapsed_ns, timer_init};
use omega::OmegaHashSet;
use slab::Slab;

pub type Tok = u64;
pub type DocId = u64;

#[derive(Debug, Clone, Default)]
pub struct Posting {
    pub docs: OmegaHashSet<DocId, ()>,
}

#[derive(Debug, Clone)]
pub struct MetaEntry {
    tokens: Vec<Tok>,
    timestamp: u64,
    level: Option<String>,
    service: Option<String>,
    ts: u64,
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
            docs: OmegaHashSet::new(20_000),
        }
    }

    #[inline]
    fn add(&mut self, id: DocId) {
        self.docs.insert(id, ());
    }

    #[inline]
    fn remove(&mut self, id: DocId) {
        self.docs.remove(&id);
    }

    #[inline]
    fn empty(&self) -> bool {
        self.docs.is_empty()
    }

    #[inline]
    fn contains(&self, id: DocId) -> bool {
        self.docs.get(&id).is_some()
    }
}

impl LogDB {
    pub fn new() -> Self {
        Self {
            ufhg: UFHGHeadquarters::new(),
            postings: OmegaHashSet::new(40000),
            docs: Slab::with_capacity(50000),
            max_postings: 32_000,
            stale_secs: 3600,
            config: LogConfig::default(),
        }
    }

    pub fn with_config(config: LogConfig) -> Self {
        Self {
            ufhg: UFHGHeadquarters::new(),
            postings: OmegaHashSet::new(40000),
            docs: Slab::with_capacity(50000),
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
            (Some(l), Some(s)) => format!("level {} service {} content {}", l, s, content),
            (Some(l), None) => format!("level {} content {}", l, content),
            (None, Some(s)) => format!("service {} content {}", s, content),
            (None, None) => format!("content {}", content),
        };

        let (token_slice, token_slice_cloned) = self.ufhg.tokenize_zero_copy(&descriptor);
        let timestamp = now_secs();
        let doc_id = self.docs.insert(MetaEntry {
            tokens: token_slice_cloned,
            timestamp: timestamp,
            level: level,
            service: service,
            ts: timestamp,
        }) as u64;

        for &tok in &token_slice {
            if let Some(post) = self.postings.get_mut(&tok) {
                post.add(doc_id);
            } else {
                let mut v = Posting::new();
                v.add(doc_id);
                self.postings.insert(tok, v);
            }
        }
        self.evict_if_needed();
        doc_id
    }

    pub fn upsert_simple(&mut self, content: &str) -> DocId {
        self.upsert_log(content, None, None)
    }

    pub fn query(&self, q: &str) -> Vec<DocId> {
        let ast = parse_query(q, &self.config);
        self.exec(&ast)
    }

    pub fn get_content(&self, doc_id: DocId) -> Option<String> {
        self.docs.get(doc_id as usize).map(|e| {
            format!("Log entry {} - level:{:?} service:{:?}", 
                   doc_id, e.level, e.service)
        })
    }

    pub fn query_content(&self, q: &str) -> Vec<String> {
        let doc_ids = self.query(q);
        doc_ids.into_iter()
            .filter_map(|id| self.get_content(id))
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
                        .iter_keys()
                        .next()
                        .and_then(|d| self.docs.get(d as usize).map(|e| (tok, e.ts)))
                })
            })
            .collect();
        oldest.sort_by_key(|(_, ts)| *ts);
        for (tok, _) in oldest.into_iter().take(over) {
            self.postings.remove(&tok);
        }
    }

    #[inline]
    fn postings_set(&self, tok: Tok) -> Option<&OmegaHashSet<DocId, ()>> {
        self.postings.get(&tok).map(|p| &p.docs)
    }

    fn exec_set(&self, node: &QueryNode) -> OmegaHashSet<DocId, ()> {
        match node {
            QueryNode::Term(w) => {
                let hash = lightning_hash_str(w);
                match self.postings_set(hash) {
                    Some(set) => set.clone(),
                    None => OmegaHashSet::new(0),
                }
            }
            QueryNode::Contains(w) => {
                let hash = lightning_hash_str(w);
                match self.postings_set(hash) {
                    Some(set) => set.clone(),
                    None => OmegaHashSet::new(0),
                }
            }
            QueryNode::Phrase(p) => {
                let seq_hash = self.ufhg.string_to_u64_to_seq_hash(p);
                match self.postings_set(seq_hash) {
                    Some(set) => set.clone(),
                    None => OmegaHashSet::new(0),
                }
            }
            QueryNode::FieldTerm(f, v) => match *f {
                "level" => {
                    let mut result = OmegaHashSet::new(100);
                    for (id, e) in self.docs.iter() {
                        if e.level.as_deref() == Some(v) {
                            result.insert(id as u64, ());
                        }
                    }
                    result
                }
                "service" => {
                    let mut result = OmegaHashSet::new(100);
                    for (id, e) in self.docs.iter() {
                        if e.service.as_deref() == Some(v) {
                            result.insert(id as u64, ());
                        }
                    }
                    result
                }
                _ => {
                    let f_hash = lightning_hash_str(f);
                    let v_hash = lightning_hash_str(v);
                    match (self.postings_set(f_hash), self.postings_set(v_hash)) {
                        (Some(a), Some(b)) => intersect_sets(a, b),
                        _ => OmegaHashSet::new(0),
                    }
                }
            },
            QueryNode::NumericRange("timestamp", lo, hi) => {
                let mut result = OmegaHashSet::new(100);
                for (id, e) in self.docs.iter() {
                    if e.timestamp >= *lo && e.timestamp <= *hi {
                        result.insert(id as u64, ());
                    }
                }
                result
            }
            QueryNode::And(xs) => {
                if xs.is_empty() {
                    return OmegaHashSet::new(0);
                }
                xs.iter()
                    .skip(1)
                    .fold(self.exec_set(&xs[0]), |acc, n| intersect_sets(&acc, &self.exec_set(n)))
            }
            QueryNode::Or(xs) => {
                if xs.is_empty() {
                    return OmegaHashSet::new(0);
                }
                xs.iter()
                    .skip(1)
                    .fold(self.exec_set(&xs[0]), |acc, n| union_sets(&acc, &self.exec_set(n)))
            }
            QueryNode::Not(x) => {
                let excl = self.exec_set(x);
                let mut all = OmegaHashSet::new(self.docs.len());
                for i in 0..self.docs.len() as u64 {
                    all.insert(i, ());
                }
                difference_sets(&all, &excl)
            }
            _ => OmegaHashSet::new(0),
        }
    }

    fn exec(&self, node: &QueryNode) -> Vec<DocId> {
        self.exec_set(node).iter_keys().collect()
    }

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

// O(1) hash set operations
#[inline]
fn intersect_sets(a: &OmegaHashSet<DocId, ()>, b: &OmegaHashSet<DocId, ()>) -> OmegaHashSet<DocId, ()> {
    let mut result = OmegaHashSet::new(a.len().min(b.len()));
    
    let (small, large) = if a.len() < b.len() { (a, b) } else { (b, a) };
    
    for id in small.iter_keys() {
        if large.get(&id).is_some() {  // O(1) lookup!
            result.insert(id, ());
        }
    }
    result
}

#[inline]
fn union_sets(a: &OmegaHashSet<DocId, ()>, b: &OmegaHashSet<DocId, ()>) -> OmegaHashSet<DocId, ()> {
    let mut result = OmegaHashSet::new(a.len() + b.len());
    
    for id in a.iter_keys() {
        result.insert(id, ());
    }
    for id in b.iter_keys() {
        result.insert(id, ());
    }
    result
}

#[inline]
fn difference_sets(a: &OmegaHashSet<DocId, ()>, b: &OmegaHashSet<DocId, ()>) -> OmegaHashSet<DocId, ()> {
    let mut result = OmegaHashSet::new(a.len());
    
    for id in a.iter_keys() {
        if b.get(&id).is_none() {  // O(1) lookup!
            result.insert(id, ());
        }
    }
    result
}

#[inline]
fn now_secs() -> u64 {
    elapsed_ns() / 1_000_000_000
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