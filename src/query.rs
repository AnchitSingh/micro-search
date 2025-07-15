//! Query parsing and execution for LogDB.

use crate::config::LogConfig;

/// AST node for parsed queries.
#[derive(Debug, Clone)]
pub enum QueryNode {
    /// Simple term search
    Term(String),
    /// Exact phrase search
    Phrase(String),
    /// Field-specific search (level:ERROR)
    FieldTerm(&'static str, String),
    /// Numeric range (timestamp:>=1234567890)
    NumericRange(&'static str, u64, u64),
    /// Contains search for unstructured text
    Contains(String),
    /// N-gram search
    NGram(Vec<String>),
    /// Fuzzy matching
    Fuzzy(String, u8),
    /// Regex pattern
    Regex(String),
    /// Logical AND
    And(Vec<QueryNode>),
    /// Logical OR
    Or(Vec<QueryNode>),
    /// Logical NOT
    Not(Box<QueryNode>),
}

/// Parse a query string into an AST.
pub fn parse_query(q: &str, config: &LogConfig) -> QueryNode {
    let mut nodes = Vec::new();
    let mut it = q.split_whitespace().peekable();

    while let Some(tok) = it.next() {
        if tok.contains(':') {
            let mut sp = tok.splitn(2, ':');
            let field = sp.next().unwrap();
            let mut val = sp.next().unwrap().to_string();

            // Handle quoted values
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
                "phrase" => nodes.push(QueryNode::Phrase(val)),
                "fuzzy" => {
                    if let Some((word, dist)) = val.split_once('~') {
                        let distance = dist.parse::<u8>().unwrap_or(1);
                        nodes.push(QueryNode::Fuzzy(word.to_string(), distance));
                    } else {
                        nodes.push(QueryNode::Fuzzy(val, 1));
                    }
                }
                "regex" => nodes.push(QueryNode::Regex(val)),
                "timestamp" => {
                    if let Some(lo) = val.strip_prefix(">=") {
                        let lo = lo.parse::<u64>().unwrap_or(0);
                        nodes.push(QueryNode::NumericRange("timestamp", lo, u64::MAX));
                    } else if let Some(hi) = val.strip_prefix("<=") {
                        let hi = hi.parse::<u64>().unwrap_or(u64::MAX);
                        nodes.push(QueryNode::NumericRange("timestamp", 0, hi));
                    } else {
                        let ts = val.parse::<u64>().unwrap_or(0);
                        nodes.push(QueryNode::NumericRange("timestamp", ts, ts));
                    }
                }
                _ => nodes.push(QueryNode::FieldTerm("unknown", val)),
            }
        } else if tok.starts_with('"') {
            let phrase = tok.trim_matches('"').to_string();
            nodes.push(QueryNode::Phrase(phrase));
        } else {
            // Handle logical operators
            match tok.to_uppercase().as_str() {
                "AND" => continue, // Default is AND
                "OR" => {
                    // Convert last node and next node to OR
                    if let Some(last) = nodes.pop() {
                        if let Some(next_tok) = it.next() {
                            let next_node = if next_tok.contains(':') {
                                // Parse field term
                                QueryNode::Term(next_tok.to_string()) // Simplified
                            } else {
                                QueryNode::Term(next_tok.to_string())
                            };
                            nodes.push(QueryNode::Or(vec![last, next_node]));
                        }
                    }
                }
                "NOT" => {
                    if let Some(next_tok) = it.next() {
                        let next_node = QueryNode::Term(next_tok.to_string());
                        nodes.push(QueryNode::Not(Box::new(next_node)));
                    }
                }
                _ => nodes.push(QueryNode::Term(tok.to_string())),
            }
        }
    }

    if nodes.len() == 1 {
        nodes.pop().unwrap()
    } else if nodes.is_empty() {
        QueryNode::Term("".to_string())
    } else {
        QueryNode::And(nodes)
    }
}
