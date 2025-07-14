use omega::oufh::lightning_hash_str_64;
use omega::oufh::omega_hash_u64_minimal;
use omega::OmegaHashSet;

#[inline(always)]
pub fn lightning_hash_str(s: &str) -> u64 {
    if s.is_empty() {
        return 0;
    }
    let mut result = 0u64;
    let mut has_special = false;
    for &byte in s.as_bytes() {
        let pos = match byte {
            b'a'..=b'z' => byte - b'a' + 1,
            b'A'..=b'Z' => byte - b'A' + 27,
            _ => {
                has_special = true;
                break;
            }
        };
        result = if pos < 10 {
            result * 10 + pos as u64
        } else {
            result * 100 + pos as u64
        };
    }
    if has_special {
        return lightning_hash_str_64(s);
    }
    result
}

#[derive(Debug, Clone)]
pub enum HashToken {
    Word(u64),
    Sequence(u64),
    Prediction(u64),
}

#[derive(Debug)]
pub struct CompressionResult {
    pub tokens: Vec<HashToken>,

    pub original_bytes: usize,

    pub compressed_bytes: usize,

    pub compression_ratio: f32,

    pub predictions_used: usize,

    pub sequences_used: usize,
}

#[derive(Debug, Clone)]
pub struct UFHGHeadquarters {
    word_hashes: Vec<u64>,
    word_bounds: Vec<(usize, usize)>,
    transitions: OmegaHashSet<u64, Vec<u64>>,
    sequences: OmegaHashSet<u64, (Vec<u64>, u64)>,
    predictions: OmegaHashSet<u64, (u64, u64)>,
    hash_to_word: OmegaHashSet<u64, String>,
    total_messages: u64,
    total_bandwidth_saved: usize,
}

impl UFHGHeadquarters {
    pub fn new() -> Self {
        Self {
            word_hashes: Vec::with_capacity(64),
            word_bounds: Vec::with_capacity(64),
            transitions: OmegaHashSet::new(4),
            sequences: OmegaHashSet::new(4),
            predictions: OmegaHashSet::new(4),
            hash_to_word: OmegaHashSet::new(4),
            total_messages: 0,
            total_bandwidth_saved: 0,
        }
    }

    #[inline(always)]
    pub fn lightning_hash_str(&mut self, s: &str) -> u64 {
        if s.is_empty() {
            return 0;
        }
        let mut result = 0u64;
        let mut has_special = false;
        for &byte in s.as_bytes() {
            let pos = match byte {
                b'a'..=b'z' => byte - b'a' + 1,
                b'A'..=b'Z' => byte - b'A' + 27,
                _ => {
                    has_special = true;
                    break;
                }
            };
            result = if pos < 10 {
                result * 10 + pos as u64
            } else {
                result * 100 + pos as u64
            };
        }
        if has_special {
            return lightning_hash_str_64(s);
        }
        result
    }

    #[inline(always)]
    pub fn string_to_u64_to_seq_hash(&self, s: &str) -> u64 {
        let bytes = s.as_bytes();
        let mut i = 0;
        let mut seq_hash: u64 = 0;
        while i < bytes.len() {
            let byte = bytes[i];
            let is_whitespace = byte == b' ' || byte == b'\t' || byte == b'\n' || byte == b'\r';
            if !is_whitespace {
                let start = i;
                while i < bytes.len()
                    && bytes[i] != b' '
                    && bytes[i] != b'\t'
                    && bytes[i] != b'\n'
                    && bytes[i] != b'\r'
                {
                    i += 1;
                }
                let word_slice = unsafe { std::str::from_utf8_unchecked(&bytes[start..i]) };
                if !word_slice.is_empty() {
                    seq_hash = (lightning_hash_str(word_slice))
                        .wrapping_mul(31)
                        .wrapping_add(seq_hash);
                }
            }
        }
        seq_hash
    }

    #[inline(always)]
    pub fn tokenize_zero_copy(&mut self, message: &str) -> (Vec<u64>, Vec<(usize, usize)>) {
        self.word_hashes.clear();
        self.word_bounds.clear();
        if message.is_empty() {
            return (vec![], vec![]);
        }
        let bytes = message.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let byte = bytes[i];
            let is_whitespace = byte == b' ' || byte == b'\t' || byte == b'\n' || byte == b'\r';
            if is_whitespace {
                let start = i;
                let mut whitespace_count = 0u64;
                while i < bytes.len() {
                    let b = bytes[i];
                    if b == b' ' || b == b'\t' || b == b'\n' || b == b'\r' {
                        whitespace_count += 1;
                        i += 1;
                    } else {
                        break;
                    }
                }
                let hash = process_whitespace_len(whitespace_count);
                self.word_hashes.push(hash);
                self.word_bounds.push((start, i));
                if self.hash_to_word.get(&hash).is_none() {
                    let whitespace_str = unsafe { std::str::from_utf8_unchecked(&bytes[start..i]) };
                    self.hash_to_word.insert(hash, whitespace_str.to_string());
                }
            } else {
                let start = i;
                while i < bytes.len()
                    && bytes[i] != b' '
                    && bytes[i] != b'\t'
                    && bytes[i] != b'\n'
                    && bytes[i] != b'\r'
                {
                    i += 1;
                }
                let word_slice = unsafe { std::str::from_utf8_unchecked(&bytes[start..i]) };
                if !word_slice.is_empty() {
                    let hash = self.lightning_hash_str(word_slice);
                    self.word_hashes.push(hash);
                    self.word_bounds.push((start, i));
                }
            }
        }
        (self.word_hashes.clone(), self.word_bounds.clone())
    }

    pub fn learn(&mut self, message: &str) {
        let (hashes, bounds) = self.tokenize_zero_copy(message);
        if hashes.is_empty() {
            return;
        }
        let message_bytes = message.as_bytes();
        for (i, &hash) in hashes.iter().enumerate() {
            let (start, end) = bounds[i];
            let slice = unsafe { std::str::from_utf8_unchecked(&message_bytes[start..end]) };
            if slice.chars().all(|c| c.is_whitespace()) {
                continue;
            } else if self.hash_to_word.get(&hash).is_none() {
                self.hash_to_word.insert(hash, slice.to_string());
            }
        }
        let mut new_transitions = Vec::new();
        let mut existing_updates = Vec::new();
        for window in hashes.windows(2) {
            let from_hash = window[0];
            let to_hash = window[1];
            if self.transitions.get(&from_hash).is_some() {
                existing_updates.push((from_hash, to_hash));
            } else {
                let mut new_vec = Vec::with_capacity(8);
                new_vec.push(to_hash);
                new_transitions.push((from_hash, new_vec));
            }
        }
        self.transitions.insert_batch(new_transitions);
        for (from_hash, to_hash) in existing_updates {
            if let Some(transitions) = self.transitions.get_mut(&from_hash) {
                transitions.push(to_hash);
            }
        }
        for len in 3..=12 {
            if hashes.len() >= len {
                for window in hashes.windows(len) {
                    let seq_hash = self.sequence_hash(window);
                    match self.sequences.get_mut(&seq_hash) {
                        Some((_, count)) => *count += 1,
                        None => {
                            self.sequences.insert(seq_hash, (window.to_vec(), 1));
                        }
                    }
                }
            }
        }
        for window in hashes.windows(2) {
            let from_hash = window[0];
            let to_hash = window[1];
            let freq = self
                .transitions
                .get(&from_hash)
                .map(|v| v.iter().filter(|&&h| h == to_hash).count() as u64)
                .unwrap_or(0);
            if freq >= 2 {
                match self.predictions.get(&from_hash) {
                    None => {
                        self.predictions.insert(from_hash, (to_hash, freq));
                    }
                    Some((_, curr_freq)) => {
                        if freq > *curr_freq {
                            self.predictions.insert(from_hash, (to_hash, freq));
                        }
                    }
                }
            }
        }
        self.total_messages += 1;
    }

    #[inline(always)]
    pub fn sequence_hash(&self, seq: &[u64]) -> u64 {
        seq.iter()
            .fold(0u64, |acc, &h| acc.wrapping_mul(31).wrapping_add(h))
    }

    pub fn compress(&mut self, message: &str) -> CompressionResult {
        let (hashes, bounds) = self.tokenize_zero_copy(message);
        if hashes.is_empty() {
            return CompressionResult {
                tokens: vec![],
                original_bytes: message.len(),
                compressed_bytes: 0,
                compression_ratio: if message.is_empty() {
                    1.0
                } else {
                    f32::INFINITY
                },
                predictions_used: 0,
                sequences_used: 0,
            };
        }
        let bytes = message.as_bytes();
        for (idx, &hash) in hashes.iter().enumerate() {
            if self.hash_to_word.get(&hash).is_some() {
                continue;
            }
            let (start, end) = bounds[idx];
            let slice = unsafe { std::str::from_utf8_unchecked(&bytes[start..end]) };
            self.hash_to_word.insert(hash, slice.to_string());
        }
        let mut tokens = Vec::with_capacity(hashes.len());
        let mut i = 0;
        let mut predictions_used = 0;
        let mut sequences_used = 0;
        while i < hashes.len() {
            let mut matched = false;
            for len in (3..=12).rev() {
                if i + len <= hashes.len() {
                    let window = &hashes[i..i + len];
                    let seq_hash = self.sequence_hash(window);
                    if let Some((_, freq)) = self.sequences.get(&seq_hash) {
                        if *freq >= 2 {
                            tokens.push(HashToken::Sequence(seq_hash));
                            sequences_used += 1;
                            i += len;
                            matched = true;
                            break;
                        }
                    }
                }
            }
            if !matched && i > 0 {
                let prev_hash = hashes[i - 1];
                if let Some((pred_hash, conf)) = self.predictions.get(&prev_hash) {
                    if *conf >= 2 && i < hashes.len() && hashes[i] == *pred_hash {
                        tokens.push(HashToken::Prediction(*pred_hash));
                        predictions_used += 1;
                        i += 1;
                        matched = true;
                    }
                }
            }
            if !matched {
                tokens.push(HashToken::Word(hashes[i]));
                i += 1;
            }
        }
        let original_bytes = message.len();
        let compressed_bytes = tokens.len() * 4;
        let compression_ratio = if compressed_bytes == 0 {
            f32::INFINITY
        } else {
            original_bytes as f32 / compressed_bytes as f32
        };
        self.total_bandwidth_saved += original_bytes.saturating_sub(compressed_bytes);

        CompressionResult {
            tokens,
            original_bytes,
            compressed_bytes,
            compression_ratio,
            predictions_used,
            sequences_used,
        }
    }

    pub fn decompress_external(&self, result: &CompressionResult) -> String {
        if result.tokens.is_empty() {
            return String::new();
        }
        let mut output = String::with_capacity(result.original_bytes);
        for token in &result.tokens {
            match token {
                HashToken::Word(hash) => {
                    if let Some(word) = self.hash_to_word.get(hash) {
                        output.push_str(word);
                    } else {
                        output.push_str(&format!("{}", hash));
                    }
                }
                HashToken::Sequence(seq_hash) => {
                    if let Some((word_hashes, _)) = self.sequences.get(seq_hash) {
                        for &word_hash in word_hashes {
                            if let Some(word) = self.hash_to_word.get(&word_hash) {
                                output.push_str(word);
                            } else {
                                output.push_str(&format!("{}", word_hash));
                            }
                        }
                    }
                }
                HashToken::Prediction(hash) => {
                    if let Some(word) = self.hash_to_word.get(hash) {
                        output.push_str(word);
                    } else {
                        output.push_str(&format!("{}", hash));
                    }
                }
            }
        }
        output
    }

    pub fn get_performance_stats(&self) -> String {
        let stats = self.hash_to_word.bucket_stats();
        format!(
            "📊 Performance Stats:\n\
                • Messages learned: {}\n\
                • Hash tables: transitions:{} sequences:{} predictions:{}\n\
                • Bandwidth saved: {:.1}KB\n\
                • Bucket stats: {:?}",
            self.total_messages,
            self.transitions.len(),
            self.sequences.len(),
            self.predictions.len(),
            self.total_bandwidth_saved as f64 / 1024.0,
            stats
        )
    }
}

fn process_whitespace_len(len: u64) -> u64 {
    let count = len % 8;
    let mut x = 0_u64;
    for _ in 0..count {
        x = x * 100 + 32;
    }
    x = x * 1000 + len;
    omega_hash_u64_minimal(x)
}
