use omega::oufh::lightning_hash_str_64;
use omega::oufh::omega_hash_u64_minimal;

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
pub struct UFHGHeadquarters {
    word_hashes: Vec<u64>,
}

impl UFHGHeadquarters {
    pub fn new() -> Self {
        Self {
            word_hashes: Vec::with_capacity(64),
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
    pub fn tokenize_zero_copy(&mut self, message: &str) -> (Vec<u64>, Vec<u64>) {
        self.word_hashes.clear();
        if message.is_empty() {
            return (vec![], vec![]);
        }
        let bytes = message.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            let byte = bytes[i];
            let is_whitespace = byte == b' ' || byte == b'\t' || byte == b'\n' || byte == b'\r';
            if is_whitespace {
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
                }
            }
        }
        // Take ownership of the computed hashes, leaving an empty Vec in its place.
        // This avoids cloning twice – we only clone once to create the second copy.
        let hashes = std::mem::take(&mut self.word_hashes);
        let hashes_clone = hashes.clone();
        (hashes_clone, hashes)
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
