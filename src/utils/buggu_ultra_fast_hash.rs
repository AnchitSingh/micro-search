/// Primary hash multiplier constant.
/// Derived from a large prime number, chosen for good distribution properties in hashing.
pub const FAST_K1: u64 = 0x517cc1b727220a95;

/// Computes a very fast 64-bit hash for a string slice.
///
/// This function prioritizes speed for string hashing, using optimized
/// byte-reading techniques for various lengths. It leverages `buggu_hash_u64_minimal`
/// for the final mixing.
///
/// # Arguments
/// * `s` - The string slice (`&str`) to hash.
///
/// # Returns
/// A 64-bit hash value.
///
/// # Safety
/// This function uses `unsafe` blocks for direct memory access to read bytes
/// as `u16` chunks. It assumes `bytes.as_ptr()` is valid and that subsequent
/// `add` operations do not go out of bounds for the given `len`.
#[inline(always)]
pub fn lightning_hash_str(s: &str) -> u64 {
    let bytes = s.as_bytes();
    let len = bytes.len();

    if len == 0 {
        return FAST_K1;
    }

    let data: u64 = unsafe {
        match len {
            1 => bytes[0] as u64,
            2..=3 => (bytes.as_ptr() as *const u16).read_unaligned() as u64,
            4..=5 => {
                let chunk1 = (bytes.as_ptr() as *const u16).read_unaligned() as u64;
                let chunk2 = (bytes.as_ptr().add(2) as *const u16).read_unaligned() as u64;
                chunk1 | (chunk2 << 16)
            }
            // This case handles lengths 6 and 7, reading 3 u16 chunks.
            // The commented out section below would handle lengths >= 8, reading 4 u16 chunks.
            _ => {
                // len >= 6
                let chunk1 = (bytes.as_ptr() as *const u16).read_unaligned() as u64;
                let chunk2 = (bytes.as_ptr().add(2) as *const u16).read_unaligned() as u64;
                let chunk3 = (bytes.as_ptr().add(4) as *const u16).read_unaligned() as u64;
                chunk1 | (chunk2 << 16) | (chunk3 << 32)
            }
        }
    };
    buggu_hash_u64_minimal(data)
}


/// Single-instruction hash for `u64` values (theoretical minimum).
///
/// This function performs only a multiplication, relying on the compiler to
/// potentially optimize it into a single CPU instruction for maximum speed.
/// It offers minimal collision resistance.
///
/// # Arguments
/// * `value` - The `u64` integer to hash.
///
/// # Returns
/// A 64-bit hash value.
#[inline(always)]
pub fn buggu_hash_u64_minimal(value: u64) -> u64 {
    value.wrapping_mul(FAST_K1)
}

/// Branchless zero-optimized hash for `u64` values.
///
/// This version avoids conditional branches by using bit manipulation to handle
/// the zero input case, potentially improving performance on some architectures.
///
/// # Arguments
/// * `value` - The `u64` integer to hash.
///
/// # Returns
/// A 64-bit hash value.
#[inline(always)]
pub fn buggu_hash_u64_branchless(value: u64) -> u64 {
    let mask = ((value == 0) as u64).wrapping_neg(); // 0xFFFF... if zero, 0x0000... if not
    let adjusted = value | (mask & 1); // Make it 1 if it was 0, otherwise keep original value
    adjusted.wrapping_mul(FAST_K1) ^ (adjusted >> 32)
}


#[inline(always)]
pub fn lightning_hash_str_64(s: &str) -> u64 {
    let bytes = s.as_bytes();
    let len = bytes.len();

    if len == 0 {
        return FAST_K1;
    }

    let data: u64 = unsafe {
        match len {
            1 => bytes[0] as u64,
            2..=3 => (bytes.as_ptr() as *const u16).read_unaligned() as u64,
            4..=5 => {
                let chunk1 = (bytes.as_ptr() as *const u16).read_unaligned() as u64;
                let chunk2 = (bytes.as_ptr().add(2) as *const u16).read_unaligned() as u64;
                chunk1 | (chunk2 << 16)
            }
            // This case handles lengths 6 and 7, reading 3 u16 chunks.
            // The commented out section below would handle lengths >= 8, reading 4 u16 chunks.
            _ => {
                // len >= 6
                let chunk1 = (bytes.as_ptr() as *const u16).read_unaligned() as u64;
                let chunk2 = (bytes.as_ptr().add(2) as *const u16).read_unaligned() as u64;
                let chunk3 = (bytes.as_ptr().add(4) as *const u16).read_unaligned() as u64;
                chunk1 | (chunk2 << 16) | (chunk3 << 32)
            }
        }
    };
    buggu_hash_u64_minimal(data)
}
