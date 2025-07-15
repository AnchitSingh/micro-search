use crate::utils::buggu_random_generator::BugguRng;
use crate::utils::buggu_ultra_fast_hash::{lightning_hash_str, buggu_hash_u64_minimal};

const INLINE_BUCKET_SIZE: usize = 4;
const OVERFLOW_BUCKET_SIZE: usize = 8;
// =============================================================================
// TRAIT
// =============================================================================

pub trait BugguHashable: Eq + PartialEq {
    fn buggu_hash(&self) -> u64;
}

// =============================================================================
// IMPLS FOR STRING TYPES
// =============================================================================

impl BugguHashable for &str {
    fn buggu_hash(&self) -> u64 {
        lightning_hash_str(self)
    }
}

impl BugguHashable for String {
    fn buggu_hash(&self) -> u64 {
        lightning_hash_str(self.as_str())
    }
}

// =============================================================================
// NUMERIC TYPES
// =============================================================================

// Macro for numeric types that convert to u64
macro_rules! impl_buggu_hashable_numeric {
    ($($t:ty),*) => {
        $(
            impl BugguHashable for $t {
                fn buggu_hash(&self) -> u64 {
                    buggu_hash_u64_minimal(*self as u64)
                }
            }
        )*
    };
}

impl_buggu_hashable_numeric!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, isize);

// Macro for array/slice types that pack bytes into u64
macro_rules! impl_buggu_hashable_bytes {
    ($($t:ty, $element:ty),*) => {
        $(
            impl BugguHashable for $t {
                fn buggu_hash(&self) -> u64 {
                    let mut num = 0u64;
                    let bytes_to_take = std::cmp::min(8, self.len());
                    for (i, &byte) in self.iter().enumerate().take(bytes_to_take) {
                        num |= (byte as u64) << (i * 8);
                    }
                    buggu_hash_u64_minimal(num)
                }
            }
        )*
    };
}

impl_buggu_hashable_bytes!([u8], u8, [u16], u16, Vec<u8>, u8, Vec<u16>, u16);

// Fixed-size array implementations
impl<const N: usize> BugguHashable for [u8; N] {
    fn buggu_hash(&self) -> u64 {
        let mut num = 0u64;
        let bytes_to_take = std::cmp::min(8, N);
        for i in 0..bytes_to_take {
            num |= (self[i] as u64) << (i * 8);
        }
        buggu_hash_u64_minimal(num)
    }
}

impl<const N: usize> BugguHashable for [u16; N] {
    fn buggu_hash(&self) -> u64 {
        let mut num = 0u64;
        for (i, &byte) in self.iter().enumerate().take(8) {
            num |= (byte as u64) << (i * 8);
        }
        buggu_hash_u64_minimal(num)
    }
}

// Tuple implementations
impl BugguHashable for (u32, u32) {
    fn buggu_hash(&self) -> u64 {
        let mut num = self.1;
        num |= (self.0 as u32) << 8;
        buggu_hash_u64_minimal(num as u64)
    }
}

impl BugguHashable for (i32, i32) {
    fn buggu_hash(&self) -> u64 {
        let mut num = self.1;
        num |= (self.0 as i32) << 8;
        buggu_hash_u64_minimal(num as u64)
    }
}

// =============================================================================
// BUCKET TYPES
// =============================================================================

#[derive(Debug, Clone, Default)]
pub enum BugguBucket<K, V> {
    #[default]
    Empty,
    Inline {
        entries: [(K, V); INLINE_BUCKET_SIZE],
        len: u8,
    },
    Overflow {
        entries: Vec<(K, V)>,
    },
}

// =============================================================================
// ITERATORS
// =============================================================================

#[derive(Debug, Clone)]
pub struct BugguKeyIterator<'a, K, V> {
    storage: &'a [BugguBucket<K, V>],
    bucket_idx: usize,
    entry_idx: usize,
    remaining: usize,
}

impl<'a, K, V> Iterator for BugguKeyIterator<'a, K, V>
where
    K: BugguHashable + Eq + PartialEq + Clone + Default,
    V: Clone + Default,
{
    type Item = K;

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        while self.bucket_idx < self.storage.len() {
            let bucket = unsafe { self.storage.get_unchecked(self.bucket_idx) };
            match bucket {
                BugguBucket::Empty => {
                    self.bucket_idx += 1;
                    self.entry_idx = 0;
                }
                BugguBucket::Inline { entries, len } => {
                    if self.entry_idx < *len as usize {
                        let key = unsafe { entries.get_unchecked(self.entry_idx).0.clone() };
                        self.entry_idx += 1;
                        self.remaining -= 1;
                        return Some(key);
                    } else {
                        self.bucket_idx += 1;
                        self.entry_idx = 0;
                    }
                }
                BugguBucket::Overflow { entries } => {
                    if self.entry_idx < entries.len() {
                        let key = unsafe { entries.get_unchecked(self.entry_idx).0.clone() };
                        self.entry_idx += 1;
                        self.remaining -= 1;
                        return Some(key);
                    } else {
                        self.bucket_idx += 1;
                        self.entry_idx = 0;
                    }
                }
            }
        }
        None
    }
}

pub struct BugguIterMut<'a, K, V> {
    storage: std::slice::IterMut<'a, BugguBucket<K, V>>,
    current_bucket: Option<&'a mut BugguBucket<K, V>>,
    entry_idx: usize,
    remaining: usize,
}

impl<'a, K, V> BugguIterMut<'a, K, V>
where
    K: BugguHashable + Eq + PartialEq + Clone + Default,
    V: Clone + Default,
{
    fn new(storage: &'a mut [BugguBucket<K, V>], remaining: usize) -> Self {
        let mut iter = storage.iter_mut();
        let current_bucket = iter.next();
        Self {
            storage: iter,
            current_bucket,
            entry_idx: 0,
            remaining,
        }
    }
}

impl<'a, K, V> Iterator for BugguIterMut<'a, K, V>
where
    K: BugguHashable + Eq + PartialEq + Clone + Default,
    V: Clone + Default,
{
    type Item = (&'a K, &'a mut V);

    #[inline(always)]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        loop {
            match self.current_bucket.as_mut() {
                Some(bucket) => match bucket {
                    BugguBucket::Empty => {
                        self.current_bucket = self.storage.next();
                        self.entry_idx = 0;
                    }
                    BugguBucket::Inline { entries, len } => {
                        if self.entry_idx < *len as usize {
                            let entry_ptr = entries.as_mut_ptr();
                            unsafe {
                                let entry = &mut *entry_ptr.add(self.entry_idx);
                                let key_ptr = &entry.0 as *const K;
                                let value_ptr = &mut entry.1 as *mut V;
                                self.entry_idx += 1;
                                self.remaining -= 1;
                                return Some((&*key_ptr, &mut *value_ptr));
                            }
                        } else {
                            self.current_bucket = self.storage.next();
                            self.entry_idx = 0;
                        }
                    }
                    BugguBucket::Overflow { entries } => {
                        if self.entry_idx < entries.len() {
                            let entry_ptr = entries.as_mut_ptr();
                            unsafe {
                                let entry = &mut *entry_ptr.add(self.entry_idx);
                                let key_ptr = &entry.0 as *const K;
                                let value_ptr = &mut entry.1 as *mut V;
                                self.entry_idx += 1;
                                self.remaining -= 1;
                                return Some((&*key_ptr, &mut *value_ptr));
                            }
                        } else {
                            self.current_bucket = self.storage.next();
                            self.entry_idx = 0;
                        }
                    }
                },
                None => return None,
            }
        }
    }
}

// Entry API for BugguHashSet
pub enum BugguEntry<'a, K, V>
where
    K: BugguHashable + Eq + PartialEq + Clone + Default,
    V: Clone + Default,
{
    Occupied(BugguOccupiedEntry<'a, K, V>),
    Vacant(BugguVacantEntry<'a, K, V>),
}

pub struct BugguOccupiedEntry<'a, K, V>
where
    K: BugguHashable + Eq + PartialEq + Clone + Default,
    V: Clone + Default,
{
    key: K,
    hashset: &'a mut BugguHashSet<K, V>,
    bucket_idx: usize,
    entry_idx: usize,
}

pub struct BugguVacantEntry<'a, K, V>
where
    K: BugguHashable + Eq + PartialEq + Clone + Default,
    V: Clone + Default,
{
    key: K,
    hashset: &'a mut BugguHashSet<K, V>,
    bucket_idx: usize,
}

impl<'a, K, V> BugguEntry<'a, K, V>
where
    K: BugguHashable + Eq + PartialEq + Clone + Default,
    V: Clone + Default,
{
    #[inline(always)]
    pub fn or_insert(self, default: V) -> &'a mut V {
        match self {
            BugguEntry::Occupied(entry) => entry.into_mut(),
            BugguEntry::Vacant(entry) => entry.insert(default),
        }
    }

    #[inline(always)]
    pub fn or_insert_with<F>(self, default: F) -> &'a mut V
    where
        F: FnOnce() -> V,
    {
        match self {
            BugguEntry::Occupied(entry) => entry.into_mut(),
            BugguEntry::Vacant(entry) => entry.insert(default()),
        }
    }

    #[inline(always)]
    pub fn key(&self) -> &K {
        match self {
            BugguEntry::Occupied(entry) => entry.key(),
            BugguEntry::Vacant(entry) => entry.key(),
        }
    }

    #[inline(always)]
    pub fn and_modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        match self {
            BugguEntry::Occupied(mut entry) => {
                f(entry.get_mut());
                BugguEntry::Occupied(entry)
            }
            BugguEntry::Vacant(entry) => BugguEntry::Vacant(entry),
        }
    }
}

impl<'a, K, V> BugguOccupiedEntry<'a, K, V>
where
    K: BugguHashable + Eq + PartialEq + Clone + Default,
    V: Clone + Default,
{
    #[inline(always)]
    pub fn key(&self) -> &K {
        &self.key
    }

    #[inline(always)]
    pub fn get(&self) -> &V {
        let bucket = unsafe { self.hashset.storage.get_unchecked(self.bucket_idx) };
        match bucket {
            BugguBucket::Inline { entries, .. } => unsafe {
                &entries.get_unchecked(self.entry_idx).1
            },
            BugguBucket::Overflow { entries } => unsafe {
                &entries.get_unchecked(self.entry_idx).1
            },
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    pub fn get_mut(&mut self) -> &mut V {
        let bucket = unsafe { self.hashset.storage.get_unchecked_mut(self.bucket_idx) };
        match bucket {
            BugguBucket::Inline { entries, .. } => unsafe {
                &mut entries.get_unchecked_mut(self.entry_idx).1
            },
            BugguBucket::Overflow { entries } => unsafe {
                &mut entries.get_unchecked_mut(self.entry_idx).1
            },
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    pub fn into_mut(self) -> &'a mut V {
        let bucket = unsafe { self.hashset.storage.get_unchecked_mut(self.bucket_idx) };
        match bucket {
            BugguBucket::Inline { entries, .. } => unsafe {
                &mut entries.get_unchecked_mut(self.entry_idx).1
            },
            BugguBucket::Overflow { entries } => unsafe {
                &mut entries.get_unchecked_mut(self.entry_idx).1
            },
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    pub fn insert(&mut self, value: V) -> V {
        std::mem::replace(self.get_mut(), value)
    }
}

impl<'a, K, V> BugguVacantEntry<'a, K, V>
where
    K: BugguHashable + Eq + PartialEq + Clone + Default,
    V: Clone + Default,
{
    #[inline(always)]
    pub fn key(&self) -> &K {
        &self.key
    }

    #[inline(always)]
    pub fn insert(self, value: V) -> &'a mut V {
        let bucket_idx = self.bucket_idx;
        let bucket = unsafe { self.hashset.storage.get_unchecked_mut(bucket_idx) };

        match bucket {
            BugguBucket::Empty => {
                let mut entries = core::array::from_fn(|_| (K::default(), V::default()));
                entries[0] = (self.key, value);
                *bucket = BugguBucket::Inline { entries, len: 1 };
                self.hashset.count += 1;
            }
            BugguBucket::Inline { entries, len } => {
                let current_len = *len as usize;
                if current_len < INLINE_BUCKET_SIZE {
                    entries[current_len] = (self.key, value);
                    *len += 1;
                    self.hashset.count += 1;
                } else {
                    let mut overflow_vec = Vec::with_capacity(OVERFLOW_BUCKET_SIZE);
                    for item in entries.iter_mut().take(INLINE_BUCKET_SIZE) {
                        overflow_vec.push(std::mem::take(item));
                    }
                    overflow_vec.push((self.key, value));
                    *bucket = BugguBucket::Overflow {
                        entries: overflow_vec,
                    };
                    self.hashset.count += 1;
                }
            }
            BugguBucket::Overflow { entries } => {
                if entries.len() == entries.capacity() {
                    entries.reserve(entries.capacity());
                }
                entries.push((self.key, value));
                self.hashset.count += 1;
            }
        }

        // Get the reference after all modifications are done
        let bucket = unsafe { self.hashset.storage.get_unchecked_mut(bucket_idx) };
        match bucket {
            BugguBucket::Inline { entries, len } => unsafe {
                &mut entries.get_unchecked_mut((*len as usize) - 1).1
            },
            BugguBucket::Overflow { entries } => {
                let len = entries.len();
                unsafe { &mut entries.get_unchecked_mut(len - 1).1 }
            }
            _ => unreachable!(),
        }
    }
}

// =============================================================================
// MAIN HASHSET IMPLEMENTATION
// =============================================================================

#[derive(Debug, Clone, Default)]
pub struct BugguHashSet<K, V = ()>
where
    K: BugguHashable + Eq + PartialEq + Clone + Default,
    V: Clone + Default,
{
    pub storage: Vec<BugguBucket<K, V>>,
    count: usize,
}

impl<K, V> BugguHashSet<K, V>
where
    K: BugguHashable + Eq + PartialEq + Clone + Default,
    V: Clone + Default,
{
    pub fn new(table_size: usize) -> Self {
        BugguHashSet {
            storage: vec![BugguBucket::Empty; table_size],
            count: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Retains only the elements specified by the predicate.
    ///
    /// In other words, remove all pairs `(k, v)` such that `f(&k, &mut v)` returns `false`.
    /// The elements are visited in unsorted (and unspecified) order.
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &mut V) -> bool,
    {
        let mut total_removed = 0;

        for bucket in self.storage.iter_mut() {
            match bucket {
                BugguBucket::Empty => continue,
                BugguBucket::Inline { entries, len } => {
                    let current_len = *len as usize;
                    let mut write_idx = 0;

                    for read_idx in 0..current_len {
                        let should_keep = {
                            let entry = unsafe { entries.get_unchecked_mut(read_idx) };
                            f(&entry.0, &mut entry.1)
                        };

                        if should_keep {
                            if write_idx != read_idx {
                                // Move the entry to the write position
                                let temp = std::mem::take(&mut entries[read_idx]);
                                entries[write_idx] = temp;
                            }
                            write_idx += 1;
                        } else {
                            total_removed += 1;
                        }
                    }

                    // Clear the remaining entries
                    for i in write_idx..current_len {
                        entries[i] = (K::default(), V::default());
                    }
                    *len = write_idx as u8;

                    // Convert to Empty if no entries remain
                    if write_idx == 0 {
                        *bucket = BugguBucket::Empty;
                    }
                }
                BugguBucket::Overflow { entries } => {
                    let original_len = entries.len();
                    entries.retain_mut(|entry| f(&entry.0, &mut entry.1));
                    let new_len = entries.len();
                    total_removed += original_len - new_len;

                    // Convert to Inline or Empty if few entries remain
                    if new_len == 0 {
                        *bucket = BugguBucket::Empty;
                    } else if new_len <= INLINE_BUCKET_SIZE {
                        let mut inline_entries =
                            core::array::from_fn(|_| (K::default(), V::default()));
                        for (i, entry) in entries.drain(..).enumerate() {
                            inline_entries[i] = entry;
                        }
                        *bucket = BugguBucket::Inline {
                            entries: inline_entries,
                            len: new_len as u8,
                        };
                    }
                }
            }
        }

        self.count -= total_removed;
    }

    #[inline(always)]
    fn get_rank_for_key(&self, key: &K) -> usize {
        let seed = key.buggu_hash();
        let mut rng = BugguRng::new(seed);
        rng.range(0, self.storage.len() as u64 - 1) as usize
    }
    // Fast batch contains check using direct bucket access
    pub fn fast_intersect_slice(&self, keys: &[K]) -> Vec<K> {
        let mut result = Vec::new();

        for key in keys {
            let rank_idx = self.get_rank_for_key(key);
            let bucket = unsafe { self.storage.get_unchecked(rank_idx) };

            let found = match bucket {
                BugguBucket::Empty => false,
                BugguBucket::Inline { entries, len } => {
                    let current_len = *len as usize;
                    (0..current_len).any(|i| unsafe { &entries.get_unchecked(i).0 == key })
                }
                BugguBucket::Overflow { entries } => entries.iter().any(|(k, _)| k == key),
            };

            if found {
                result.push(key.clone());
            }
        }
        result
    }
    // Add to BugguHashSet
    pub fn create_index_for<F, V2>(&self, field_extractor: F) -> BugguHashSet<V2, Vec<K>>
    where
        F: Fn(&V) -> Option<V2>,
        V2: BugguHashable + Eq + PartialEq + Clone + Default,
    {
        let mut index = BugguHashSet::new(128);

        for bucket in &self.storage {
            match bucket {
                BugguBucket::Empty => continue,
                BugguBucket::Inline { entries, len } => {
                    for i in 0..*len as usize {
                        let (k, v) = unsafe { entries.get_unchecked(i) };
                        if let Some(field_value) = field_extractor(v) {
                            index
                                .entry(field_value)
                                .or_insert_with(Vec::new)
                                .push(k.clone());
                        }
                    }
                }
                BugguBucket::Overflow { entries } => {
                    for (k, v) in entries {
                        if let Some(field_value) = field_extractor(v) {
                            index
                                .entry(field_value)
                                .or_insert_with(Vec::new)
                                .push(k.clone());
                        }
                    }
                }
            }
        }

        index
    }
    // Add to BugguHashSet
    pub fn intersect_to_set(&self, other: &BugguHashSet<K, V>) -> BugguHashSet<K, ()>
    where
        V: Default + Clone,
    {
        let mut result = BugguHashSet::new(self.len().min(other.len()));

        // Use smaller set for iteration
        let (smaller, larger) = if self.len() < other.len() {
            (self, other)
        } else {
            (other, self)
        };

        for k in smaller.iter_keys() {
            if larger.get(&k).is_some() {
                result.insert(k, ());
            }
        }

        result
    }
    // Add these methods to BugguHashSet


    // Fast intersection between set and slice
    pub fn fast_difference(&self, exclude: &BugguHashSet<K, V>) -> BugguHashSet<K, ()> 
    where 
        V: Clone + Default 
    {
        let mut result = BugguHashSet::new(self.len());
        for key in self.iter_keys() {
            if exclude.get(&key).is_none() {
                result.insert(key, ());
            }
        }
        result
    }
    
    
    
    // Direct set operations returning a new set
    pub fn union_with(&self, other: &BugguHashSet<K, V>) -> BugguHashSet<K, ()>
    where
        V: Clone + Default
    {
        let mut result = BugguHashSet::new(self.len() + other.len());
        
        // Add all from self
        for k in self.iter_keys() {
            result.insert(k, ());
        }
        
        // Add all from other
        for k in other.iter_keys() {
            result.insert(k, ());
        }
        
        result
    }
    
    // Direct intersection returning a new set
    pub fn intersect_with(&self, other: &BugguHashSet<K, V>) -> BugguHashSet<K, ()>
    where
        V: Clone + Default
    {
        // Use smaller set for iteration
        let (smaller, larger) = if self.len() < other.len() {
            (self, other)
        } else {
            (other, self)
        };
        
        let mut result = BugguHashSet::new(smaller.len());
        for k in smaller.iter_keys() {
            if larger.get(&k).is_some() {
                result.insert(k, ());
            }
        }
        
        result
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let rank_idx = self.get_rank_for_key(&key);
        let bucket = unsafe { self.storage.get_unchecked_mut(rank_idx) };

        match bucket {
            BugguBucket::Empty => {
                let mut entries = core::array::from_fn(|_| (K::default(), V::default()));
                entries[0] = (key, value);
                *bucket = BugguBucket::Inline { entries, len: 1 };
                self.count += 1;
                None
            }
            BugguBucket::Inline { entries, len } => {
                let current_len = *len as usize;
                for i in 0..current_len {
                    let entry = unsafe { entries.get_unchecked_mut(i) };
                    if entry.0 == key {
                        return Some(std::mem::replace(&mut entry.1, value));
                    }
                }
                if current_len < INLINE_BUCKET_SIZE {
                    entries[current_len] = (key, value);
                    *len += 1;
                    self.count += 1;
                    None
                } else {
                    let mut overflow_vec = Vec::with_capacity(OVERFLOW_BUCKET_SIZE);
                    for item in entries.iter_mut().take(INLINE_BUCKET_SIZE) {
                        overflow_vec.push(std::mem::take(item));
                    }
                    overflow_vec.push((key, value));
                    *bucket = BugguBucket::Overflow {
                        entries: overflow_vec,
                    };
                    self.count += 1;
                    None
                }
            }
            BugguBucket::Overflow { entries } => {
                for entry in entries.iter_mut() {
                    if entry.0 == key {
                        return Some(std::mem::replace(&mut entry.1, value));
                    }
                }
                if entries.len() == entries.capacity() {
                    entries.reserve(entries.capacity());
                }
                entries.push((key, value));
                self.count += 1;
                None
            }
        }
    }

    pub fn entry(&mut self, key: K) -> BugguEntry<K, V> {
        let bucket_idx = self.get_rank_for_key(&key);

        // First, check if the key exists and get the entry index
        let entry_info: Option<usize> = {
            let bucket = unsafe { self.storage.get_unchecked(bucket_idx) };
            match bucket {
                BugguBucket::Empty => None,
                BugguBucket::Inline { entries, len } => {
                    let current_len = *len as usize;
                    for i in 0..current_len {
                        if unsafe { &entries.get_unchecked(i).0 } == &key {
                            return BugguEntry::Occupied(BugguOccupiedEntry {
                                key,
                                hashset: self,
                                bucket_idx,
                                entry_idx: i,
                            });
                        }
                    }
                    None
                }
                BugguBucket::Overflow { entries } => {
                    for (i, (k, _)) in entries.iter().enumerate() {
                        if k == &key {
                            return BugguEntry::Occupied(BugguOccupiedEntry {
                                key,
                                hashset: self,
                                bucket_idx,
                                entry_idx: i,
                            });
                        }
                    }
                    None
                }
            }
        };

        // If we get here, the key doesn't exist
        BugguEntry::Vacant(BugguVacantEntry {
            key,
            hashset: self,
            bucket_idx,
        })
    }
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<&V> {
        let rank_idx = self.get_rank_for_key(key);
        let bucket = unsafe { self.storage.get_unchecked(rank_idx) };

        match bucket {
            BugguBucket::Empty => None,
            BugguBucket::Inline { entries, len } => {
                let current_len = *len as usize;
                for i in 0..current_len {
                    let (k, v) = unsafe { entries.get_unchecked(i) };
                    if k == key {
                        return Some(v);
                    }
                }
                None
            }
            BugguBucket::Overflow { entries } => {
                for (k, v) in entries.iter() {
                    if k == key {
                        return Some(v);
                    }
                }
                None
            }
        }
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let rank_idx = self.get_rank_for_key(key);
        let bucket = unsafe { self.storage.get_unchecked_mut(rank_idx) };

        match bucket {
            BugguBucket::Empty => None,
            BugguBucket::Inline { entries, len } => {
                let current_len = *len as usize;
                for i in 0..current_len {
                    let entry = unsafe { entries.get_unchecked_mut(i) };
                    if entry.0 == *key {
                        let old_value = std::mem::take(&mut entry.1);
                        unsafe {
                            let ptr = entries.as_mut_ptr();
                            for j in i..(current_len - 1) {
                                let src_ptr = ptr.add(j + 1);
                                let dst_ptr = ptr.add(j);
                                std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, 1);
                            }
                        }
                        unsafe {
                            let last = entries.get_unchecked_mut(current_len - 1);
                            *last = (K::default(), V::default());
                        }
                        *len -= 1;
                        if *len == 0 {
                            *bucket = BugguBucket::Empty;
                        }
                        self.count -= 1;
                        return Some(old_value);
                    }
                }
                None
            }
            BugguBucket::Overflow { entries } => {
                for i in 0..entries.len() {
                    if unsafe { &entries.get_unchecked(i).0 } == key {
                        let (_, old_value) = entries.swap_remove(i);
                        if entries.len() <= INLINE_BUCKET_SIZE {
                            let entries_len = entries.len();
                            let mut inline_entries =
                                core::array::from_fn(|_| (K::default(), V::default()));
                            for (i, entry) in entries.drain(..).enumerate() {
                                inline_entries[i] = entry;
                            }
                            *bucket = if entries_len == 0 {
                                BugguBucket::Empty
                            } else {
                                BugguBucket::Inline {
                                    entries: inline_entries,
                                    len: entries_len as u8,
                                }
                            };
                        }
                        self.count -= 1;
                        return Some(old_value);
                    }
                }
                None
            }
        }
    }

    #[inline(always)]
    pub fn update(&mut self, key: &K, value: V) -> Option<V> {
        let rank_idx = self.get_rank_for_key(key);
        let bucket = unsafe { self.storage.get_unchecked_mut(rank_idx) };

        match bucket {
            BugguBucket::Empty => None,
            BugguBucket::Inline { entries, len } => {
                let current_len = *len as usize;
                for i in 0..current_len {
                    let entry = unsafe { entries.get_unchecked_mut(i) };
                    if entry.0 == *key {
                        return Some(std::mem::replace(&mut entry.1, value));
                    }
                }
                None
            }
            BugguBucket::Overflow { entries } => {
                for entry in entries.iter_mut() {
                    if entry.0 == *key {
                        return Some(std::mem::replace(&mut entry.1, value));
                    }
                }
                None
            }
        }
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let rank_idx = self.get_rank_for_key(key);
        let bucket = unsafe { self.storage.get_unchecked_mut(rank_idx) };

        match bucket {
            BugguBucket::Empty => None,
            BugguBucket::Inline { entries, len } => {
                let current_len = *len as usize;
                let entries_ptr = entries.as_mut_ptr();
                for i in 0..current_len {
                    unsafe {
                        let entry_ptr = entries_ptr.add(i);
                        if (*entry_ptr).0 == *key {
                            return Some(&mut (*entry_ptr).1);
                        }
                    }
                }
                None
            }
            BugguBucket::Overflow { entries } => {
                for entry in entries.iter_mut() {
                    if entry.0 == *key {
                        return Some(&mut entry.1);
                    }
                }
                None
            }
        }
    }

    pub fn keys(&self) -> Vec<K> {
        if self.count == 0 {
            return Vec::new();
        }
        let mut keys = Vec::with_capacity(self.count);
        for bucket in self.storage.iter() {
            match bucket {
                BugguBucket::Empty => continue,
                BugguBucket::Inline { entries, len } => {
                    let current_len = *len as usize;
                    for i in 0..current_len {
                        keys.push(unsafe { entries.get_unchecked(i).0.clone() });
                    }
                }
                BugguBucket::Overflow { entries } => {
                    keys.extend(entries.iter().map(|(k, _)| k.clone()));
                }
            }
        }
        keys
    }

    pub fn iter_keys(&self) -> BugguKeyIterator<K, V> {
        BugguKeyIterator {
            storage: &self.storage,
            bucket_idx: 0,
            entry_idx: 0,
            remaining: self.count,
        }
    }

    pub fn iter_mut(&mut self) -> BugguIterMut<K, V> {
        BugguIterMut::new(&mut self.storage, self.count)
    }

    pub fn insert_batch(&mut self, items: Vec<(K, V)>) {
        for (key, value) in items.into_iter() {
            self.insert(key, value);
        }
    }

    pub fn bucket_stats(&self) -> (usize, usize, usize) {
        let mut empty = 0;
        let mut inline = 0;
        let mut overflow = 0;
        for bucket in &self.storage {
            match bucket {
                BugguBucket::Empty => empty += 1,
                BugguBucket::Inline { .. } => inline += 1,
                BugguBucket::Overflow { .. } => overflow += 1,
            }
        }
        (empty, inline, overflow)
    }
}
