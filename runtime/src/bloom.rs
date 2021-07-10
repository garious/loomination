//! Simple Bloom Filter
use bv::BitVec;
use fnv::FnvHasher;
use rand::{self, Rng};
use serde::{Deserialize, Serialize};
use solana_sdk::sanitize::{Sanitize, SanitizeError};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{cmp, hash::Hasher, marker::PhantomData};

/// Generate a stable hash of `self` for each `hash_index`
/// Best effort can be made for uniqueness of each hash.
pub trait BloomHashIndex {
    fn hash_at_index(&self, hash_index: u64) -> u64;
}

#[derive(Serialize, Deserialize, Default, Clone, PartialEq, AbiExample)]
pub struct Bloom<T: BloomHashIndex> {
    pub keys: Vec<u64>,
    pub bits: BitVec<u64>,
    num_bits_set: u64,
    _phantom: PhantomData<T>,
}

impl<T: BloomHashIndex> fmt::Debug for Bloom<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Bloom {{ keys.len: {} bits.len: {} num_set: {} bits: ",
            self.keys.len(),
            self.bits.len(),
            self.num_bits_set
        )?;
        const MAX_PRINT_BITS: u64 = 10;
        for i in 0..std::cmp::min(MAX_PRINT_BITS, self.bits.len()) {
            if self.bits.get(i) {
                write!(f, "1")?;
            } else {
                write!(f, "0")?;
            }
        }
        if self.bits.len() > MAX_PRINT_BITS {
            write!(f, "..")?;
        }
        write!(f, " }}")
    }
}

impl<T: BloomHashIndex> Sanitize for Bloom<T> {
    fn sanitize(&self) -> Result<(), SanitizeError> {
        // Avoid division by zero in self.pos(...).
        if self.bits.is_empty() {
            Err(SanitizeError::InvalidValue)
        } else {
            Ok(())
        }
    }
}

impl<T: BloomHashIndex> Bloom<T> {
    pub fn new(num_bits: usize, keys: Vec<u64>) -> Self {
        let bits = BitVec::new_fill(false, num_bits as u64);
        Bloom {
            keys,
            bits,
            num_bits_set: 0,
            _phantom: PhantomData::default(),
        }
    }
    /// Create filter optimal for num size given the `FALSE_RATE`.
    ///
    /// The keys are randomized for picking data out of a collision resistant hash of size
    /// `keysize` bytes.
    ///
    /// See <https://hur.st/bloomfilter/>.
    pub fn random(num_items: usize, false_rate: f64, max_bits: usize) -> Self {
        let m = Self::num_bits(num_items as f64, false_rate);
        let num_bits = cmp::max(1, cmp::min(m as usize, max_bits));
        let num_keys = Self::num_keys(num_bits as f64, num_items as f64) as usize;
        let keys: Vec<u64> = (0..num_keys).map(|_| rand::thread_rng().gen()).collect();
        Self::new(num_bits, keys)
    }
    fn num_bits(num_items: f64, false_rate: f64) -> f64 {
        let n = num_items;
        let p = false_rate;
        ((n * p.ln()) / (1f64 / 2f64.powf(2f64.ln())).ln()).ceil()
    }
    fn num_keys(num_bits: f64, num_items: f64) -> f64 {
        let n = num_items;
        let m = num_bits;
        // infinity as usize is zero in rust 1.43 but 2^64-1 in rust 1.45; ensure it's zero here
        if n == 0.0 {
            0.0
        } else {
            1f64.max(((m / n) * 2f64.ln()).round())
        }
    }
    fn pos(&self, key: &T, k: u64) -> u64 {
        key.hash_at_index(k) % self.bits.len()
    }
    pub fn clear(&mut self) {
        self.bits = BitVec::new_fill(false, self.bits.len());
        self.num_bits_set = 0;
    }
    pub fn add(&mut self, key: &T) {
        for k in &self.keys {
            let pos = self.pos(key, *k);
            if !self.bits.get(pos) {
                self.num_bits_set += 1;
                self.bits.set(pos, true);
            }
        }
    }
    pub fn contains(&self, key: &T) -> bool {
        for k in &self.keys {
            let pos = self.pos(key, *k);
            if !self.bits.get(pos) {
                return false;
            }
        }
        true
    }
}

fn slice_hash(slice: &[u8], hash_index: u64) -> u64 {
    let mut hasher = FnvHasher::with_key(hash_index);
    hasher.write(slice);
    hasher.finish()
}

impl<T: AsRef<[u8]>> BloomHashIndex for T {
    fn hash_at_index(&self, hash_index: u64) -> u64 {
        slice_hash(self.as_ref(), hash_index)
    }
}

pub struct AtomicBloom<T> {
    num_bits: u64,
    keys: Vec<u64>,
    bits: Vec<AtomicU64>,
    _phantom: PhantomData<T>,
}

impl<T: BloomHashIndex> From<Bloom<T>> for AtomicBloom<T> {
    fn from(bloom: Bloom<T>) -> Self {
        AtomicBloom {
            num_bits: bloom.bits.len(),
            keys: bloom.keys,
            bits: bloom
                .bits
                .into_boxed_slice()
                .iter()
                .map(|&x| AtomicU64::new(x))
                .collect(),
            _phantom: PhantomData::default(),
        }
    }
}

impl<T: BloomHashIndex> AtomicBloom<T> {
    fn pos(&self, key: &T, hash_index: u64) -> (usize, u64) {
        let pos = key.hash_at_index(hash_index) % self.num_bits;
        // Divide by 64 to figure out which of the
        // AtomicU64 bit chunks we need to modify.
        let index = pos >> 6;
        // (pos & 63) is equivalent to mod 64 so that we can find
        // the index of the bit within the AtomicU64 to modify.
        let mask = 1u64 << (pos & 63);
        (index as usize, mask)
    }

    pub fn add(&self, key: &T) {
        for k in &self.keys {
            let (index, mask) = self.pos(key, *k);
            self.bits[index].fetch_or(mask, Ordering::Relaxed);
        }
    }

    pub fn contains(&self, key: &T) -> bool {
        self.keys.iter().all(|k| {
            let (index, mask) = self.pos(key, *k);
            let bit = self.bits[index].load(Ordering::Relaxed) & mask;
            bit != 0u64
        })
    }

    // Only for tests and simulations.
    pub fn mock_clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            bits: self
                .bits
                .iter()
                .map(|v| AtomicU64::new(v.load(Ordering::Relaxed)))
                .collect(),
            ..*self
        }
    }
}

impl<T: BloomHashIndex> From<AtomicBloom<T>> for Bloom<T> {
    fn from(atomic_bloom: AtomicBloom<T>) -> Self {
        let bits: Vec<_> = atomic_bloom
            .bits
            .into_iter()
            .map(AtomicU64::into_inner)
            .collect();
        let num_bits_set = bits.iter().map(|x| x.count_ones() as u64).sum();
        let mut bits: BitVec<u64> = bits.into();
        bits.truncate(atomic_bloom.num_bits);
        Bloom {
            keys: atomic_bloom.keys,
            bits,
            num_bits_set,
            _phantom: PhantomData::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rayon::prelude::*;
    use solana_sdk::hash::{hash, Hash};

    #[test]
    fn test_bloom_filter() {
        //empty
        let bloom: Bloom<Hash> = Bloom::random(0, 0.1, 100);
        assert_eq!(bloom.keys.len(), 0);
        assert_eq!(bloom.bits.len(), 1);

        //normal
        let bloom: Bloom<Hash> = Bloom::random(10, 0.1, 100);
        assert_eq!(bloom.keys.len(), 3);
        assert_eq!(bloom.bits.len(), 48);

        //saturated
        let bloom: Bloom<Hash> = Bloom::random(100, 0.1, 100);
        assert_eq!(bloom.keys.len(), 1);
        assert_eq!(bloom.bits.len(), 100);
    }
    #[test]
    fn test_add_contains() {
        let mut bloom: Bloom<Hash> = Bloom::random(100, 0.1, 100);
        //known keys to avoid false positives in the test
        bloom.keys = vec![0, 1, 2, 3];

        let key = hash(b"hello");
        assert!(!bloom.contains(&key));
        bloom.add(&key);
        assert!(bloom.contains(&key));

        let key = hash(b"world");
        assert!(!bloom.contains(&key));
        bloom.add(&key);
        assert!(bloom.contains(&key));
    }
    #[test]
    fn test_random() {
        let mut b1: Bloom<Hash> = Bloom::random(10, 0.1, 100);
        let mut b2: Bloom<Hash> = Bloom::random(10, 0.1, 100);
        b1.keys.sort_unstable();
        b2.keys.sort_unstable();
        assert_ne!(b1.keys, b2.keys);
    }
    // Bloom filter math in python
    // n number of items
    // p false rate
    // m number of bits
    // k number of keys
    //
    // n = ceil(m / (-k / log(1 - exp(log(p) / k))))
    // p = pow(1 - exp(-k / (m / n)), k)
    // m = ceil((n * log(p)) / log(1 / pow(2, log(2))));
    // k = round((m / n) * log(2));
    #[test]
    fn test_filter_math() {
        assert_eq!(Bloom::<Hash>::num_bits(100f64, 0.1f64) as u64, 480u64);
        assert_eq!(Bloom::<Hash>::num_bits(100f64, 0.01f64) as u64, 959u64);
        assert_eq!(Bloom::<Hash>::num_keys(1000f64, 50f64) as u64, 14u64);
        assert_eq!(Bloom::<Hash>::num_keys(2000f64, 50f64) as u64, 28u64);
        assert_eq!(Bloom::<Hash>::num_keys(2000f64, 25f64) as u64, 55u64);
        //ensure min keys is 1
        assert_eq!(Bloom::<Hash>::num_keys(20f64, 1000f64) as u64, 1u64);
    }

    #[test]
    fn test_debug() {
        let mut b: Bloom<Hash> = Bloom::new(3, vec![100]);
        b.add(&Hash::default());
        assert_eq!(
            format!("{:?}", b),
            "Bloom { keys.len: 1 bits.len: 3 num_set: 1 bits: 001 }"
        );

        let mut b: Bloom<Hash> = Bloom::new(1000, vec![100]);
        b.add(&Hash::default());
        b.add(&hash(&[1, 2]));
        assert_eq!(
            format!("{:?}", b),
            "Bloom { keys.len: 1 bits.len: 1000 num_set: 2 bits: 0000000000.. }"
        );
    }

    #[test]
    fn test_atomic_bloom() {
        let mut rng = rand::thread_rng();
        let hash_values: Vec<_> = std::iter::repeat_with(|| solana_sdk::hash::new_rand(&mut rng))
            .take(1200)
            .collect();
        let bloom: AtomicBloom<_> = Bloom::<Hash>::random(1287, 0.1, 7424).into();
        assert_eq!(bloom.keys.len(), 3);
        assert_eq!(bloom.num_bits, 6168);
        assert_eq!(bloom.bits.len(), 97);
        hash_values.par_iter().for_each(|v| bloom.add(v));
        let bloom: Bloom<Hash> = bloom.into();
        assert_eq!(bloom.keys.len(), 3);
        assert_eq!(bloom.bits.len(), 6168);
        assert!(bloom.num_bits_set > 2000);
        for hash_value in hash_values {
            assert!(bloom.contains(&hash_value));
        }
        let false_positive = std::iter::repeat_with(|| solana_sdk::hash::new_rand(&mut rng))
            .take(10_000)
            .filter(|hash_value| bloom.contains(hash_value))
            .count();
        assert!(false_positive < 2_000, "false_positive: {}", false_positive);
    }

    #[test]
    fn test_atomic_bloom_round_trip() {
        let mut rng = rand::thread_rng();
        let keys: Vec<_> = std::iter::repeat_with(|| rng.gen()).take(5).collect();
        let mut bloom = Bloom::<Hash>::new(9731, keys.clone());
        let hash_values: Vec<_> = std::iter::repeat_with(|| solana_sdk::hash::new_rand(&mut rng))
            .take(1000)
            .collect();
        for hash_value in &hash_values {
            bloom.add(hash_value);
        }
        let num_bits_set = bloom.num_bits_set;
        assert!(num_bits_set > 2000, "# bits set: {}", num_bits_set);
        // Round-trip with no inserts.
        let bloom: AtomicBloom<_> = bloom.into();
        assert_eq!(bloom.num_bits, 9731);
        assert_eq!(bloom.bits.len(), (9731 + 63) / 64);
        for hash_value in &hash_values {
            assert!(bloom.contains(hash_value));
        }
        let bloom: Bloom<_> = bloom.into();
        assert_eq!(bloom.num_bits_set, num_bits_set);
        for hash_value in &hash_values {
            assert!(bloom.contains(hash_value));
        }
        // Round trip, re-inserting the same hash values.
        let bloom: AtomicBloom<_> = bloom.into();
        hash_values.par_iter().for_each(|v| bloom.add(v));
        for hash_value in &hash_values {
            assert!(bloom.contains(hash_value));
        }
        let bloom: Bloom<_> = bloom.into();
        assert_eq!(bloom.num_bits_set, num_bits_set);
        assert_eq!(bloom.bits.len(), 9731);
        for hash_value in &hash_values {
            assert!(bloom.contains(hash_value));
        }
        // Round trip, inserting new hash values.
        let more_hash_values: Vec<_> =
            std::iter::repeat_with(|| solana_sdk::hash::new_rand(&mut rng))
                .take(1000)
                .collect();
        let bloom: AtomicBloom<_> = bloom.into();
        assert_eq!(bloom.num_bits, 9731);
        assert_eq!(bloom.bits.len(), (9731 + 63) / 64);
        more_hash_values.par_iter().for_each(|v| bloom.add(v));
        for hash_value in &hash_values {
            assert!(bloom.contains(hash_value));
        }
        for hash_value in &more_hash_values {
            assert!(bloom.contains(hash_value));
        }
        let false_positive = std::iter::repeat_with(|| solana_sdk::hash::new_rand(&mut rng))
            .take(10_000)
            .filter(|hash_value| bloom.contains(hash_value))
            .count();
        assert!(false_positive < 2000, "false_positive: {}", false_positive);
        let bloom: Bloom<_> = bloom.into();
        assert_eq!(bloom.bits.len(), 9731);
        assert!(bloom.num_bits_set > num_bits_set);
        assert!(
            bloom.num_bits_set > 4000,
            "# bits set: {}",
            bloom.num_bits_set
        );
        for hash_value in &hash_values {
            assert!(bloom.contains(hash_value));
        }
        for hash_value in &more_hash_values {
            assert!(bloom.contains(hash_value));
        }
        let false_positive = std::iter::repeat_with(|| solana_sdk::hash::new_rand(&mut rng))
            .take(10_000)
            .filter(|hash_value| bloom.contains(hash_value))
            .count();
        assert!(false_positive < 2000, "false_positive: {}", false_positive);
        // Assert that the bits vector precisely match if no atomic ops were
        // used.
        let bits = bloom.bits;
        let mut bloom = Bloom::<Hash>::new(9731, keys);
        for hash_value in &hash_values {
            bloom.add(hash_value);
        }
        for hash_value in &more_hash_values {
            bloom.add(hash_value);
        }
        assert_eq!(bits, bloom.bits);
    }
}
