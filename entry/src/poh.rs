//! The `Poh` module provides an object for generating a Proof of History.
use log::*;
use solana_sdk::hash::{hash, hashv, Hash};
use std::time::{Duration, Instant};

pub struct Poh {
    pub hash: Hash,
    num_hashes: u64,
    hashes_per_tick: u64,
    remaining_hashes: u64,
    ticks_per_slot: u64,
    tick_number: u64,
    slot_start_time: Instant,
}

#[derive(Debug)]
pub struct PohEntry {
    pub num_hashes: u64,
    pub hash: Hash,
}

impl Poh {
    pub fn new(hash: Hash, hashes_per_tick: Option<u64>) -> Self {
        Self::new_with_slot_info(hash, hashes_per_tick, 0, 0)
    }

    pub fn new_with_slot_info(
        hash: Hash,
        hashes_per_tick: Option<u64>,
        ticks_per_slot: u64,
        tick_number: u64,
    ) -> Self {
        let hashes_per_tick = hashes_per_tick.unwrap_or(std::u64::MAX);
        assert!(hashes_per_tick > 1);
        let now = Instant::now();
        Poh {
            hash,
            num_hashes: 0,
            hashes_per_tick,
            remaining_hashes: hashes_per_tick,
            ticks_per_slot,
            tick_number,
            slot_start_time: now,
        }
    }

    pub fn reset(&mut self, hash: Hash, hashes_per_tick: Option<u64>) {
        // retains ticks_per_slot: this cannot change without restarting the validator
        let tick_number = 0;
        let mut poh =
            Poh::new_with_slot_info(hash, hashes_per_tick, self.ticks_per_slot, tick_number);
        std::mem::swap(&mut poh, self);
    }

    pub fn target_poh_time(&self, target_ns_per_tick: u64) -> Instant {
        assert!(self.hashes_per_tick > 0);
        let offset_tick_ns = target_ns_per_tick * self.tick_number;
        let offset_ns = target_ns_per_tick * self.num_hashes / self.hashes_per_tick;
        self.slot_start_time + Duration::from_nanos(offset_ns + offset_tick_ns)
    }

    pub fn hash(&mut self, max_num_hashes: u64) -> bool {
        let num_hashes = std::cmp::min(self.remaining_hashes - 1, max_num_hashes);

        for _ in 0..num_hashes {
            self.hash = hash(self.hash.as_ref());
        }
        self.num_hashes += num_hashes;
        self.remaining_hashes -= num_hashes;

        assert!(self.remaining_hashes > 0);
        self.remaining_hashes == 1 // Return `true` if caller needs to `tick()` next
    }

    pub fn record(&mut self, mixin: Hash) -> Option<PohEntry> {
        if self.remaining_hashes == 1 {
            return None; // Caller needs to `tick()` first
        }

        self.hash = hashv(&[self.hash.as_ref(), mixin.as_ref()]);
        let num_hashes = self.num_hashes + 1;
        self.num_hashes = 0;
        self.remaining_hashes -= 1;

        Some(PohEntry {
            num_hashes,
            hash: self.hash,
        })
    }

    pub fn tick(&mut self) -> Option<PohEntry> {
        self.hash = hash(self.hash.as_ref());
        self.num_hashes += 1;
        self.remaining_hashes -= 1;

        // If the hashes_per_tick is variable (std::u64::MAX) then always generate a tick.
        // Otherwise only tick if there are no remaining hashes
        if self.hashes_per_tick < std::u64::MAX && self.remaining_hashes != 0 {
            return None;
        }

        let num_hashes = self.num_hashes;
        self.remaining_hashes = self.hashes_per_tick;
        self.num_hashes = 0;
        self.tick_number += 1;
        Some(PohEntry {
            num_hashes,
            hash: self.hash,
        })
    }
}

pub fn compute_hash_time_ns(hashes_sample_size: u64) -> u64 {
    info!("Running {} hashes...", hashes_sample_size);
    let mut v = Hash::default();
    let start = Instant::now();
    for _ in 0..hashes_sample_size {
        v = hash(v.as_ref());
    }
    start.elapsed().as_nanos() as u64
}

pub fn compute_hashes_per_tick(duration: Duration, hashes_sample_size: u64) -> u64 {
    let elapsed = compute_hash_time_ns(hashes_sample_size) / (1000 * 1000);
    duration.as_millis() as u64 * hashes_sample_size / elapsed
}

#[cfg(test)]
mod tests {
    use crate::poh::{Poh, PohEntry};
    use matches::assert_matches;
    use solana_sdk::hash::{hash, hashv, Hash};
    use std::time::Duration;

    fn verify(initial_hash: Hash, entries: &[(PohEntry, Option<Hash>)]) -> bool {
        let mut current_hash = initial_hash;

        for (entry, mixin) in entries {
            assert_ne!(entry.num_hashes, 0);

            for _ in 1..entry.num_hashes {
                current_hash = hash(current_hash.as_ref());
            }
            current_hash = match mixin {
                Some(mixin) => hashv(&[current_hash.as_ref(), mixin.as_ref()]),
                None => hash(current_hash.as_ref()),
            };
            if current_hash != entry.hash {
                return false;
            }
        }

        true
    }

    #[test]
    fn test_target_poh_time() {
        let zero = Hash::default();
        for target_ns_per_tick in 10..12 {
            let mut poh = Poh::new(zero, None);
            assert_eq!(poh.target_poh_time(target_ns_per_tick), poh.slot_start_time);
            poh.tick_number = 2;
            assert_eq!(
                poh.target_poh_time(target_ns_per_tick),
                poh.slot_start_time + Duration::from_nanos(target_ns_per_tick * 2)
            );
            let mut poh = Poh::new(zero, Some(5));
            assert_eq!(poh.target_poh_time(target_ns_per_tick), poh.slot_start_time);
            poh.tick_number = 2;
            assert_eq!(
                poh.target_poh_time(target_ns_per_tick),
                poh.slot_start_time + Duration::from_nanos(target_ns_per_tick * 2)
            );
            poh.num_hashes = 3;
            assert_eq!(
                poh.target_poh_time(target_ns_per_tick),
                poh.slot_start_time
                    + Duration::from_nanos(target_ns_per_tick * 2 + target_ns_per_tick * 3 / 5)
            );
        }
    }

    #[test]
    #[should_panic(expected = "assertion failed: hashes_per_tick > 1")]
    fn test_target_poh_time_hashes_per_tick() {
        let zero = Hash::default();
        let poh = Poh::new(zero, Some(0));
        let target_ns_per_tick = 10;
        poh.target_poh_time(target_ns_per_tick);
    }

    #[test]
    fn test_poh_verify() {
        let zero = Hash::default();
        let one = hash(zero.as_ref());
        let two = hash(one.as_ref());
        let one_with_zero = hashv(&[zero.as_ref(), zero.as_ref()]);

        let mut poh = Poh::new(zero, None);
        assert!(verify(
            zero,
            &[
                (poh.tick().unwrap(), None),
                (poh.record(zero).unwrap(), Some(zero)),
                (poh.record(zero).unwrap(), Some(zero)),
                (poh.tick().unwrap(), None),
            ],
        ));

        assert!(verify(
            zero,
            &[(
                PohEntry {
                    num_hashes: 1,
                    hash: one,
                },
                None
            )],
        ));
        assert!(verify(
            zero,
            &[(
                PohEntry {
                    num_hashes: 2,
                    hash: two,
                },
                None
            )]
        ));

        assert!(verify(
            zero,
            &[(
                PohEntry {
                    num_hashes: 1,
                    hash: one_with_zero,
                },
                Some(zero)
            )]
        ));
        assert!(!verify(
            zero,
            &[(
                PohEntry {
                    num_hashes: 1,
                    hash: zero,
                },
                None
            )]
        ));

        assert!(verify(
            zero,
            &[
                (
                    PohEntry {
                        num_hashes: 1,
                        hash: one_with_zero,
                    },
                    Some(zero)
                ),
                (
                    PohEntry {
                        num_hashes: 1,
                        hash: hash(one_with_zero.as_ref()),
                    },
                    None
                )
            ]
        ));
    }

    #[test]
    #[should_panic]
    fn test_poh_verify_assert() {
        verify(
            Hash::default(),
            &[(
                PohEntry {
                    num_hashes: 0,
                    hash: Hash::default(),
                },
                None,
            )],
        );
    }

    #[test]
    fn test_poh_tick() {
        let mut poh = Poh::new(Hash::default(), Some(2));
        assert_eq!(poh.remaining_hashes, 2);
        assert!(poh.tick().is_none());
        assert_eq!(poh.remaining_hashes, 1);
        assert_matches!(poh.tick(), Some(PohEntry { num_hashes: 2, .. }));
        assert_eq!(poh.remaining_hashes, 2); // Ready for the next tick
    }

    #[test]
    fn test_poh_tick_large_batch() {
        let mut poh = Poh::new(Hash::default(), Some(2));
        assert_eq!(poh.remaining_hashes, 2);
        assert!(poh.hash(1_000_000)); // Stop hashing before the next tick
        assert_eq!(poh.remaining_hashes, 1);
        assert!(poh.hash(1_000_000)); // Does nothing...
        assert_eq!(poh.remaining_hashes, 1);
        poh.tick();
        assert_eq!(poh.remaining_hashes, 2); // Ready for the next tick
    }

    #[test]
    fn test_poh_tick_too_soon() {
        let mut poh = Poh::new(Hash::default(), Some(2));
        assert_eq!(poh.remaining_hashes, 2);
        assert!(poh.tick().is_none());
    }

    #[test]
    fn test_poh_record_not_permitted_at_final_hash() {
        let mut poh = Poh::new(Hash::default(), Some(10));
        assert!(poh.hash(9));
        assert_eq!(poh.remaining_hashes, 1);
        assert!(poh.record(Hash::default()).is_none()); // <-- record() rejected to avoid exceeding hashes_per_tick
        assert_matches!(poh.tick(), Some(PohEntry { num_hashes: 10, .. }));
        assert_matches!(
            poh.record(Hash::default()),
            Some(PohEntry { num_hashes: 1, .. }) // <-- record() ok
        );
        assert_eq!(poh.remaining_hashes, 9);
    }
}
