// This bench attempts to justify the value of `solana_core::poh_service::NUM_HASHES_PER_BATCH`

#![feature(test)]
extern crate test;

use {
    solana_entry::poh::Poh,
    solana_poh::poh_service::DEFAULT_HASHES_PER_BATCH,
    solana_sdk::hash::Hash,
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    test::Bencher,
};

const NUM_HASHES: u64 = 30_000; // Should require ~10ms on a 2017 MacBook Pro

#[bench]
// No locking.  Fastest.
fn bench_poh_hash(bencher: &mut Bencher) {
    let mut poh = Poh::new(Hash::default(), None);
    bencher.iter(|| {
        poh.hash(NUM_HASHES);
    })
}

#[bench]
// Lock on each iteration.  Slowest.
fn bench_arc_mutex_poh_hash(bencher: &mut Bencher) {
    let poh = Arc::new(Mutex::new(Poh::new(Hash::default(), None)));
    bencher.iter(|| {
        for _ in 0..NUM_HASHES {
            poh.lock().unwrap().hash(1);
        }
    })
}

#[bench]
// Acquire lock every NUM_HASHES_PER_BATCH iterations.
// Speed should be close to bench_poh_hash() if NUM_HASHES_PER_BATCH is set well.
fn bench_arc_mutex_poh_batched_hash(bencher: &mut Bencher) {
    let poh = Arc::new(Mutex::new(Poh::new(Hash::default(), Some(NUM_HASHES))));
    //let exit = Arc::new(AtomicBool::new(false));
    let exit = Arc::new(AtomicBool::new(true));

    bencher.iter(|| {
        // NOTE: This block attempts to look as close as possible to `PohService::tick_producer()`
        loop {
            if poh.lock().unwrap().hash(DEFAULT_HASHES_PER_BATCH) {
                poh.lock().unwrap().tick().unwrap();
                if exit.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    })
}

#[bench]
// Worst case transaction record delay due to batch hashing at NUM_HASHES_PER_BATCH
fn bench_poh_lock_time_per_batch(bencher: &mut Bencher) {
    let mut poh = Poh::new(Hash::default(), None);
    bencher.iter(|| {
        poh.hash(DEFAULT_HASHES_PER_BATCH);
    })
}
