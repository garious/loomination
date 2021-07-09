#![allow(clippy::integer_arithmetic)]
#![feature(test)]
extern crate solana_ledger;
extern crate test;

use rand::Rng;
use solana_entry::entry::{create_ticks, Entry};
use solana_ledger::{
    blockstore::{entries_to_test_shreds, Blockstore},
    get_tmp_ledger_path,
};
use solana_sdk::{clock::Slot, hash::Hash};
use std::path::Path;
use test::Bencher;

// Given some shreds and a ledger at ledger_path, benchmark writing the shreds to the ledger
fn bench_write_shreds(bench: &mut Bencher, entries: Vec<Entry>, ledger_path: &Path) {
    let blockstore =
        Blockstore::open(ledger_path).expect("Expected to be able to open database ledger");
    bench.iter(move || {
        let shreds = entries_to_test_shreds(entries.clone(), 0, 0, true, 0);
        blockstore.insert_shreds(shreds, None, false).unwrap();
    });

    Blockstore::destroy(ledger_path).expect("Expected successful database destruction");
}

// Insert some shreds into the ledger in preparation for read benchmarks
fn setup_read_bench(
    blockstore: &mut Blockstore,
    num_small_shreds: u64,
    num_large_shreds: u64,
    slot: Slot,
) {
    // Make some big and small entries
    let entries = create_ticks(
        num_large_shreds * 4 + num_small_shreds * 2,
        0,
        Hash::default(),
    );

    // Convert the entries to shreds, write the shreds to the ledger
    let shreds = entries_to_test_shreds(entries, slot, slot.saturating_sub(1), true, 0);
    blockstore
        .insert_shreds(shreds, None, false)
        .expect("Expectd successful insertion of shreds into ledger");
}

// Write small shreds to the ledger
#[bench]
#[ignore]
fn bench_write_small(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let num_entries = 32 * 1024;
    let entries = create_ticks(num_entries, 0, Hash::default());
    bench_write_shreds(bench, entries, &ledger_path);
}

// Write big shreds to the ledger
#[bench]
#[ignore]
fn bench_write_big(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let num_entries = 32 * 1024;
    let entries = create_ticks(num_entries, 0, Hash::default());
    bench_write_shreds(bench, entries, &ledger_path);
}

#[bench]
#[ignore]
fn bench_read_sequential(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let mut blockstore =
        Blockstore::open(&ledger_path).expect("Expected to be able to open database ledger");

    // Insert some big and small shreds into the ledger
    let num_small_shreds = 32 * 1024;
    let num_large_shreds = 32 * 1024;
    let total_shreds = num_small_shreds + num_large_shreds;
    let slot = 0;
    setup_read_bench(&mut blockstore, num_small_shreds, num_large_shreds, slot);

    let num_reads = total_shreds / 15;
    let mut rng = rand::thread_rng();
    bench.iter(move || {
        // Generate random starting point in the range [0, total_shreds - 1], read num_reads shreds sequentially
        let start_index = rng.gen_range(0, num_small_shreds + num_large_shreds);
        for i in start_index..start_index + num_reads {
            let _ = blockstore.get_data_shred(slot, i as u64 % total_shreds);
        }
    });

    Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
}

#[bench]
#[ignore]
fn bench_read_random(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let mut blockstore =
        Blockstore::open(&ledger_path).expect("Expected to be able to open database ledger");

    // Insert some big and small shreds into the ledger
    let num_small_shreds = 32 * 1024;
    let num_large_shreds = 32 * 1024;
    let total_shreds = num_small_shreds + num_large_shreds;
    let slot = 0;
    setup_read_bench(&mut blockstore, num_small_shreds, num_large_shreds, slot);

    let num_reads = total_shreds / 15;

    // Generate a num_reads sized random sample of indexes in range [0, total_shreds - 1],
    // simulating random reads
    let mut rng = rand::thread_rng();
    let indexes: Vec<usize> = (0..num_reads)
        .map(|_| rng.gen_range(0, total_shreds) as usize)
        .collect();
    bench.iter(move || {
        for i in indexes.iter() {
            let _ = blockstore.get_data_shred(slot, *i as u64);
        }
    });

    Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
}

#[bench]
#[ignore]
fn bench_insert_data_shred_small(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let blockstore =
        Blockstore::open(&ledger_path).expect("Expected to be able to open database ledger");
    let num_entries = 32 * 1024;
    let entries = create_ticks(num_entries, 0, Hash::default());
    bench.iter(move || {
        let shreds = entries_to_test_shreds(entries.clone(), 0, 0, true, 0);
        blockstore.insert_shreds(shreds, None, false).unwrap();
    });
    Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
}

#[bench]
#[ignore]
fn bench_insert_data_shred_big(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path!();
    let blockstore =
        Blockstore::open(&ledger_path).expect("Expected to be able to open database ledger");
    let num_entries = 32 * 1024;
    let entries = create_ticks(num_entries, 0, Hash::default());
    bench.iter(move || {
        let shreds = entries_to_test_shreds(entries.clone(), 0, 0, true, 0);
        blockstore.insert_shreds(shreds, None, false).unwrap();
    });
    Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
}
