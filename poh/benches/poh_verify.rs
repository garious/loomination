#![feature(test)]
extern crate test;

use {
    solana_entry::entry::{next_entry_mut, Entry, EntrySlice},
    solana_sdk::{
        hash::{hash, Hash},
        signature::{Keypair, Signer},
        system_transaction,
    },
    test::Bencher,
};

const NUM_HASHES: u64 = 400;
const NUM_ENTRIES: usize = 800;

#[bench]
fn bench_poh_verify_ticks(bencher: &mut Bencher) {
    solana_logger::setup();
    let zero = Hash::default();
    let start_hash = hash(zero.as_ref());
    let mut cur_hash = start_hash;

    let mut ticks: Vec<Entry> = Vec::with_capacity(NUM_ENTRIES);
    for _ in 0..NUM_ENTRIES {
        ticks.push(next_entry_mut(&mut cur_hash, NUM_HASHES, vec![]));
    }

    bencher.iter(|| {
        assert!(ticks.verify(&start_hash));
    })
}

#[bench]
fn bench_poh_verify_transaction_entries(bencher: &mut Bencher) {
    let zero = Hash::default();
    let start_hash = hash(zero.as_ref());
    let mut cur_hash = start_hash;

    let keypair1 = Keypair::new();
    let pubkey1 = keypair1.pubkey();

    let mut ticks: Vec<Entry> = Vec::with_capacity(NUM_ENTRIES);
    for _ in 0..NUM_ENTRIES {
        let tx = system_transaction::transfer(&keypair1, &pubkey1, 42, cur_hash);
        ticks.push(next_entry_mut(&mut cur_hash, NUM_HASHES, vec![tx]));
    }

    bencher.iter(|| {
        assert!(ticks.verify(&start_hash));
    })
}
