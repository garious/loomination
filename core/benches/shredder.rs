#![allow(clippy::integer_arithmetic)]
#![feature(test)]

extern crate test;

use rand::seq::SliceRandom;
use raptorq::{Decoder, Encoder};
use solana_entry::entry::{create_ticks, Entry};
use solana_ledger::shred::{
    max_entries_per_n_shred, max_ticks_per_n_shreds, ProcessShredsStats, Shred, Shredder,
    MAX_DATA_SHREDS_PER_FEC_BLOCK, SHRED_PAYLOAD_SIZE, SIZE_OF_CODING_SHRED_HEADERS,
    SIZE_OF_DATA_SHRED_PAYLOAD,
};
use solana_perf::test_tx;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Keypair;
use test::Bencher;

fn make_test_entry(txs_per_entry: u64) -> Entry {
    Entry {
        num_hashes: 100_000,
        hash: Hash::default(),
        transactions: vec![test_tx::test_tx(); txs_per_entry as usize],
    }
}
fn make_large_unchained_entries(txs_per_entry: u64, num_entries: u64) -> Vec<Entry> {
    (0..num_entries)
        .map(|_| make_test_entry(txs_per_entry))
        .collect()
}

fn make_shreds(num_shreds: usize) -> Vec<Shred> {
    let shred_size = SIZE_OF_DATA_SHRED_PAYLOAD;
    let txs_per_entry = 128;
    let num_entries = max_entries_per_n_shred(
        &make_test_entry(txs_per_entry),
        2 * num_shreds as u64,
        Some(shred_size),
    );
    let entries = make_large_unchained_entries(txs_per_entry, num_entries);
    let shredder = Shredder::new(1, 0, 0, 0).unwrap();
    let data_shreds = shredder
        .entries_to_data_shreds(
            &Keypair::new(),
            &entries,
            true, // is_last_in_slot
            0,    // next_shred_index
            0,    // fec_set_offset
            &mut ProcessShredsStats::default(),
        )
        .0;
    assert!(data_shreds.len() >= num_shreds);
    data_shreds
}

fn make_concatenated_shreds(num_shreds: usize) -> Vec<u8> {
    let data_shreds = make_shreds(num_shreds);
    let valid_shred_data_len = (SHRED_PAYLOAD_SIZE - SIZE_OF_CODING_SHRED_HEADERS) as usize;
    let mut data: Vec<u8> = vec![0; num_shreds * valid_shred_data_len];
    for (i, shred) in (data_shreds[0..num_shreds]).iter().enumerate() {
        data[i * valid_shred_data_len..(i + 1) * valid_shred_data_len]
            .copy_from_slice(&shred.payload[..valid_shred_data_len]);
    }

    data
}

#[bench]
fn bench_shredder_ticks(bencher: &mut Bencher) {
    let kp = Keypair::new();
    let shred_size = SIZE_OF_DATA_SHRED_PAYLOAD;
    let num_shreds = ((1000 * 1000) + (shred_size - 1)) / shred_size;
    // ~1Mb
    let num_ticks = max_ticks_per_n_shreds(1, Some(SIZE_OF_DATA_SHRED_PAYLOAD)) * num_shreds as u64;
    let entries = create_ticks(num_ticks, 0, Hash::default());
    bencher.iter(|| {
        let shredder = Shredder::new(1, 0, 0, 0).unwrap();
        shredder.entries_to_shreds(&kp, &entries, true, 0);
    })
}

#[bench]
fn bench_shredder_large_entries(bencher: &mut Bencher) {
    let kp = Keypair::new();
    let shred_size = SIZE_OF_DATA_SHRED_PAYLOAD;
    let num_shreds = ((1000 * 1000) + (shred_size - 1)) / shred_size;
    let txs_per_entry = 128;
    let num_entries = max_entries_per_n_shred(
        &make_test_entry(txs_per_entry),
        num_shreds as u64,
        Some(shred_size),
    );
    let entries = make_large_unchained_entries(txs_per_entry, num_entries);
    // 1Mb
    bencher.iter(|| {
        let shredder = Shredder::new(1, 0, 0, 0).unwrap();
        shredder.entries_to_shreds(&kp, &entries, true, 0);
    })
}

#[bench]
fn bench_deshredder(bencher: &mut Bencher) {
    let kp = Keypair::new();
    let shred_size = SIZE_OF_DATA_SHRED_PAYLOAD;
    // ~10Mb
    let num_shreds = ((10000 * 1000) + (shred_size - 1)) / shred_size;
    let num_ticks = max_ticks_per_n_shreds(1, Some(shred_size)) * num_shreds as u64;
    let entries = create_ticks(num_ticks, 0, Hash::default());
    let shredder = Shredder::new(1, 0, 0, 0).unwrap();
    let data_shreds = shredder.entries_to_shreds(&kp, &entries, true, 0).0;
    bencher.iter(|| {
        let raw = &mut Shredder::deshred(&data_shreds).unwrap();
        assert_ne!(raw.len(), 0);
    })
}

#[bench]
fn bench_deserialize_hdr(bencher: &mut Bencher) {
    let data = vec![0; SIZE_OF_DATA_SHRED_PAYLOAD];

    let shred = Shred::new_from_data(2, 1, 1, Some(&data), true, true, 0, 0, 1);

    bencher.iter(|| {
        let payload = shred.payload.clone();
        let _ = Shred::new_from_serialized_shred(payload).unwrap();
    })
}

#[bench]
fn bench_shredder_coding(bencher: &mut Bencher) {
    let symbol_count = MAX_DATA_SHREDS_PER_FEC_BLOCK as usize;
    let data_shreds = make_shreds(symbol_count);
    bencher.iter(|| {
        Shredder::generate_coding_shreds(
            &data_shreds[..symbol_count],
            true, // is_last_in_slot
        )
        .len();
    })
}

#[bench]
fn bench_shredder_decoding(bencher: &mut Bencher) {
    let symbol_count = MAX_DATA_SHREDS_PER_FEC_BLOCK as usize;
    let data_shreds = make_shreds(symbol_count);
    let coding_shreds = Shredder::generate_coding_shreds(
        &data_shreds[..symbol_count],
        true, // is_last_in_slot
    );
    bencher.iter(|| {
        Shredder::try_recovery(
            coding_shreds[..].to_vec(),
            symbol_count,
            symbol_count,
            0, // first index
            1, // slot
        )
        .unwrap();
    })
}

#[bench]
fn bench_shredder_coding_raptorq(bencher: &mut Bencher) {
    let symbol_count = MAX_DATA_SHREDS_PER_FEC_BLOCK;
    let data = make_concatenated_shreds(symbol_count as usize);
    let valid_shred_data_len = (SHRED_PAYLOAD_SIZE - SIZE_OF_CODING_SHRED_HEADERS) as usize;
    bencher.iter(|| {
        let encoder = Encoder::with_defaults(&data, valid_shred_data_len as u16);
        encoder.get_encoded_packets(symbol_count);
    })
}

#[bench]
fn bench_shredder_decoding_raptorq(bencher: &mut Bencher) {
    let symbol_count = MAX_DATA_SHREDS_PER_FEC_BLOCK;
    let data = make_concatenated_shreds(symbol_count as usize);
    let valid_shred_data_len = (SHRED_PAYLOAD_SIZE - SIZE_OF_CODING_SHRED_HEADERS) as usize;
    let encoder = Encoder::with_defaults(&data, valid_shred_data_len as u16);
    let mut packets = encoder.get_encoded_packets(symbol_count as u32);
    packets.shuffle(&mut rand::thread_rng());

    // Here we simulate losing 1 less than 50% of the packets randomly
    packets.truncate(packets.len() - packets.len() / 2 + 1);

    bencher.iter(|| {
        let mut decoder = Decoder::new(encoder.get_config());
        let mut result = None;
        for packet in &packets {
            result = decoder.decode(packet.clone());
            if result != None {
                break;
            }
        }
        assert_eq!(result.unwrap(), data);
    })
}
