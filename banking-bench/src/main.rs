#![allow(clippy::integer_arithmetic)]
use clap::{crate_description, crate_name, value_t, App, Arg};
use crossbeam_channel::unbounded;
use log::*;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use solana_core::{banking_stage::BankingStage, cost_model::CostModel, cost_tracker::CostTracker};
use solana_gossip::{cluster_info::ClusterInfo, cluster_info::Node};
use solana_ledger::{
    blockstore::Blockstore,
    genesis_utils::{create_genesis_config, GenesisConfigInfo},
    get_tmp_ledger_path,
};
use solana_measure::measure::Measure;
use solana_perf::packet::to_packets_chunked;
use solana_poh::poh_recorder::{create_test_recorder, PohRecorder, WorkingBankEntry};
use solana_runtime::{
    accounts_background_service::AbsRequestSender, bank::Bank, bank_forks::BankForks,
};
use solana_sdk::{
    hash::Hash,
    signature::Keypair,
    signature::Signature,
    system_transaction,
    timing::{duration_as_us, timestamp},
    transaction::Transaction,
};
use std::{
    sync::{
        atomic::Ordering,
        mpsc::{channel, Receiver},
        Arc, Mutex, RwLock,
    },
    thread::sleep,
    time::{Duration, Instant},
};

fn check_txs(
    receiver: &Arc<Receiver<WorkingBankEntry>>,
    ref_tx_count: usize,
    poh_recorder: &Arc<Mutex<PohRecorder>>,
) -> bool {
    let mut total = 0;
    let now = Instant::now();
    let mut no_bank = false;
    loop {
        if let Ok((_bank, (entry, _tick_height))) = receiver.recv_timeout(Duration::from_millis(10))
        {
            total += entry.transactions.len();
        }
        if total >= ref_tx_count {
            break;
        }
        if now.elapsed().as_secs() > 60 {
            break;
        }
        if poh_recorder.lock().unwrap().bank().is_none() {
            trace!("no bank");
            no_bank = true;
            break;
        }
    }
    if !no_bank {
        assert!(total >= ref_tx_count);
    }
    no_bank
}

fn make_accounts_txs(
    total_num_transactions: usize,
    hash: Hash,
    same_payer: bool,
) -> Vec<Transaction> {
    let to_pubkey = solana_sdk::pubkey::new_rand();
    let payer_key = Keypair::new();
    let dummy = system_transaction::transfer(&payer_key, &to_pubkey, 1, hash);
    (0..total_num_transactions)
        .into_par_iter()
        .map(|_| {
            let mut new = dummy.clone();
            let sig: Vec<u8> = (0..64).map(|_| thread_rng().gen::<u8>()).collect();
            if !same_payer {
                new.message.account_keys[0] = solana_sdk::pubkey::new_rand();
            }
            new.message.account_keys[1] = solana_sdk::pubkey::new_rand();
            new.signatures = vec![Signature::new(&sig[0..64])];
            new
        })
        .collect()
}

struct Config {
    packets_per_batch: usize,
    chunk_len: usize,
    num_threads: usize,
}

impl Config {
    fn get_transactions_index(&self, chunk_index: usize) -> usize {
        chunk_index * (self.chunk_len / self.num_threads) * self.packets_per_batch
    }
}

fn bytes_as_usize(bytes: &[u8]) -> usize {
    bytes[0] as usize | (bytes[1] as usize) << 8
}

#[allow(clippy::cognitive_complexity)]
fn main() {
    solana_logger::setup();

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::with_name("num_chunks")
                .long("num-chunks")
                .takes_value(true)
                .value_name("SIZE")
                .help("Number of transaction chunks."),
        )
        .arg(
            Arg::with_name("packets_per_chunk")
                .long("packets-per-chunk")
                .takes_value(true)
                .value_name("SIZE")
                .help("Packets per chunk"),
        )
        .arg(
            Arg::with_name("skip_sanity")
                .long("skip-sanity")
                .takes_value(false)
                .help("Skip transaction sanity execution"),
        )
        .arg(
            Arg::with_name("same_payer")
                .long("same-payer")
                .takes_value(false)
                .help("Use the same payer for transfers"),
        )
        .arg(
            Arg::with_name("iterations")
                .long("iterations")
                .takes_value(true)
                .help("Number of iterations"),
        )
        .arg(
            Arg::with_name("num_threads")
                .long("num-threads")
                .takes_value(true)
                .help("Number of iterations"),
        )
        .get_matches();

    let num_threads =
        value_t!(matches, "num_threads", usize).unwrap_or(BankingStage::num_threads() as usize);
    //   a multiple of packet chunk duplicates to avoid races
    let num_chunks = value_t!(matches, "num_chunks", usize).unwrap_or(16);
    let packets_per_chunk = value_t!(matches, "packets_per_chunk", usize).unwrap_or(192);
    let iterations = value_t!(matches, "iterations", usize).unwrap_or(1000);

    let total_num_transactions = num_chunks * num_threads * packets_per_chunk;
    let mint_total = 1_000_000_000_000;
    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config(mint_total);

    let (verified_sender, verified_receiver) = unbounded();
    let (vote_sender, vote_receiver) = unbounded();
    let (replay_vote_sender, _replay_vote_receiver) = unbounded();
    let bank0 = Bank::new(&genesis_config);
    let mut bank_forks = BankForks::new(bank0);
    let mut bank = bank_forks.working_bank();

    info!("threads: {} txs: {}", num_threads, total_num_transactions);

    let same_payer = matches.is_present("same_payer");
    let mut transactions =
        make_accounts_txs(total_num_transactions, genesis_config.hash(), same_payer);

    // fund all the accounts
    transactions.iter().for_each(|tx| {
        let mut fund = system_transaction::transfer(
            &mint_keypair,
            &tx.message.account_keys[0],
            mint_total / total_num_transactions as u64,
            genesis_config.hash(),
        );
        // Ignore any pesky duplicate signature errors in the case we are using single-payer
        let sig: Vec<u8> = (0..64).map(|_| thread_rng().gen::<u8>()).collect();
        fund.signatures = vec![Signature::new(&sig[0..64])];
        let x = bank.process_transaction(&fund);
        x.unwrap();
    });

    let skip_sanity = matches.is_present("skip_sanity");
    if !skip_sanity {
        //sanity check, make sure all the transactions can execute sequentially
        transactions.iter().for_each(|tx| {
            let res = bank.process_transaction(tx);
            assert!(res.is_ok(), "sanity test transactions error: {:?}", res);
        });
        bank.clear_signatures();
        //sanity check, make sure all the transactions can execute in parallel
        let res = bank.process_transactions(&transactions);
        for r in res {
            assert!(r.is_ok(), "sanity parallel execution error: {:?}", r);
        }
        bank.clear_signatures();
    }

    let mut verified: Vec<_> = to_packets_chunked(&transactions, packets_per_chunk);
    let ledger_path = get_tmp_ledger_path!();
    {
        let blockstore = Arc::new(
            Blockstore::open(&ledger_path).expect("Expected to be able to open database ledger"),
        );
        let (exit, poh_recorder, poh_service, signal_receiver) =
            create_test_recorder(&bank, &blockstore, None);
        let cluster_info = ClusterInfo::new_with_invalid_keypair(Node::new_localhost().info);
        let cluster_info = Arc::new(cluster_info);
        let (cost_tracking_sender, _) = channel();
        let banking_stage = BankingStage::new(
            &cluster_info,
            &poh_recorder,
            verified_receiver,
            vote_receiver,
            None,
            replay_vote_sender,
            Arc::new(RwLock::new(CostTracker::new(Arc::new(RwLock::new(
                CostModel::default(),
            ))))),
            cost_tracking_sender,
        );
        poh_recorder.lock().unwrap().set_bank(&bank);

        let chunk_len = verified.len() / num_chunks;
        let mut start = 0;

        // This is so that the signal_receiver does not go out of scope after the closure.
        // If it is dropped before poh_service, then poh_service will error when
        // calling send() on the channel.
        let signal_receiver = Arc::new(signal_receiver);
        let mut total_us = 0;
        let mut tx_total_us = 0;
        let base_tx_count = bank.transaction_count();
        let mut txs_processed = 0;
        let mut root = 1;
        let collector = solana_sdk::pubkey::new_rand();
        let config = Config {
            packets_per_batch: packets_per_chunk,
            chunk_len,
            num_threads,
        };
        let mut total_sent = 0;
        for _ in 0..iterations {
            let now = Instant::now();
            let mut sent = 0;

            for (i, v) in verified[start..start + chunk_len]
                .chunks(chunk_len / num_threads)
                .enumerate()
            {
                let mut byte = 0;
                let index = config.get_transactions_index(start + i);
                if index < transactions.len() {
                    byte = bytes_as_usize(transactions[index].signatures[0].as_ref());
                }
                trace!(
                    "sending... {}..{} {} v.len: {} sig: {} transactions.len: {} index: {}",
                    start + i,
                    start + chunk_len,
                    timestamp(),
                    v.len(),
                    byte,
                    transactions.len(),
                    index,
                );
                for xv in v {
                    sent += xv.packets.len();
                }
                verified_sender.send(v.to_vec()).unwrap();
            }
            let start_tx_index = config.get_transactions_index(start);
            let end_tx_index = config.get_transactions_index(start + chunk_len);
            for tx in &transactions[start_tx_index..end_tx_index] {
                loop {
                    if bank.get_signature_status(&tx.signatures[0]).is_some() {
                        break;
                    }
                    if poh_recorder.lock().unwrap().bank().is_none() {
                        break;
                    }
                    sleep(Duration::from_millis(5));
                }
            }
            if check_txs(
                &signal_receiver,
                total_num_transactions / num_chunks,
                &poh_recorder,
            ) {
                debug!(
                    "resetting bank {} tx count: {} txs_proc: {}",
                    bank.slot(),
                    bank.transaction_count(),
                    txs_processed
                );
                assert!(txs_processed < bank.transaction_count());
                txs_processed = bank.transaction_count();
                tx_total_us += duration_as_us(&now.elapsed());

                let mut poh_time = Measure::start("poh_time");
                poh_recorder.lock().unwrap().reset(
                    bank.last_blockhash(),
                    bank.slot(),
                    Some((bank.slot(), bank.slot() + 1)),
                );
                poh_time.stop();

                let mut new_bank_time = Measure::start("new_bank");
                let new_bank = Bank::new_from_parent(&bank, &collector, bank.slot() + 1);
                new_bank_time.stop();

                let mut insert_time = Measure::start("insert_time");
                bank_forks.insert(new_bank);
                bank = bank_forks.working_bank();
                insert_time.stop();

                poh_recorder.lock().unwrap().set_bank(&bank);
                assert!(poh_recorder.lock().unwrap().bank().is_some());
                if bank.slot() > 32 {
                    bank_forks.set_root(root, &AbsRequestSender::default(), None);
                    root += 1;
                }
                debug!(
                    "new_bank_time: {}us insert_time: {}us poh_time: {}us",
                    new_bank_time.as_us(),
                    insert_time.as_us(),
                    poh_time.as_us(),
                );
            } else {
                tx_total_us += duration_as_us(&now.elapsed());
            }

            // This signature clear may not actually clear the signatures
            // in this chunk, but since we rotate between CHUNKS then
            // we should clear them by the time we come around again to re-use that chunk.
            bank.clear_signatures();
            total_us += duration_as_us(&now.elapsed());
            debug!(
                "time: {} us checked: {} sent: {}",
                duration_as_us(&now.elapsed()),
                total_num_transactions / num_chunks,
                sent,
            );
            total_sent += sent;

            if bank.slot() > 0 && bank.slot() % 16 == 0 {
                for tx in transactions.iter_mut() {
                    tx.message.recent_blockhash = bank.last_blockhash();
                    let sig: Vec<u8> = (0..64).map(|_| thread_rng().gen::<u8>()).collect();
                    tx.signatures[0] = Signature::new(&sig[0..64]);
                }
                verified = to_packets_chunked(&transactions.clone(), packets_per_chunk);
            }

            start += chunk_len;
            start %= verified.len();
        }
        let txs_processed = bank_forks.working_bank().transaction_count();
        debug!("processed: {} base: {}", txs_processed, base_tx_count);
        eprintln!(
            "{{'name': 'banking_bench_total', 'median': '{:.2}'}}",
            (1000.0 * 1000.0 * total_sent as f64) / (total_us as f64),
        );
        eprintln!(
            "{{'name': 'banking_bench_tx_total', 'median': '{:.2}'}}",
            (1000.0 * 1000.0 * total_sent as f64) / (tx_total_us as f64),
        );
        eprintln!(
            "{{'name': 'banking_bench_success_tx_total', 'median': '{:.2}'}}",
            (1000.0 * 1000.0 * (txs_processed - base_tx_count) as f64) / (total_us as f64),
        );

        drop(verified_sender);
        drop(vote_sender);
        exit.store(true, Ordering::Relaxed);
        banking_stage.join().unwrap();
        debug!("waited for banking_stage");
        poh_service.join().unwrap();
        sleep(Duration::from_secs(1));
        debug!("waited for poh_service");
    }
    let _unused = Blockstore::destroy(&ledger_path);
}
