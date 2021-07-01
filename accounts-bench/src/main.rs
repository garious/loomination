#![allow(clippy::integer_arithmetic)]
#[macro_use]
extern crate log;
use clap::{crate_description, crate_name, value_t, App, Arg};
use rayon::prelude::*;
use solana_measure::measure::Measure;
use solana_runtime::{
    accounts::{create_test_accounts, update_accounts_bench, Accounts},
    accounts_db::AccountShrinkThreshold,
    accounts_index::AccountSecondaryIndexes,
    ancestors::Ancestors,
};
use solana_sdk::{genesis_config::ClusterType, pubkey::Pubkey};
use std::{env, fs, path::PathBuf};

fn main() {
    solana_logger::setup();

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .version(solana_version::version!())
        .arg(
            Arg::with_name("num_slots")
                .long("num_slots")
                .takes_value(true)
                .value_name("SLOTS")
                .help("Number of slots to store to."),
        )
        .arg(
            Arg::with_name("num_accounts")
                .long("num_accounts")
                .takes_value(true)
                .value_name("NUM_ACCOUNTS")
                .help("Total number of accounts"),
        )
        .arg(
            Arg::with_name("iterations")
                .long("iterations")
                .takes_value(true)
                .value_name("ITERATIONS")
                .help("Number of bench iterations"),
        )
        .arg(
            Arg::with_name("clean")
                .long("clean")
                .takes_value(false)
                .help("Run clean"),
        )
        .get_matches();

    let num_slots = value_t!(matches, "num_slots", usize).unwrap_or(4);
    let num_accounts = value_t!(matches, "num_accounts", usize).unwrap_or(10_000);
    let iterations = value_t!(matches, "iterations", usize).unwrap_or(20);
    let clean = matches.is_present("clean");
    println!("clean: {:?}", clean);

    let path = PathBuf::from(env::var("FARF_DIR").unwrap_or_else(|_| "farf".to_owned()))
        .join("accounts-bench");
    println!("cleaning file system: {:?}", path);
    if fs::remove_dir_all(path.clone()).is_err() {
        println!("Warning: Couldn't remove {:?}", path);
    }
    let accounts = Accounts::new_with_config(
        vec![path],
        &ClusterType::Testnet,
        AccountSecondaryIndexes::default(),
        false,
        AccountShrinkThreshold::default(),
        None,
    );
    println!("Creating {} accounts", num_accounts);
    let mut create_time = Measure::start("create accounts");
    let pubkeys: Vec<_> = (0..num_slots)
        .into_par_iter()
        .map(|slot| {
            let mut pubkeys: Vec<Pubkey> = vec![];
            create_test_accounts(
                &accounts,
                &mut pubkeys,
                num_accounts / num_slots,
                slot as u64,
            );
            pubkeys
        })
        .collect();
    let pubkeys: Vec<_> = pubkeys.into_iter().flatten().collect();
    create_time.stop();
    println!(
        "created {} accounts in {} slots {}",
        (num_accounts / num_slots) * num_slots,
        num_slots,
        create_time
    );
    let mut ancestors = Vec::with_capacity(num_slots);
    ancestors.push(0);
    for i in 1..num_slots {
        ancestors.push(i as u64);
        accounts.add_root(i as u64);
    }
    let ancestors = Ancestors::from(ancestors);
    let mut elapsed = vec![0; iterations];
    let mut elapsed_store = vec![0; iterations];
    for x in 0..iterations {
        if clean {
            let mut time = Measure::start("clean");
            accounts.accounts_db.clean_accounts(None, false);
            time.stop();
            println!("{}", time);
            for slot in 0..num_slots {
                update_accounts_bench(&accounts, &pubkeys, ((x + 1) * num_slots + slot) as u64);
                accounts.add_root((x * num_slots + slot) as u64);
            }
        } else {
            let mut pubkeys: Vec<Pubkey> = vec![];
            let mut time = Measure::start("hash");
            let results = accounts.accounts_db.update_accounts_hash(0, &ancestors);
            time.stop();
            let mut time_store = Measure::start("hash using store");
            let results_store = accounts.accounts_db.update_accounts_hash_with_index_option(
                false,
                false,
                solana_sdk::clock::Slot::default(),
                &ancestors,
                None,
                false,
            );
            time_store.stop();
            if results != results_store {
                error!("results different: \n{:?}\n{:?}", results, results_store);
            }
            println!(
                "hash,{},{},{},{}%",
                results.0,
                time,
                time_store,
                (time_store.as_us() as f64 / time.as_us() as f64 * 100.0f64) as u32
            );
            create_test_accounts(&accounts, &mut pubkeys, 1, 0);
            elapsed[x] = time.as_us();
            elapsed_store[x] = time_store.as_us();
        }
    }

    for x in elapsed {
        info!("update_accounts_hash(us),{}", x);
    }
    for x in elapsed_store {
        info!("calculate_accounts_hash_without_index(us),{}", x);
    }
}
