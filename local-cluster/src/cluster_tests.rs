/// Cluster independent integration tests
///
/// All tests must start from an entry point and a funding keypair and
/// discover the rest of the network.
use log::*;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use solana_client::thin_client::create_client;
use solana_core::consensus::VOTE_THRESHOLD_DEPTH;
use solana_entry::entry::{Entry, EntrySlice};
use solana_gossip::{
    cluster_info::VALIDATOR_PORT_RANGE, contact_info::ContactInfo, gossip_service::discover_cluster,
};
use solana_ledger::blockstore::Blockstore;
use solana_sdk::{
    client::SyncClient,
    clock::{self, Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
    commitment_config::CommitmentConfig,
    epoch_schedule::MINIMUM_SLOTS_PER_EPOCH,
    exit::Exit,
    hash::Hash,
    poh_config::PohConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_transaction,
    timing::duration_as_ms,
    transport::TransportError,
};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::{Arc, RwLock},
    thread::sleep,
    time::{Duration, Instant},
};

/// Spend and verify from every node in the network
pub fn spend_and_verify_all_nodes<S: ::std::hash::BuildHasher + Sync + Send>(
    entry_point_info: &ContactInfo,
    funding_keypair: &Keypair,
    nodes: usize,
    ignore_nodes: HashSet<Pubkey, S>,
) {
    let cluster_nodes = discover_cluster(&entry_point_info.gossip, nodes).unwrap();
    assert!(cluster_nodes.len() >= nodes);
    let ignore_nodes = Arc::new(ignore_nodes);
    cluster_nodes.par_iter().for_each(|ingress_node| {
        if ignore_nodes.contains(&ingress_node.id) {
            return;
        }
        let random_keypair = Keypair::new();
        let client = create_client(ingress_node.client_facing_addr(), VALIDATOR_PORT_RANGE);
        let bal = client
            .poll_get_balance_with_commitment(
                &funding_keypair.pubkey(),
                CommitmentConfig::processed(),
            )
            .expect("balance in source");
        assert!(bal > 0);
        let (blockhash, _fee_calculator, _last_valid_slot) = client
            .get_recent_blockhash_with_commitment(CommitmentConfig::confirmed())
            .unwrap();
        let mut transaction =
            system_transaction::transfer(funding_keypair, &random_keypair.pubkey(), 1, blockhash);
        let confs = VOTE_THRESHOLD_DEPTH + 1;
        let sig = client
            .retry_transfer_until_confirmed(funding_keypair, &mut transaction, 10, confs)
            .unwrap();
        for validator in &cluster_nodes {
            if ignore_nodes.contains(&validator.id) {
                continue;
            }
            let client = create_client(validator.client_facing_addr(), VALIDATOR_PORT_RANGE);
            client.poll_for_signature_confirmation(&sig, confs).unwrap();
        }
    });
}

pub fn verify_balances<S: ::std::hash::BuildHasher>(
    expected_balances: HashMap<Pubkey, u64, S>,
    node: &ContactInfo,
) {
    let client = create_client(node.client_facing_addr(), VALIDATOR_PORT_RANGE);
    for (pk, b) in expected_balances {
        let bal = client
            .poll_get_balance_with_commitment(&pk, CommitmentConfig::processed())
            .expect("balance in source");
        assert_eq!(bal, b);
    }
}

pub fn send_many_transactions(
    node: &ContactInfo,
    funding_keypair: &Keypair,
    max_tokens_per_transfer: u64,
    num_txs: u64,
) -> HashMap<Pubkey, u64> {
    let client = create_client(node.client_facing_addr(), VALIDATOR_PORT_RANGE);
    let mut expected_balances = HashMap::new();
    for _ in 0..num_txs {
        let random_keypair = Keypair::new();
        let bal = client
            .poll_get_balance_with_commitment(
                &funding_keypair.pubkey(),
                CommitmentConfig::processed(),
            )
            .expect("balance in source");
        assert!(bal > 0);
        let (blockhash, _fee_calculator, _last_valid_slot) = client
            .get_recent_blockhash_with_commitment(CommitmentConfig::processed())
            .unwrap();
        let transfer_amount = thread_rng().gen_range(1, max_tokens_per_transfer);

        let mut transaction = system_transaction::transfer(
            funding_keypair,
            &random_keypair.pubkey(),
            transfer_amount,
            blockhash,
        );

        client
            .retry_transfer(funding_keypair, &mut transaction, 5)
            .unwrap();

        expected_balances.insert(random_keypair.pubkey(), transfer_amount);
    }

    expected_balances
}

pub fn verify_ledger_ticks(ledger_path: &Path, ticks_per_slot: usize) {
    let ledger = Blockstore::open(ledger_path).unwrap();
    let zeroth_slot = ledger.get_slot_entries(0, 0).unwrap();
    let last_id = zeroth_slot.last().unwrap().hash;
    let next_slots = ledger.get_slots_since(&[0]).unwrap().remove(&0).unwrap();
    let mut pending_slots: Vec<_> = next_slots
        .into_iter()
        .map(|slot| (slot, 0, last_id))
        .collect();
    while !pending_slots.is_empty() {
        let (slot, parent_slot, last_id) = pending_slots.pop().unwrap();
        let next_slots = ledger
            .get_slots_since(&[slot])
            .unwrap()
            .remove(&slot)
            .unwrap();

        // If you're not the last slot, you should have a full set of ticks
        let should_verify_ticks = if !next_slots.is_empty() {
            Some((slot - parent_slot) as usize * ticks_per_slot)
        } else {
            None
        };

        let last_id = verify_slot_ticks(&ledger, slot, &last_id, should_verify_ticks);
        pending_slots.extend(
            next_slots
                .into_iter()
                .map(|child_slot| (child_slot, slot, last_id)),
        );
    }
}

pub fn sleep_n_epochs(
    num_epochs: f64,
    config: &PohConfig,
    ticks_per_slot: u64,
    slots_per_epoch: u64,
) {
    let num_ticks_per_second = (1000 / duration_as_ms(&config.target_tick_duration)) as f64;
    let num_ticks_to_sleep = num_epochs * ticks_per_slot as f64 * slots_per_epoch as f64;
    let secs = ((num_ticks_to_sleep + num_ticks_per_second - 1.0) / num_ticks_per_second) as u64;
    warn!("sleep_n_epochs: {} seconds", secs);
    sleep(Duration::from_secs(secs));
}

pub fn kill_entry_and_spend_and_verify_rest(
    entry_point_info: &ContactInfo,
    entry_point_validator_exit: &Arc<RwLock<Exit>>,
    funding_keypair: &Keypair,
    nodes: usize,
    slot_millis: u64,
) {
    info!("kill_entry_and_spend_and_verify_rest...");
    let cluster_nodes = discover_cluster(&entry_point_info.gossip, nodes).unwrap();
    assert!(cluster_nodes.len() >= nodes);
    let client = create_client(entry_point_info.client_facing_addr(), VALIDATOR_PORT_RANGE);
    // sleep long enough to make sure we are in epoch 3
    let first_two_epoch_slots = MINIMUM_SLOTS_PER_EPOCH * (3 + 1);

    for ingress_node in &cluster_nodes {
        client
            .poll_get_balance_with_commitment(&ingress_node.id, CommitmentConfig::processed())
            .unwrap_or_else(|err| panic!("Node {} has no balance: {}", ingress_node.id, err));
    }

    info!("sleeping for 2 leader fortnights");
    sleep(Duration::from_millis(
        slot_millis * first_two_epoch_slots as u64,
    ));
    info!("done sleeping for first 2 warmup epochs");
    info!("killing entry point: {}", entry_point_info.id);
    entry_point_validator_exit.write().unwrap().exit();
    info!("sleeping for some time");
    sleep(Duration::from_millis(
        slot_millis * NUM_CONSECUTIVE_LEADER_SLOTS,
    ));
    info!("done sleeping for 2 fortnights");
    for ingress_node in &cluster_nodes {
        if ingress_node.id == entry_point_info.id {
            info!("ingress_node.id == entry_point_info.id, continuing...");
            continue;
        }

        let client = create_client(ingress_node.client_facing_addr(), VALIDATOR_PORT_RANGE);
        let balance = client
            .poll_get_balance_with_commitment(
                &funding_keypair.pubkey(),
                CommitmentConfig::processed(),
            )
            .expect("balance in source");
        assert_ne!(balance, 0);

        let mut result = Ok(());
        let mut retries = 0;
        loop {
            retries += 1;
            if retries > 5 {
                result.unwrap();
            }

            let random_keypair = Keypair::new();
            let (blockhash, _fee_calculator, _last_valid_slot) = client
                .get_recent_blockhash_with_commitment(CommitmentConfig::processed())
                .unwrap();
            let mut transaction = system_transaction::transfer(
                funding_keypair,
                &random_keypair.pubkey(),
                1,
                blockhash,
            );

            let confs = VOTE_THRESHOLD_DEPTH + 1;
            let sig = {
                let sig = client.retry_transfer_until_confirmed(
                    funding_keypair,
                    &mut transaction,
                    5,
                    confs,
                );
                match sig {
                    Err(e) => {
                        result = Err(e);
                        continue;
                    }

                    Ok(sig) => sig,
                }
            };
            info!("poll_all_nodes_for_signature()");
            match poll_all_nodes_for_signature(entry_point_info, &cluster_nodes, &sig, confs) {
                Err(e) => {
                    info!("poll_all_nodes_for_signature() failed {:?}", e);
                    result = Err(e);
                }
                Ok(()) => {
                    info!("poll_all_nodes_for_signature() succeeded, done.");
                    break;
                }
            }
        }
    }
}

pub fn check_for_new_roots(num_new_roots: usize, contact_infos: &[ContactInfo], test_name: &str) {
    let mut roots = vec![HashSet::new(); contact_infos.len()];
    let mut done = false;
    let mut last_print = Instant::now();
    let loop_start = Instant::now();
    let loop_timeout = Duration::from_secs(60);
    let mut num_roots_map = HashMap::new();
    while !done {
        assert!(loop_start.elapsed() < loop_timeout);

        for (i, ingress_node) in contact_infos.iter().enumerate() {
            let client = create_client(ingress_node.client_facing_addr(), VALIDATOR_PORT_RANGE);
            let root_slot = client
                .get_slot_with_commitment(CommitmentConfig::finalized())
                .unwrap_or(0);
            roots[i].insert(root_slot);
            num_roots_map.insert(ingress_node.id, roots[i].len());
            let num_roots = roots.iter().map(|r| r.len()).min().unwrap();
            done = num_roots >= num_new_roots;
            if done || last_print.elapsed().as_secs() > 3 {
                info!(
                    "{} waiting for {} new roots.. observed: {:?}",
                    test_name, num_new_roots, num_roots_map
                );
                last_print = Instant::now();
            }
        }
        sleep(Duration::from_millis(clock::DEFAULT_MS_PER_SLOT / 2));
    }
}

pub fn check_no_new_roots(
    num_slots_to_wait: usize,
    contact_infos: &[ContactInfo],
    test_name: &str,
) {
    assert!(!contact_infos.is_empty());
    let mut roots = vec![0; contact_infos.len()];
    let max_slot = contact_infos
        .iter()
        .enumerate()
        .map(|(i, ingress_node)| {
            let client = create_client(ingress_node.client_facing_addr(), VALIDATOR_PORT_RANGE);
            let initial_root = client
                .get_slot()
                .unwrap_or_else(|_| panic!("get_slot for {} failed", ingress_node.id));
            roots[i] = initial_root;
            client
                .get_slot_with_commitment(CommitmentConfig::processed())
                .unwrap_or_else(|_| panic!("get_slot for {} failed", ingress_node.id))
        })
        .max()
        .unwrap();

    let end_slot = max_slot + num_slots_to_wait as u64;
    let mut current_slot;
    let mut last_print = Instant::now();
    let mut reached_end_slot = false;
    loop {
        for contact_info in contact_infos {
            let client = create_client(contact_info.client_facing_addr(), VALIDATOR_PORT_RANGE);
            current_slot = client
                .get_slot_with_commitment(CommitmentConfig::processed())
                .unwrap_or_else(|_| panic!("get_slot for {} failed", contact_infos[0].id));
            if current_slot > end_slot {
                reached_end_slot = true;
                break;
            }
            if last_print.elapsed().as_secs() > 3 {
                info!(
                    "{} current slot: {} on validator: {}, waiting for any validator with slot: {}",
                    test_name, current_slot, contact_info.id, end_slot
                );
                last_print = Instant::now();
            }
        }
        if reached_end_slot {
            break;
        }
    }

    for (i, ingress_node) in contact_infos.iter().enumerate() {
        let client = create_client(ingress_node.client_facing_addr(), VALIDATOR_PORT_RANGE);
        assert_eq!(
            client
                .get_slot()
                .unwrap_or_else(|_| panic!("get_slot for {} failed", ingress_node.id)),
            roots[i]
        );
    }
}

fn poll_all_nodes_for_signature(
    entry_point_info: &ContactInfo,
    cluster_nodes: &[ContactInfo],
    sig: &Signature,
    confs: usize,
) -> Result<(), TransportError> {
    for validator in cluster_nodes {
        if validator.id == entry_point_info.id {
            continue;
        }
        let client = create_client(validator.client_facing_addr(), VALIDATOR_PORT_RANGE);
        client.poll_for_signature_confirmation(sig, confs)?;
    }

    Ok(())
}

fn get_and_verify_slot_entries(
    blockstore: &Blockstore,
    slot: Slot,
    last_entry: &Hash,
) -> Vec<Entry> {
    let entries = blockstore.get_slot_entries(slot, 0).unwrap();
    assert!(entries.verify(last_entry));
    entries
}

fn verify_slot_ticks(
    blockstore: &Blockstore,
    slot: Slot,
    last_entry: &Hash,
    expected_num_ticks: Option<usize>,
) -> Hash {
    let entries = get_and_verify_slot_entries(blockstore, slot, last_entry);
    let num_ticks: usize = entries.iter().map(|entry| entry.is_tick() as usize).sum();
    if let Some(expected_num_ticks) = expected_num_ticks {
        assert_eq!(num_ticks, expected_num_ticks);
    }
    entries.last().unwrap().hash
}
