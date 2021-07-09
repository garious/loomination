#![feature(test)]

extern crate solana_core;
extern crate test;

use log::*;
use solana_core::retransmit_stage::retransmitter;
use solana_entry::entry::Entry;
use solana_gossip::cluster_info::{ClusterInfo, Node};
use solana_gossip::contact_info::ContactInfo;
use solana_ledger::genesis_utils::{create_genesis_config, GenesisConfigInfo};
use solana_ledger::leader_schedule_cache::LeaderScheduleCache;
use solana_ledger::shred::Shredder;
use solana_measure::measure::Measure;
use solana_perf::packet::{Packet, Packets};
use solana_rpc::max_slots::MaxSlots;
use solana_runtime::bank::Bank;
use solana_runtime::bank_forks::BankForks;
use solana_sdk::hash::Hash;
use solana_sdk::pubkey;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::system_transaction;
use solana_sdk::timing::timestamp;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::Mutex;
use std::sync::{Arc, RwLock};
use std::thread::sleep;
use std::thread::Builder;
use std::time::Duration;
use test::Bencher;

#[bench]
#[allow(clippy::same_item_push)]
fn bench_retransmitter(bencher: &mut Bencher) {
    solana_logger::setup();
    let cluster_info = ClusterInfo::new_with_invalid_keypair(Node::new_localhost().info);
    const NUM_PEERS: usize = 4;
    let mut peer_sockets = Vec::new();
    for _ in 0..NUM_PEERS {
        // This ensures that cluster_info.id() is the root of turbine
        // retransmit tree and so the shreds are retransmited to all other
        // nodes in the cluster.
        let id = std::iter::repeat_with(pubkey::new_rand)
            .find(|pk| cluster_info.id() < *pk)
            .unwrap();
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let mut contact_info = ContactInfo::new_localhost(&id, timestamp());
        contact_info.tvu = socket.local_addr().unwrap();
        contact_info.tvu.set_ip("127.0.0.1".parse().unwrap());
        contact_info.tvu_forwards = contact_info.tvu;
        info!("local: {:?}", contact_info.tvu);
        cluster_info.insert_info(contact_info);
        socket.set_nonblocking(true).unwrap();
        peer_sockets.push(socket);
    }
    let peer_sockets = Arc::new(peer_sockets);
    let cluster_info = Arc::new(cluster_info);

    let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(100_000);
    let bank0 = Bank::new(&genesis_config);
    let bank_forks = BankForks::new(bank0);
    let bank = bank_forks.working_bank();
    let bank_forks = Arc::new(RwLock::new(bank_forks));
    let (packet_sender, packet_receiver) = channel();
    let packet_receiver = Arc::new(Mutex::new(packet_receiver));
    const NUM_THREADS: usize = 2;
    let sockets = (0..NUM_THREADS)
        .map(|_| UdpSocket::bind("0.0.0.0:0").unwrap())
        .collect();

    let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));

    // To work reliably with higher values, this needs larger udp rmem size
    let entries: Vec<_> = (0..5)
        .map(|_| {
            let keypair0 = Keypair::new();
            let keypair1 = Keypair::new();
            let tx0 =
                system_transaction::transfer(&keypair0, &keypair1.pubkey(), 1, Hash::default());
            Entry::new(&Hash::default(), 1, vec![tx0])
        })
        .collect();

    let keypair = Keypair::new();
    let slot = 0;
    let parent = 0;
    let shredder = Shredder::new(slot, parent, 0, 0).unwrap();
    let mut data_shreds = shredder.entries_to_shreds(&keypair, &entries, true, 0).0;

    let num_packets = data_shreds.len();

    let retransmitter_handles = retransmitter(
        Arc::new(sockets),
        bank_forks,
        &leader_schedule_cache,
        cluster_info,
        packet_receiver,
        &Arc::new(MaxSlots::default()),
        None,
    );

    let mut index = 0;
    let mut slot = 0;
    let total = Arc::new(AtomicUsize::new(0));
    bencher.iter(move || {
        let peer_sockets1 = peer_sockets.clone();
        let handles: Vec<_> = (0..NUM_PEERS)
            .map(|p| {
                let peer_sockets2 = peer_sockets1.clone();
                let total2 = total.clone();
                Builder::new()
                    .name("recv".to_string())
                    .spawn(move || {
                        info!("{} waiting on {:?}", p, peer_sockets2[p]);
                        let mut buf = [0u8; 1024];
                        loop {
                            while peer_sockets2[p].recv(&mut buf).is_ok() {
                                total2.fetch_add(1, Ordering::Relaxed);
                            }
                            if total2.load(Ordering::Relaxed) >= num_packets {
                                break;
                            }
                            info!("{} recv", total2.load(Ordering::Relaxed));
                            sleep(Duration::from_millis(1));
                        }
                    })
                    .unwrap()
            })
            .collect();

        for shred in data_shreds.iter_mut() {
            shred.set_slot(slot);
            shred.set_index(index);
            index += 1;
            index %= 200;
            let mut p = Packet::default();
            shred.copy_to_packet(&mut p);
            let _ = packet_sender.send(Packets::new(vec![p]));
        }
        slot += 1;

        info!("sent...");

        let mut join_time = Measure::start("join");
        for h in handles {
            h.join().unwrap();
        }
        join_time.stop();
        info!("took: {}ms", join_time.as_ms());

        total.store(0, Ordering::Relaxed);
    });

    for t in retransmitter_handles {
        t.join().unwrap();
    }
}
