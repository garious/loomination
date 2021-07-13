#![allow(clippy::integer_arithmetic)]
#[macro_use]
extern crate log;

use {
    rayon::iter::*,
    solana_gossip::{
        cluster_info::{ClusterInfo, Node},
        crds::Cursor,
        gossip_service::GossipService,
    },
    solana_perf::packet::Packet,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{
        hash::Hash,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        timing::timestamp,
        transaction::Transaction,
    },
    solana_vote_program::{vote_instruction, vote_state::Vote},
    std::{
        net::UdpSocket,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::sleep,
        time::Duration,
    },
};

fn test_node(exit: &Arc<AtomicBool>) -> (Arc<ClusterInfo>, GossipService, UdpSocket) {
    let keypair = Arc::new(Keypair::new());
    let mut test_node = Node::new_localhost_with_pubkey(&keypair.pubkey());
    let cluster_info = Arc::new(ClusterInfo::new(test_node.info.clone(), keypair));
    let gossip_service = GossipService::new(
        &cluster_info,
        None,
        test_node.sockets.gossip,
        None,
        true, // should_check_duplicate_instance
        exit,
    );
    let _ = cluster_info.my_contact_info();
    (
        cluster_info,
        gossip_service,
        test_node.sockets.tvu.pop().unwrap(),
    )
}

fn test_node_with_bank(
    node_keypair: Arc<Keypair>,
    exit: &Arc<AtomicBool>,
    bank_forks: Arc<RwLock<BankForks>>,
) -> (Arc<ClusterInfo>, GossipService, UdpSocket) {
    let mut test_node = Node::new_localhost_with_pubkey(&node_keypair.pubkey());
    let cluster_info = Arc::new(ClusterInfo::new(test_node.info.clone(), node_keypair));
    let gossip_service = GossipService::new(
        &cluster_info,
        Some(bank_forks),
        test_node.sockets.gossip,
        None,
        true, // should_check_duplicate_instance
        exit,
    );
    let _ = cluster_info.my_contact_info();
    (
        cluster_info,
        gossip_service,
        test_node.sockets.tvu.pop().unwrap(),
    )
}

/// Test that the network converges.
/// Run until every node in the network has a full ContactInfo set.
/// Check that nodes stop sending updates after all the ContactInfo has been shared.
/// tests that actually use this function are below
fn run_gossip_topo<F>(num: usize, topo: F)
where
    F: Fn(&Vec<(Arc<ClusterInfo>, GossipService, UdpSocket)>),
{
    let exit = Arc::new(AtomicBool::new(false));
    let listen: Vec<_> = (0..num).map(|_| test_node(&exit)).collect();
    topo(&listen);
    let mut done = true;
    for i in 0..(num * 32) {
        done = true;
        let total: usize = listen.iter().map(|v| v.0.gossip_peers().len()).sum();
        if (total + num) * 10 > num * num * 9 {
            done = true;
            break;
        } else {
            trace!("not converged {} {} {}", i, total + num, num * num);
        }
        sleep(Duration::new(1, 0));
    }
    exit.store(true, Ordering::Relaxed);
    for (_, dr, _) in listen {
        dr.join().unwrap();
    }
    assert!(done);
}
/// ring a -> b -> c -> d -> e -> a
#[test]
fn gossip_ring() {
    solana_logger::setup();
    run_gossip_topo(50, |listen| {
        let num = listen.len();
        for n in 0..num {
            let y = n % listen.len();
            let x = (n + 1) % listen.len();
            let yv = &listen[y].0;
            let mut d = yv.lookup_contact_info(&yv.id(), |ci| ci.clone()).unwrap();
            d.wallclock = timestamp();
            listen[x].0.insert_info(d);
        }
    });
}

/// ring a -> b -> c -> d -> e -> a
#[test]
#[ignore]
fn gossip_ring_large() {
    solana_logger::setup();
    run_gossip_topo(600, |listen| {
        let num = listen.len();
        for n in 0..num {
            let y = n % listen.len();
            let x = (n + 1) % listen.len();
            let yv = &listen[y].0;
            let mut d = yv.lookup_contact_info(&yv.id(), |ci| ci.clone()).unwrap();
            d.wallclock = timestamp();
            listen[x].0.insert_info(d);
        }
    });
}
/// star a -> (b,c,d,e)
#[test]
fn gossip_star() {
    solana_logger::setup();
    run_gossip_topo(10, |listen| {
        let num = listen.len();
        for n in 0..(num - 1) {
            let x = 0;
            let y = (n + 1) % listen.len();
            let yv = &listen[y].0;
            let mut yd = yv.lookup_contact_info(&yv.id(), |ci| ci.clone()).unwrap();
            yd.wallclock = timestamp();
            let xv = &listen[x].0;
            xv.insert_info(yd);
            trace!("star leader {}", &xv.id());
        }
    });
}

/// rstar a <- (b,c,d,e)
#[test]
fn gossip_rstar() {
    solana_logger::setup();
    run_gossip_topo(10, |listen| {
        let num = listen.len();
        let xd = {
            let xv = &listen[0].0;
            xv.lookup_contact_info(&xv.id(), |ci| ci.clone()).unwrap()
        };
        trace!("rstar leader {}", xd.id);
        for n in 0..(num - 1) {
            let y = (n + 1) % listen.len();
            let yv = &listen[y].0;
            yv.insert_info(xd.clone());
            trace!("rstar insert {} into {}", xd.id, yv.id());
        }
    });
}

#[test]
pub fn cluster_info_retransmit() {
    solana_logger::setup();
    let exit = Arc::new(AtomicBool::new(false));
    trace!("c1:");
    let (c1, dr1, tn1) = test_node(&exit);
    trace!("c2:");
    let (c2, dr2, tn2) = test_node(&exit);
    trace!("c3:");
    let (c3, dr3, tn3) = test_node(&exit);
    let c1_contact_info = c1.my_contact_info();

    c2.insert_info(c1_contact_info.clone());
    c3.insert_info(c1_contact_info);

    let num = 3;

    //wait to converge
    trace!("waiting to converge:");
    let mut done = false;
    for _ in 0..30 {
        done = c1.gossip_peers().len() == num - 1
            && c2.gossip_peers().len() == num - 1
            && c3.gossip_peers().len() == num - 1;
        if done {
            break;
        }
        sleep(Duration::new(1, 0));
    }
    assert!(done);
    let mut p = Packet::default();
    p.meta.size = 10;
    let peers = c1.tvu_peers();
    let retransmit_peers: Vec<_> = peers.iter().collect();
    ClusterInfo::retransmit_to(&retransmit_peers, &p, &tn1, false);
    let res: Vec<_> = [tn1, tn2, tn3]
        .into_par_iter()
        .map(|s| {
            let mut p = Packet::default();
            s.set_read_timeout(Some(Duration::new(1, 0))).unwrap();
            let res = s.recv_from(&mut p.data);
            res.is_err() //true if failed to receive the retransmit packet
        })
        .collect();
    //true if failed receive the retransmit packet, r2, and r3 should succeed
    //r1 was the sender, so it should fail to receive the packet
    assert_eq!(res, [true, false, false]);
    exit.store(true, Ordering::Relaxed);
    dr1.join().unwrap();
    dr2.join().unwrap();
    dr3.join().unwrap();
}

#[test]
#[ignore]
pub fn cluster_info_scale() {
    use solana_measure::measure::Measure;
    use solana_perf::test_tx::test_tx;
    use solana_runtime::bank::Bank;
    use solana_runtime::genesis_utils::{
        create_genesis_config_with_vote_accounts, ValidatorVoteKeypairs,
    };
    solana_logger::setup();
    let exit = Arc::new(AtomicBool::new(false));
    let num_nodes: usize = std::env::var("NUM_NODES")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .expect("could not parse NUM_NODES as a number");

    let vote_keypairs: Vec<_> = (0..num_nodes)
        .map(|_| ValidatorVoteKeypairs::new_rand())
        .collect();
    let genesis_config_info = create_genesis_config_with_vote_accounts(
        10_000,
        &vote_keypairs,
        vec![100; vote_keypairs.len()],
    );
    let bank0 = Bank::new(&genesis_config_info.genesis_config);
    let bank_forks = Arc::new(RwLock::new(BankForks::new(bank0)));

    let nodes: Vec<_> = vote_keypairs
        .into_iter()
        .map(|keypairs| {
            test_node_with_bank(Arc::new(keypairs.node_keypair), &exit, bank_forks.clone())
        })
        .collect();
    let ci0 = nodes[0].0.my_contact_info();
    for node in &nodes[1..] {
        node.0.insert_info(ci0.clone());
    }

    let mut time = Measure::start("time");
    let mut done;
    let mut success = false;
    for _ in 0..30 {
        done = true;
        for (i, node) in nodes.iter().enumerate() {
            warn!("node {} peers: {}", i, node.0.gossip_peers().len());
            if node.0.gossip_peers().len() != num_nodes - 1 {
                done = false;
                break;
            }
        }
        if done {
            success = true;
            break;
        }
        sleep(Duration::from_secs(1));
    }
    time.stop();
    warn!("found {} nodes in {} success: {}", num_nodes, time, success);

    for num_votes in 1..1000 {
        let mut time = Measure::start("votes");
        let tx = test_tx();
        warn!("tx.message.account_keys: {:?}", tx.message.account_keys);
        let vote = Vote::new(
            vec![1, 3, num_votes + 5], // slots
            Hash::default(),
        );
        let ix = vote_instruction::vote(
            &Pubkey::new_unique(), // vote_pubkey
            &Pubkey::new_unique(), // authorized_voter_pubkey
            vote,
        );
        let tx = Transaction::new_with_payer(
            &[ix], // instructions
            None,  // payer
        );
        let tower = vec![num_votes + 5];
        nodes[0].0.push_vote(&tower, tx.clone());
        let mut success = false;
        for _ in 0..(30 * 5) {
            let mut not_done = 0;
            let mut num_old = 0;
            let mut num_push_total = 0;
            let mut num_pushes = 0;
            let mut num_pulls = 0;
            for (node, _, _) in nodes.iter() {
                //if node.0.get_votes(0).1.len() != (num_nodes * num_votes) {
                let has_tx = node
                    .get_votes(&mut Cursor::default())
                    .1
                    .iter()
                    .filter(|v| v.message.account_keys == tx.message.account_keys)
                    .count();
                let gossip = node.gossip.read().unwrap();
                num_old += gossip.push.num_old.load(Ordering::Relaxed);
                num_push_total += gossip.push.num_total.load(Ordering::Relaxed);
                num_pushes += gossip.push.num_pushes.load(Ordering::Relaxed);
                num_pulls += gossip.pull.num_pulls.load(Ordering::Relaxed);
                if has_tx == 0 {
                    not_done += 1;
                }
            }
            warn!("not_done: {}/{}", not_done, nodes.len());
            warn!("num_old: {}", num_old);
            warn!("num_push_total: {}", num_push_total);
            warn!("num_pushes: {}", num_pushes);
            warn!("num_pulls: {}", num_pulls);
            success = not_done < (nodes.len() / 20);
            if success {
                break;
            }
            sleep(Duration::from_millis(200));
        }
        time.stop();
        warn!(
            "propagated vote {} in {} success: {}",
            num_votes, time, success
        );
        sleep(Duration::from_millis(200));
        for (node, _, _) in nodes.iter() {
            let gossip = node.gossip.read().unwrap();
            gossip.push.num_old.store(0, Ordering::Relaxed);
            gossip.push.num_total.store(0, Ordering::Relaxed);
            gossip.push.num_pushes.store(0, Ordering::Relaxed);
            gossip.pull.num_pulls.store(0, Ordering::Relaxed);
        }
    }

    exit.store(true, Ordering::Relaxed);
    for node in nodes {
        node.1.join().unwrap();
    }
}
