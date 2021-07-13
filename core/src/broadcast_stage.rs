//! A stage to broadcast data from a leader node to validators
#![allow(clippy::rc_buffer)]
use self::{
    broadcast_duplicates_run::{BroadcastDuplicatesConfig, BroadcastDuplicatesRun},
    broadcast_fake_shreds_run::BroadcastFakeShredsRun,
    broadcast_metrics::*,
    fail_entry_verification_broadcast_run::FailEntryVerificationBroadcastRun,
    standard_broadcast_run::StandardBroadcastRun,
};
use crate::{
    cluster_nodes::ClusterNodes,
    result::{Error, Result},
};
use crossbeam_channel::{
    Receiver as CrossbeamReceiver, RecvTimeoutError as CrossbeamRecvTimeoutError,
    Sender as CrossbeamSender,
};
use solana_gossip::cluster_info::{ClusterInfo, ClusterInfoError};
use solana_ledger::{blockstore::Blockstore, shred::Shred};
use solana_measure::measure::Measure;
use solana_metrics::{inc_new_counter_error, inc_new_counter_info};
use solana_poh::poh_recorder::WorkingBankEntry;
use solana_runtime::{bank::Bank, bank_forks::BankForks};
use solana_sdk::timing::timestamp;
use solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Keypair};
use solana_streamer::sendmmsg::batch_send;
use std::sync::atomic::AtomicU64;
use std::{
    collections::HashMap,
    net::UdpSocket,
    sync::atomic::{AtomicBool, Ordering},
    sync::mpsc::{channel, Receiver, RecvError, RecvTimeoutError, Sender},
    sync::{Arc, Mutex, RwLock},
    thread::{self, Builder, JoinHandle},
    time::{Duration, Instant},
};

pub mod broadcast_duplicates_run;
mod broadcast_fake_shreds_run;
pub mod broadcast_metrics;
pub(crate) mod broadcast_utils;
mod fail_entry_verification_broadcast_run;
mod standard_broadcast_run;

pub(crate) const NUM_INSERT_THREADS: usize = 2;
pub(crate) type RetransmitSlotsSender = CrossbeamSender<HashMap<Slot, Arc<Bank>>>;
pub(crate) type RetransmitSlotsReceiver = CrossbeamReceiver<HashMap<Slot, Arc<Bank>>>;
pub(crate) type RecordReceiver = Receiver<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>;
pub(crate) type TransmitReceiver = Receiver<(TransmitShreds, Option<BroadcastShredBatchInfo>)>;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum BroadcastStageReturnType {
    ChannelDisconnected,
}

#[derive(PartialEq, Clone, Debug)]
pub enum BroadcastStageType {
    Standard,
    FailEntryVerification,
    BroadcastFakeShreds,
    BroadcastDuplicates(BroadcastDuplicatesConfig),
}

impl BroadcastStageType {
    #[allow(clippy::too_many_arguments)]
    pub fn new_broadcast_stage(
        &self,
        sock: Vec<UdpSocket>,
        cluster_info: Arc<ClusterInfo>,
        receiver: Receiver<WorkingBankEntry>,
        retransmit_slots_receiver: RetransmitSlotsReceiver,
        exit_sender: &Arc<AtomicBool>,
        blockstore: &Arc<Blockstore>,
        bank_forks: &Arc<RwLock<BankForks>>,
        shred_version: u16,
    ) -> BroadcastStage {
        match self {
            BroadcastStageType::Standard => BroadcastStage::new(
                sock,
                cluster_info,
                receiver,
                retransmit_slots_receiver,
                exit_sender,
                blockstore,
                bank_forks,
                StandardBroadcastRun::new(shred_version),
            ),

            BroadcastStageType::FailEntryVerification => BroadcastStage::new(
                sock,
                cluster_info,
                receiver,
                retransmit_slots_receiver,
                exit_sender,
                blockstore,
                bank_forks,
                FailEntryVerificationBroadcastRun::new(shred_version),
            ),

            BroadcastStageType::BroadcastFakeShreds => BroadcastStage::new(
                sock,
                cluster_info,
                receiver,
                retransmit_slots_receiver,
                exit_sender,
                blockstore,
                bank_forks,
                BroadcastFakeShredsRun::new(0, shred_version),
            ),

            BroadcastStageType::BroadcastDuplicates(config) => BroadcastStage::new(
                sock,
                cluster_info,
                receiver,
                retransmit_slots_receiver,
                exit_sender,
                blockstore,
                bank_forks,
                BroadcastDuplicatesRun::new(shred_version, config.clone()),
            ),
        }
    }
}

pub type TransmitShreds = (Option<Arc<HashMap<Pubkey, u64>>>, Arc<Vec<Shred>>);
trait BroadcastRun {
    fn run(
        &mut self,
        keypair: &Keypair,
        blockstore: &Arc<Blockstore>,
        receiver: &Receiver<WorkingBankEntry>,
        socket_sender: &Sender<(TransmitShreds, Option<BroadcastShredBatchInfo>)>,
        blockstore_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
    ) -> Result<()>;
    fn transmit(
        &mut self,
        receiver: &Arc<Mutex<TransmitReceiver>>,
        cluster_info: &ClusterInfo,
        sock: &UdpSocket,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> Result<()>;
    fn record(
        &mut self,
        receiver: &Arc<Mutex<RecordReceiver>>,
        blockstore: &Arc<Blockstore>,
    ) -> Result<()>;
}

// Implement a destructor for the BroadcastStage thread to signal it exited
// even on panics
struct Finalizer {
    exit_sender: Arc<AtomicBool>,
}

impl Finalizer {
    fn new(exit_sender: Arc<AtomicBool>) -> Self {
        Finalizer { exit_sender }
    }
}
// Implement a destructor for Finalizer.
impl Drop for Finalizer {
    fn drop(&mut self) {
        self.exit_sender.clone().store(true, Ordering::Relaxed);
    }
}

pub struct BroadcastStage {
    thread_hdls: Vec<JoinHandle<BroadcastStageReturnType>>,
}

impl BroadcastStage {
    #[allow(clippy::too_many_arguments)]
    fn run(
        cluster_info: Arc<ClusterInfo>,
        blockstore: &Arc<Blockstore>,
        receiver: &Receiver<WorkingBankEntry>,
        socket_sender: &Sender<(TransmitShreds, Option<BroadcastShredBatchInfo>)>,
        blockstore_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
        mut broadcast_stage_run: impl BroadcastRun,
    ) -> BroadcastStageReturnType {
        loop {
            let res = broadcast_stage_run.run(
                &cluster_info.keypair(),
                blockstore,
                receiver,
                socket_sender,
                blockstore_sender,
            );
            let res = Self::handle_error(res, "run");
            if let Some(res) = res {
                return res;
            }
        }
    }
    fn handle_error(r: Result<()>, name: &str) -> Option<BroadcastStageReturnType> {
        if let Err(e) = r {
            match e {
                Error::RecvTimeout(RecvTimeoutError::Disconnected)
                | Error::Send
                | Error::Recv(RecvError)
                | Error::CrossbeamRecvTimeout(CrossbeamRecvTimeoutError::Disconnected) => {
                    return Some(BroadcastStageReturnType::ChannelDisconnected);
                }
                Error::RecvTimeout(RecvTimeoutError::Timeout)
                | Error::CrossbeamRecvTimeout(CrossbeamRecvTimeoutError::Timeout) => (),
                Error::ClusterInfo(ClusterInfoError::NoPeers) => (), // TODO: Why are the unit-tests throwing hundreds of these?
                _ => {
                    inc_new_counter_error!("streamer-broadcaster-error", 1, 1);
                    error!("{} broadcaster error: {:?}", name, e);
                }
            }
        }
        None
    }

    /// Service to broadcast messages from the leader to layer 1 nodes.
    /// See `cluster_info` for network layer definitions.
    /// # Arguments
    /// * `sock` - Socket to send from.
    /// * `exit` - Boolean to signal system exit.
    /// * `cluster_info` - ClusterInfo structure
    /// * `window` - Cache of Shreds that we have broadcast
    /// * `receiver` - Receive channel for Shreds to be retransmitted to all the layer 1 nodes.
    /// * `exit_sender` - Set to true when this service exits, allows rest of Tpu to exit cleanly.
    /// Otherwise, when a Tpu closes, it only closes the stages that come after it. The stages
    /// that come before could be blocked on a receive, and never notice that they need to
    /// exit. Now, if any stage of the Tpu closes, it will lead to closing the WriteStage (b/c
    /// WriteStage is the last stage in the pipeline), which will then close Broadcast service,
    /// which will then close FetchStage in the Tpu, and then the rest of the Tpu,
    /// completing the cycle.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::same_item_push)]
    fn new(
        socks: Vec<UdpSocket>,
        cluster_info: Arc<ClusterInfo>,
        receiver: Receiver<WorkingBankEntry>,
        retransmit_slots_receiver: RetransmitSlotsReceiver,
        exit_sender: &Arc<AtomicBool>,
        blockstore: &Arc<Blockstore>,
        bank_forks: &Arc<RwLock<BankForks>>,
        broadcast_stage_run: impl BroadcastRun + Send + 'static + Clone,
    ) -> Self {
        let btree = blockstore.clone();
        let exit = exit_sender.clone();
        let (socket_sender, socket_receiver) = channel();
        let (blockstore_sender, blockstore_receiver) = channel();
        let bs_run = broadcast_stage_run.clone();

        let socket_sender_ = socket_sender.clone();
        let thread_hdl = {
            let cluster_info = cluster_info.clone();
            Builder::new()
                .name("solana-broadcaster".to_string())
                .spawn(move || {
                    let _finalizer = Finalizer::new(exit);
                    Self::run(
                        cluster_info,
                        &btree,
                        &receiver,
                        &socket_sender_,
                        &blockstore_sender,
                        bs_run,
                    )
                })
                .unwrap()
        };
        let mut thread_hdls = vec![thread_hdl];
        let socket_receiver = Arc::new(Mutex::new(socket_receiver));
        for sock in socks.into_iter() {
            let socket_receiver = socket_receiver.clone();
            let mut bs_transmit = broadcast_stage_run.clone();
            let cluster_info = cluster_info.clone();
            let bank_forks = bank_forks.clone();
            let t = Builder::new()
                .name("solana-broadcaster-transmit".to_string())
                .spawn(move || loop {
                    let res =
                        bs_transmit.transmit(&socket_receiver, &cluster_info, &sock, &bank_forks);
                    let res = Self::handle_error(res, "solana-broadcaster-transmit");
                    if let Some(res) = res {
                        return res;
                    }
                })
                .unwrap();
            thread_hdls.push(t);
        }
        let blockstore_receiver = Arc::new(Mutex::new(blockstore_receiver));
        for _ in 0..NUM_INSERT_THREADS {
            let blockstore_receiver = blockstore_receiver.clone();
            let mut bs_record = broadcast_stage_run.clone();
            let btree = blockstore.clone();
            let t = Builder::new()
                .name("solana-broadcaster-record".to_string())
                .spawn(move || loop {
                    let res = bs_record.record(&blockstore_receiver, &btree);
                    let res = Self::handle_error(res, "solana-broadcaster-record");
                    if let Some(res) = res {
                        return res;
                    }
                })
                .unwrap();
            thread_hdls.push(t);
        }

        let blockstore = blockstore.clone();
        let retransmit_thread = Builder::new()
            .name("solana-broadcaster-retransmit".to_string())
            .spawn(move || loop {
                if let Some(res) = Self::handle_error(
                    Self::check_retransmit_signals(
                        &blockstore,
                        &retransmit_slots_receiver,
                        &socket_sender,
                    ),
                    "solana-broadcaster-retransmit-check_retransmit_signals",
                ) {
                    return res;
                }
            })
            .unwrap();

        thread_hdls.push(retransmit_thread);
        Self { thread_hdls }
    }

    fn check_retransmit_signals(
        blockstore: &Blockstore,
        retransmit_slots_receiver: &RetransmitSlotsReceiver,
        socket_sender: &Sender<(TransmitShreds, Option<BroadcastShredBatchInfo>)>,
    ) -> Result<()> {
        let timer = Duration::from_millis(100);

        // Check for a retransmit signal
        let mut retransmit_slots = retransmit_slots_receiver.recv_timeout(timer)?;
        while let Ok(new_retransmit_slots) = retransmit_slots_receiver.try_recv() {
            retransmit_slots.extend(new_retransmit_slots);
        }

        for (_, bank) in retransmit_slots.iter() {
            let bank_epoch = bank.get_leader_schedule_epoch(bank.slot());
            let stakes = bank.epoch_staked_nodes(bank_epoch);
            let stakes = stakes.map(Arc::new);
            let data_shreds = Arc::new(
                blockstore
                    .get_data_shreds_for_slot(bank.slot(), 0)
                    .expect("My own shreds must be reconstructable"),
            );

            if !data_shreds.is_empty() {
                socket_sender.send(((stakes.clone(), data_shreds), None))?;
            }

            let coding_shreds = Arc::new(
                blockstore
                    .get_coding_shreds_for_slot(bank.slot(), 0)
                    .expect("My own shreds must be reconstructable"),
            );

            if !coding_shreds.is_empty() {
                socket_sender.send(((stakes.clone(), coding_shreds), None))?;
            }
        }

        Ok(())
    }

    pub fn join(self) -> thread::Result<BroadcastStageReturnType> {
        for thread_hdl in self.thread_hdls.into_iter() {
            let _ = thread_hdl.join();
        }
        Ok(BroadcastStageReturnType::ChannelDisconnected)
    }
}

fn update_peer_stats(
    num_live_peers: i64,
    broadcast_len: i64,
    last_datapoint_submit: &Arc<AtomicU64>,
) {
    let now = timestamp();
    let last = last_datapoint_submit.load(Ordering::Relaxed);
    #[allow(deprecated)]
    if now.saturating_sub(last) > 1000
        && last_datapoint_submit.compare_and_swap(last, now, Ordering::Relaxed) == last
    {
        datapoint_info!(
            "cluster_info-num_nodes",
            ("live_count", num_live_peers, i64),
            ("broadcast_count", broadcast_len, i64)
        );
    }
}

/// broadcast messages from the leader to layer 1 nodes
/// # Remarks
pub fn broadcast_shreds(
    s: &UdpSocket,
    shreds: &[Shred],
    cluster_nodes: &ClusterNodes<BroadcastStage>,
    last_datapoint_submit: &Arc<AtomicU64>,
    transmit_stats: &mut TransmitShredsStats,
    self_pubkey: Pubkey,
    bank_forks: &Arc<RwLock<BankForks>>,
) -> Result<()> {
    let broadcast_len = cluster_nodes.num_peers();
    if broadcast_len == 0 {
        update_peer_stats(1, 1, last_datapoint_submit);
        return Ok(());
    }
    let mut shred_select = Measure::start("shred_select");
    let root_bank = bank_forks.read().unwrap().root_bank();
    let packets: Vec<_> = shreds
        .iter()
        .filter_map(|shred| {
            let seed = shred.seed(Some(self_pubkey), &root_bank);
            let node = cluster_nodes.get_broadcast_peer(seed)?;
            Some((&shred.payload, &node.tvu))
        })
        .collect();
    shred_select.stop();
    transmit_stats.shred_select += shred_select.as_us();

    let mut send_mmsg_time = Measure::start("send_mmsg");
    if let Err(err) = batch_send(s, &packets[..]) {
        error!("broadcast_shreds batch_send error: {:?}", err);
    }
    send_mmsg_time.stop();
    transmit_stats.send_mmsg_elapsed += send_mmsg_time.as_us();

    let num_live_peers = cluster_nodes.num_peers_live(timestamp()) as i64;
    update_peer_stats(
        num_live_peers,
        broadcast_len as i64 + 1,
        last_datapoint_submit,
    );
    Ok(())
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crossbeam_channel::unbounded;
    use solana_gossip::cluster_info::{ClusterInfo, Node};
    use solana_ledger::{
        blockstore::{make_slot_entries, Blockstore},
        entry::create_ticks,
        genesis_utils::{create_genesis_config, GenesisConfigInfo},
        get_tmp_ledger_path,
        shred::{max_ticks_per_n_shreds, ProcessShredsStats, Shredder},
    };
    use solana_runtime::bank::Bank;
    use solana_sdk::{
        hash::Hash,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
    };
    use std::{
        path::Path, sync::atomic::AtomicBool, sync::mpsc::channel, sync::Arc, thread::sleep,
    };

    #[allow(clippy::implicit_hasher)]
    pub fn make_transmit_shreds(
        slot: Slot,
        num: u64,
        stakes: Option<Arc<HashMap<Pubkey, u64>>>,
    ) -> (
        Vec<Shred>,
        Vec<Shred>,
        Vec<TransmitShreds>,
        Vec<TransmitShreds>,
    ) {
        let num_entries = max_ticks_per_n_shreds(num, None);
        let (data_shreds, _) = make_slot_entries(slot, 0, num_entries);
        let keypair = Keypair::new();
        let coding_shreds = Shredder::data_shreds_to_coding_shreds(
            &keypair,
            &data_shreds[0..],
            true, // is_last_in_slot
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        (
            data_shreds.clone(),
            coding_shreds.clone(),
            data_shreds
                .into_iter()
                .map(|s| (stakes.clone(), Arc::new(vec![s])))
                .collect(),
            coding_shreds
                .into_iter()
                .map(|s| (stakes.clone(), Arc::new(vec![s])))
                .collect(),
        )
    }

    fn check_all_shreds_received(
        transmit_receiver: &TransmitReceiver,
        mut data_index: u64,
        mut coding_index: u64,
        num_expected_data_shreds: u64,
        num_expected_coding_shreds: u64,
    ) {
        while let Ok((new_retransmit_slots, _)) = transmit_receiver.try_recv() {
            if new_retransmit_slots.1[0].is_data() {
                for data_shred in new_retransmit_slots.1.iter() {
                    assert_eq!(data_shred.index() as u64, data_index);
                    data_index += 1;
                }
            } else {
                assert_eq!(new_retransmit_slots.1[0].index() as u64, coding_index);
                for coding_shred in new_retransmit_slots.1.iter() {
                    assert_eq!(coding_shred.index() as u64, coding_index);
                    coding_index += 1;
                }
            }
        }

        assert_eq!(num_expected_data_shreds, data_index);
        assert_eq!(num_expected_coding_shreds, coding_index);
    }

    #[test]
    fn test_duplicate_retransmit_signal() {
        // Setup
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(Blockstore::open(&ledger_path).unwrap());
        let (transmit_sender, transmit_receiver) = channel();
        let (retransmit_slots_sender, retransmit_slots_receiver) = unbounded();
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(100_000);
        let bank0 = Arc::new(Bank::new(&genesis_config));

        // Make some shreds
        let updated_slot = 0;
        let (all_data_shreds, all_coding_shreds, _, _all_coding_transmit_shreds) =
            make_transmit_shreds(updated_slot, 10, None);
        let num_data_shreds = all_data_shreds.len();
        let num_coding_shreds = all_coding_shreds.len();
        assert!(num_data_shreds >= 10);

        // Insert all the shreds
        blockstore
            .insert_shreds(all_data_shreds, None, true)
            .unwrap();
        blockstore
            .insert_shreds(all_coding_shreds, None, true)
            .unwrap();

        // Insert duplicate retransmit signal, blocks should
        // only be retransmitted once
        retransmit_slots_sender
            .send(vec![(updated_slot, bank0.clone())].into_iter().collect())
            .unwrap();
        retransmit_slots_sender
            .send(vec![(updated_slot, bank0)].into_iter().collect())
            .unwrap();
        BroadcastStage::check_retransmit_signals(
            &blockstore,
            &retransmit_slots_receiver,
            &transmit_sender,
        )
        .unwrap();
        // Check all the data shreds were received only once
        check_all_shreds_received(
            &transmit_receiver,
            0,
            0,
            num_data_shreds as u64,
            num_coding_shreds as u64,
        );
    }

    struct MockBroadcastStage {
        blockstore: Arc<Blockstore>,
        broadcast_service: BroadcastStage,
        bank: Arc<Bank>,
    }

    fn setup_dummy_broadcast_service(
        leader_pubkey: &Pubkey,
        ledger_path: &Path,
        entry_receiver: Receiver<WorkingBankEntry>,
        retransmit_slots_receiver: RetransmitSlotsReceiver,
    ) -> MockBroadcastStage {
        // Make the database ledger
        let blockstore = Arc::new(Blockstore::open(ledger_path).unwrap());

        // Make the leader node and scheduler
        let leader_info = Node::new_localhost_with_pubkey(leader_pubkey);

        // Make a node to broadcast to
        let buddy_keypair = Keypair::new();
        let broadcast_buddy = Node::new_localhost_with_pubkey(&buddy_keypair.pubkey());

        // Fill the cluster_info with the buddy's info
        let cluster_info = ClusterInfo::new_with_invalid_keypair(leader_info.info.clone());
        cluster_info.insert_info(broadcast_buddy.info);
        let cluster_info = Arc::new(cluster_info);

        let exit_sender = Arc::new(AtomicBool::new(false));

        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank = bank_forks.read().unwrap().root_bank();

        // Start up the broadcast stage
        let broadcast_service = BroadcastStage::new(
            leader_info.sockets.broadcast,
            cluster_info,
            entry_receiver,
            retransmit_slots_receiver,
            &exit_sender,
            &blockstore,
            &bank_forks,
            StandardBroadcastRun::new(0),
        );

        MockBroadcastStage {
            blockstore,
            broadcast_service,
            bank,
        }
    }

    #[test]
    fn test_broadcast_ledger() {
        solana_logger::setup();
        let ledger_path = get_tmp_ledger_path!();

        {
            // Create the leader scheduler
            let leader_keypair = Keypair::new();

            let (entry_sender, entry_receiver) = channel();
            let (retransmit_slots_sender, retransmit_slots_receiver) = unbounded();
            let broadcast_service = setup_dummy_broadcast_service(
                &leader_keypair.pubkey(),
                &ledger_path,
                entry_receiver,
                retransmit_slots_receiver,
            );
            let start_tick_height;
            let max_tick_height;
            let ticks_per_slot;
            let slot;
            {
                let bank = broadcast_service.bank.clone();
                start_tick_height = bank.tick_height();
                max_tick_height = bank.max_tick_height();
                ticks_per_slot = bank.ticks_per_slot();
                slot = bank.slot();
                let ticks = create_ticks(max_tick_height - start_tick_height, 0, Hash::default());
                for (i, tick) in ticks.into_iter().enumerate() {
                    entry_sender
                        .send((bank.clone(), (tick, i as u64 + 1)))
                        .expect("Expect successful send to broadcast service");
                }
            }

            trace!(
                "[broadcast_ledger] max_tick_height: {}, start_tick_height: {}, ticks_per_slot: {}",
                max_tick_height,
                start_tick_height,
                ticks_per_slot,
            );

            let mut entries = vec![];
            for _ in 0..10 {
                entries = broadcast_service
                    .blockstore
                    .get_slot_entries(slot, 0)
                    .expect("Expect entries to be present");
                if entries.len() >= max_tick_height as usize {
                    break;
                }
                sleep(Duration::from_millis(1000));
            }
            assert_eq!(entries.len(), max_tick_height as usize);

            drop(entry_sender);
            drop(retransmit_slots_sender);
            broadcast_service
                .broadcast_service
                .join()
                .expect("Expect successful join of broadcast service");
        }

        Blockstore::destroy(&ledger_path).expect("Expected successful database destruction");
    }
}
