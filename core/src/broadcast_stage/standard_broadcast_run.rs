#![allow(clippy::rc_buffer)]

use super::{
    broadcast_utils::{self, ReceiveResults},
    *,
};
use crate::{broadcast_stage::broadcast_utils::UnfinishedSlotInfo, cluster_nodes::ClusterNodes};
use solana_entry::entry::Entry;
use solana_ledger::shred::{
    ProcessShredsStats, Shred, Shredder, MAX_DATA_SHREDS_PER_FEC_BLOCK, SHRED_TICK_REFERENCE_MASK,
};
use solana_sdk::{pubkey::Pubkey, signature::Keypair, timing::duration_as_us};
use std::{collections::HashMap, sync::RwLock, time::Duration};

#[derive(Clone)]
pub struct StandardBroadcastRun {
    process_shreds_stats: ProcessShredsStats,
    transmit_shreds_stats: Arc<Mutex<SlotBroadcastStats<TransmitShredsStats>>>,
    insert_shreds_stats: Arc<Mutex<SlotBroadcastStats<InsertShredsStats>>>,
    unfinished_slot: Option<UnfinishedSlotInfo>,
    current_slot_and_parent: Option<(u64, u64)>,
    slot_broadcast_start: Option<Instant>,
    shred_version: u16,
    last_datapoint_submit: Arc<AtomicU64>,
    num_batches: usize,
    cluster_nodes: Arc<RwLock<ClusterNodes<BroadcastStage>>>,
    last_peer_update: Arc<AtomicU64>,
}

impl StandardBroadcastRun {
    pub(super) fn new(shred_version: u16) -> Self {
        Self {
            process_shreds_stats: ProcessShredsStats::default(),
            transmit_shreds_stats: Arc::default(),
            insert_shreds_stats: Arc::default(),
            unfinished_slot: None,
            current_slot_and_parent: None,
            slot_broadcast_start: None,
            shred_version,
            last_datapoint_submit: Arc::default(),
            num_batches: 0,
            cluster_nodes: Arc::default(),
            last_peer_update: Arc::default(),
        }
    }

    // If the current slot has changed, generates an empty shred indicating
    // last shred in the previous slot, along with coding shreds for the data
    // shreds buffered.
    fn finish_prev_slot(
        &mut self,
        keypair: &Keypair,
        max_ticks_in_slot: u8,
        stats: &mut ProcessShredsStats,
    ) -> Vec<Shred> {
        let (current_slot, _) = self.current_slot_and_parent.unwrap();
        match self.unfinished_slot {
            None => Vec::default(),
            Some(ref state) if state.slot == current_slot => Vec::default(),
            Some(ref mut state) => {
                let parent_offset = state.slot - state.parent;
                let reference_tick = max_ticks_in_slot & SHRED_TICK_REFERENCE_MASK;
                let fec_set_index =
                    Shredder::fec_set_index(state.next_shred_index, state.fec_set_offset);
                let mut shred = Shred::new_from_data(
                    state.slot,
                    state.next_shred_index,
                    parent_offset as u16,
                    None, // data
                    true, // is_last_in_fec_set
                    true, // is_last_in_slot
                    reference_tick,
                    self.shred_version,
                    fec_set_index.unwrap(),
                );
                Shredder::sign_shred(keypair, &mut shred);
                state.data_shreds_buffer.push(shred.clone());
                let mut shreds = make_coding_shreds(
                    keypair,
                    &mut self.unfinished_slot,
                    true, // is_last_in_slot
                    stats,
                );
                shreds.insert(0, shred);
                self.report_and_reset_stats();
                self.unfinished_slot = None;
                shreds
            }
        }
    }

    fn entries_to_data_shreds(
        &mut self,
        keypair: &Keypair,
        entries: &[Entry],
        blockstore: &Blockstore,
        reference_tick: u8,
        is_slot_end: bool,
        process_stats: &mut ProcessShredsStats,
    ) -> Vec<Shred> {
        let (slot, parent_slot) = self.current_slot_and_parent.unwrap();
        let (next_shred_index, fec_set_offset) = match &self.unfinished_slot {
            Some(state) => (state.next_shred_index, state.fec_set_offset),
            None => match blockstore.meta(slot).unwrap() {
                Some(slot_meta) => {
                    let shreds_consumed = slot_meta.consumed as u32;
                    (shreds_consumed, shreds_consumed)
                }
                None => (0, 0),
            },
        };
        let (data_shreds, next_shred_index) =
            Shredder::new(slot, parent_slot, reference_tick, self.shred_version)
                .unwrap()
                .entries_to_data_shreds(
                    keypair,
                    entries,
                    is_slot_end,
                    next_shred_index,
                    fec_set_offset,
                    process_stats,
                );
        let mut data_shreds_buffer = match &mut self.unfinished_slot {
            Some(state) => {
                assert_eq!(state.slot, slot);
                std::mem::take(&mut state.data_shreds_buffer)
            }
            None => Vec::default(),
        };
        data_shreds_buffer.extend(data_shreds.clone());
        self.unfinished_slot = Some(UnfinishedSlotInfo {
            next_shred_index,
            slot,
            parent: parent_slot,
            data_shreds_buffer,
            fec_set_offset,
        });
        data_shreds
    }

    #[cfg(test)]
    fn test_process_receive_results(
        &mut self,
        keypair: &Keypair,
        cluster_info: &ClusterInfo,
        sock: &UdpSocket,
        blockstore: &Arc<Blockstore>,
        receive_results: ReceiveResults,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> Result<()> {
        let (bsend, brecv) = channel();
        let (ssend, srecv) = channel();
        self.process_receive_results(keypair, blockstore, &ssend, &bsend, receive_results)?;
        let srecv = Arc::new(Mutex::new(srecv));
        let brecv = Arc::new(Mutex::new(brecv));

        //data
        let _ = self.transmit(&srecv, cluster_info, sock, bank_forks);
        let _ = self.record(&brecv, blockstore);
        //coding
        let _ = self.transmit(&srecv, cluster_info, sock, bank_forks);
        let _ = self.record(&brecv, blockstore);
        Ok(())
    }

    fn process_receive_results(
        &mut self,
        keypair: &Keypair,
        blockstore: &Arc<Blockstore>,
        socket_sender: &Sender<(TransmitShreds, Option<BroadcastShredBatchInfo>)>,
        blockstore_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
        receive_results: ReceiveResults,
    ) -> Result<()> {
        let mut receive_elapsed = receive_results.time_elapsed;
        let num_entries = receive_results.entries.len();
        let bank = receive_results.bank.clone();
        let last_tick_height = receive_results.last_tick_height;
        inc_new_counter_info!("broadcast_service-entries_received", num_entries);
        let old_broadcast_start = self.slot_broadcast_start;
        let old_num_batches = self.num_batches;
        if self.current_slot_and_parent.is_none()
            || bank.slot() != self.current_slot_and_parent.unwrap().0
        {
            self.slot_broadcast_start = Some(Instant::now());
            self.num_batches = 0;
            let slot = bank.slot();
            let parent_slot = bank.parent_slot();

            self.current_slot_and_parent = Some((slot, parent_slot));
            receive_elapsed = Duration::new(0, 0);
        }

        let mut process_stats = ProcessShredsStats::default();

        let mut to_shreds_time = Measure::start("broadcast_to_shreds");

        // 1) Check if slot was interrupted
        let prev_slot_shreds =
            self.finish_prev_slot(keypair, bank.ticks_per_slot() as u8, &mut process_stats);

        // 2) Convert entries to shreds and coding shreds
        let is_last_in_slot = last_tick_height == bank.max_tick_height();
        let reference_tick = bank.tick_height() % bank.ticks_per_slot();
        let data_shreds = self.entries_to_data_shreds(
            keypair,
            &receive_results.entries,
            blockstore,
            reference_tick as u8,
            is_last_in_slot,
            &mut process_stats,
        );
        // Insert the first shred so blockstore stores that the leader started this block
        // This must be done before the blocks are sent out over the wire.
        if !data_shreds.is_empty() && data_shreds[0].index() == 0 {
            let first = vec![data_shreds[0].clone()];
            blockstore
                .insert_shreds(first, None, true)
                .expect("Failed to insert shreds in blockstore");
        }
        to_shreds_time.stop();

        let mut get_leader_schedule_time = Measure::start("broadcast_get_leader_schedule");
        let bank_epoch = bank.get_leader_schedule_epoch(bank.slot());
        let stakes = bank.epoch_staked_nodes(bank_epoch).map(Arc::new);

        // Broadcast the last shred of the interrupted slot if necessary
        if !prev_slot_shreds.is_empty() {
            let batch_info = Some(BroadcastShredBatchInfo {
                slot: prev_slot_shreds[0].slot(),
                num_expected_batches: Some(old_num_batches + 1),
                slot_start_ts: old_broadcast_start.expect(
                    "Old broadcast start time for previous slot must exist if the previous slot
                 was interrupted",
                ),
            });
            let shreds = Arc::new(prev_slot_shreds);
            socket_sender.send(((stakes.clone(), shreds.clone()), batch_info.clone()))?;
            blockstore_sender.send((shreds, batch_info))?;
        }

        // Increment by two batches, one for the data batch, one for the coding batch.
        self.num_batches += 2;
        let num_expected_batches = {
            if is_last_in_slot {
                Some(self.num_batches)
            } else {
                None
            }
        };
        let batch_info = Some(BroadcastShredBatchInfo {
            slot: bank.slot(),
            num_expected_batches,
            slot_start_ts: self
                .slot_broadcast_start
                .expect("Start timestamp must exist for a slot if we're broadcasting the slot"),
        });
        get_leader_schedule_time.stop();

        let mut coding_send_time = Measure::start("broadcast_coding_send");

        // Send data shreds
        let data_shreds = Arc::new(data_shreds);
        socket_sender.send(((stakes.clone(), data_shreds.clone()), batch_info.clone()))?;
        blockstore_sender.send((data_shreds, batch_info.clone()))?;

        // Create and send coding shreds
        let coding_shreds = make_coding_shreds(
            keypair,
            &mut self.unfinished_slot,
            is_last_in_slot,
            &mut process_stats,
        );
        let coding_shreds = Arc::new(coding_shreds);
        socket_sender.send(((stakes, coding_shreds.clone()), batch_info.clone()))?;
        blockstore_sender.send((coding_shreds, batch_info))?;

        coding_send_time.stop();

        process_stats.shredding_elapsed = to_shreds_time.as_us();
        process_stats.get_leader_schedule_elapsed = get_leader_schedule_time.as_us();
        process_stats.receive_elapsed = duration_as_us(&receive_elapsed);
        process_stats.coding_send_elapsed = coding_send_time.as_us();

        self.process_shreds_stats.update(&process_stats);

        if last_tick_height == bank.max_tick_height() {
            self.report_and_reset_stats();
            self.unfinished_slot = None;
        }

        Ok(())
    }

    fn insert(
        &mut self,
        blockstore: &Arc<Blockstore>,
        shreds: Arc<Vec<Shred>>,
        broadcast_shred_batch_info: Option<BroadcastShredBatchInfo>,
    ) {
        // Insert shreds into blockstore
        let insert_shreds_start = Instant::now();
        // The first shred is inserted synchronously
        let data_shreds = if !shreds.is_empty() && shreds[0].index() == 0 {
            shreds[1..].to_vec()
        } else {
            shreds.to_vec()
        };
        blockstore
            .insert_shreds(data_shreds, None, true)
            .expect("Failed to insert shreds in blockstore");
        let insert_shreds_elapsed = insert_shreds_start.elapsed();
        let new_insert_shreds_stats = InsertShredsStats {
            insert_shreds_elapsed: duration_as_us(&insert_shreds_elapsed),
            num_shreds: shreds.len(),
        };
        self.update_insertion_metrics(&new_insert_shreds_stats, &broadcast_shred_batch_info);
    }

    fn update_insertion_metrics(
        &mut self,
        new_insertion_shreds_stats: &InsertShredsStats,
        broadcast_shred_batch_info: &Option<BroadcastShredBatchInfo>,
    ) {
        let mut insert_shreds_stats = self.insert_shreds_stats.lock().unwrap();
        insert_shreds_stats.update(new_insertion_shreds_stats, broadcast_shred_batch_info);
    }

    fn broadcast(
        &mut self,
        sock: &UdpSocket,
        cluster_info: &ClusterInfo,
        stakes: Option<&HashMap<Pubkey, u64>>,
        shreds: Arc<Vec<Shred>>,
        broadcast_shred_batch_info: Option<BroadcastShredBatchInfo>,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> Result<()> {
        const BROADCAST_PEER_UPDATE_INTERVAL_MS: u64 = 1000;
        trace!("Broadcasting {:?} shreds", shreds.len());
        // Get the list of peers to broadcast to
        let mut get_peers_time = Measure::start("broadcast::get_peers");
        let now = timestamp();
        let last = self.last_peer_update.load(Ordering::Relaxed);
        #[allow(deprecated)]
        if now - last > BROADCAST_PEER_UPDATE_INTERVAL_MS
            && self
                .last_peer_update
                .compare_and_swap(now, last, Ordering::Relaxed)
                == last
        {
            *self.cluster_nodes.write().unwrap() = ClusterNodes::<BroadcastStage>::new(
                cluster_info,
                stakes.unwrap_or(&HashMap::default()),
            );
        }
        get_peers_time.stop();
        let cluster_nodes = self.cluster_nodes.read().unwrap();

        let mut transmit_stats = TransmitShredsStats::default();
        // Broadcast the shreds
        let mut transmit_time = Measure::start("broadcast_shreds");

        broadcast_shreds(
            sock,
            &shreds,
            &cluster_nodes,
            &self.last_datapoint_submit,
            &mut transmit_stats,
            cluster_info.id(),
            bank_forks,
        )?;
        drop(cluster_nodes);
        transmit_time.stop();

        transmit_stats.transmit_elapsed = transmit_time.as_us();
        transmit_stats.get_peers_elapsed = get_peers_time.as_us();
        transmit_stats.num_shreds = shreds.len();

        // Process metrics
        self.update_transmit_metrics(&transmit_stats, &broadcast_shred_batch_info);
        Ok(())
    }

    fn update_transmit_metrics(
        &mut self,
        new_transmit_shreds_stats: &TransmitShredsStats,
        broadcast_shred_batch_info: &Option<BroadcastShredBatchInfo>,
    ) {
        let mut transmit_shreds_stats = self.transmit_shreds_stats.lock().unwrap();
        transmit_shreds_stats.update(new_transmit_shreds_stats, broadcast_shred_batch_info);
    }

    fn report_and_reset_stats(&mut self) {
        let stats = &self.process_shreds_stats;
        let unfinished_slot = self.unfinished_slot.as_ref().unwrap();
        datapoint_info!(
            "broadcast-process-shreds-stats",
            ("slot", unfinished_slot.slot as i64, i64),
            ("shredding_time", stats.shredding_elapsed, i64),
            ("receive_time", stats.receive_elapsed, i64),
            (
                "num_data_shreds",
                unfinished_slot.next_shred_index as i64,
                i64
            ),
            (
                "slot_broadcast_time",
                self.slot_broadcast_start.unwrap().elapsed().as_micros() as i64,
                i64
            ),
            (
                "get_leader_schedule_time",
                stats.get_leader_schedule_elapsed,
                i64
            ),
            ("serialize_shreds_time", stats.serialize_elapsed, i64),
            ("gen_data_time", stats.gen_data_elapsed, i64),
            ("gen_coding_time", stats.gen_coding_elapsed, i64),
            ("sign_coding_time", stats.sign_coding_elapsed, i64),
            ("coding_send_time", stats.coding_send_elapsed, i64),
        );
        self.process_shreds_stats.reset();
    }
}

// Consumes data_shreds_buffer returning corresponding coding shreds.
fn make_coding_shreds(
    keypair: &Keypair,
    unfinished_slot: &mut Option<UnfinishedSlotInfo>,
    is_slot_end: bool,
    stats: &mut ProcessShredsStats,
) -> Vec<Shred> {
    let data_shreds = match unfinished_slot {
        None => Vec::default(),
        Some(unfinished_slot) => {
            let size = unfinished_slot.data_shreds_buffer.len();
            // Consume a multiple of 32, unless this is the slot end.
            let offset = if is_slot_end {
                0
            } else {
                size % MAX_DATA_SHREDS_PER_FEC_BLOCK as usize
            };
            unfinished_slot
                .data_shreds_buffer
                .drain(0..size - offset)
                .collect()
        }
    };
    Shredder::data_shreds_to_coding_shreds(keypair, &data_shreds, is_slot_end, stats).unwrap()
}

impl BroadcastRun for StandardBroadcastRun {
    fn run(
        &mut self,
        keypair: &Keypair,
        blockstore: &Arc<Blockstore>,
        receiver: &Receiver<WorkingBankEntry>,
        socket_sender: &Sender<(TransmitShreds, Option<BroadcastShredBatchInfo>)>,
        blockstore_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
    ) -> Result<()> {
        let receive_results = broadcast_utils::recv_slot_entries(receiver)?;
        // TODO: Confirm that last chunk of coding shreds
        // will not be lost or delayed for too long.
        self.process_receive_results(
            keypair,
            blockstore,
            socket_sender,
            blockstore_sender,
            receive_results,
        )
    }
    fn transmit(
        &mut self,
        receiver: &Arc<Mutex<TransmitReceiver>>,
        cluster_info: &ClusterInfo,
        sock: &UdpSocket,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> Result<()> {
        let ((stakes, shreds), slot_start_ts) = receiver.lock().unwrap().recv()?;
        self.broadcast(
            sock,
            cluster_info,
            stakes.as_deref(),
            shreds,
            slot_start_ts,
            bank_forks,
        )
    }
    fn record(
        &mut self,
        receiver: &Arc<Mutex<RecordReceiver>>,
        blockstore: &Arc<Blockstore>,
    ) -> Result<()> {
        let (shreds, slot_start_ts) = receiver.lock().unwrap().recv()?;
        self.insert(blockstore, shreds, slot_start_ts);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use solana_entry::entry::create_ticks;
    use solana_gossip::cluster_info::{ClusterInfo, Node};
    use solana_ledger::genesis_utils::create_genesis_config;
    use solana_ledger::{
        blockstore::Blockstore, get_tmp_ledger_path, shred::max_ticks_per_n_shreds,
    };
    use solana_runtime::bank::Bank;
    use solana_sdk::{
        genesis_config::GenesisConfig,
        signature::{Keypair, Signer},
    };
    use std::ops::Deref;
    use std::sync::Arc;
    use std::time::Duration;

    #[allow(clippy::type_complexity)]
    fn setup(
        num_shreds_per_slot: Slot,
    ) -> (
        Arc<Blockstore>,
        GenesisConfig,
        Arc<ClusterInfo>,
        Arc<Bank>,
        Arc<Keypair>,
        UdpSocket,
        Arc<RwLock<BankForks>>,
    ) {
        // Setup
        let ledger_path = get_tmp_ledger_path!();
        let blockstore = Arc::new(
            Blockstore::open(&ledger_path).expect("Expected to be able to open database ledger"),
        );
        let leader_keypair = Arc::new(Keypair::new());
        let leader_pubkey = leader_keypair.pubkey();
        let leader_info = Node::new_localhost_with_pubkey(&leader_pubkey);
        let cluster_info = Arc::new(ClusterInfo::new_with_invalid_keypair(leader_info.info));
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let mut genesis_config = create_genesis_config(10_000).genesis_config;
        genesis_config.ticks_per_slot = max_ticks_per_n_shreds(num_shreds_per_slot, None) + 1;

        let bank = Bank::new(&genesis_config);
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
        let bank0 = bank_forks.read().unwrap().root_bank();
        (
            blockstore,
            genesis_config,
            cluster_info,
            bank0,
            leader_keypair,
            socket,
            bank_forks,
        )
    }

    #[test]
    fn test_interrupted_slot_last_shred() {
        let keypair = Arc::new(Keypair::new());
        let mut run = StandardBroadcastRun::new(0);

        // Set up the slot to be interrupted
        let next_shred_index = 10;
        let slot = 1;
        let parent = 0;
        run.unfinished_slot = Some(UnfinishedSlotInfo {
            next_shred_index,
            slot,
            parent,
            data_shreds_buffer: Vec::default(),
            fec_set_offset: next_shred_index,
        });
        run.slot_broadcast_start = Some(Instant::now());

        // Set up a slot to interrupt the old slot
        run.current_slot_and_parent = Some((4, 2));

        // Slot 2 interrupted slot 1
        let shreds = run.finish_prev_slot(&keypair, 0, &mut ProcessShredsStats::default());
        let shred = shreds
            .get(0)
            .expect("Expected a shred that signals an interrupt");

        // Validate the shred
        assert_eq!(shred.parent(), parent);
        assert_eq!(shred.slot(), slot);
        assert_eq!(shred.index(), next_shred_index);
        assert!(shred.is_data());
        assert!(shred.verify(&keypair.pubkey()));
    }

    #[test]
    fn test_slot_interrupt() {
        // Setup
        let num_shreds_per_slot = 2;
        let (blockstore, genesis_config, cluster_info, bank0, leader_keypair, socket, bank_forks) =
            setup(num_shreds_per_slot);

        // Insert 1 less than the number of ticks needed to finish the slot
        let ticks0 = create_ticks(genesis_config.ticks_per_slot - 1, 0, genesis_config.hash());
        let receive_results = ReceiveResults {
            entries: ticks0.clone(),
            time_elapsed: Duration::new(3, 0),
            bank: bank0.clone(),
            last_tick_height: (ticks0.len() - 1) as u64,
        };

        // Step 1: Make an incomplete transmission for slot 0
        let mut standard_broadcast_run = StandardBroadcastRun::new(0);
        standard_broadcast_run
            .test_process_receive_results(
                &leader_keypair,
                &cluster_info,
                &socket,
                &blockstore,
                receive_results,
                &bank_forks,
            )
            .unwrap();
        let unfinished_slot = standard_broadcast_run.unfinished_slot.as_ref().unwrap();
        assert_eq!(unfinished_slot.next_shred_index as u64, num_shreds_per_slot);
        assert_eq!(unfinished_slot.slot, 0);
        assert_eq!(unfinished_slot.parent, 0);
        // Make sure the slot is not complete
        assert!(!blockstore.is_full(0));
        // Modify the stats, should reset later
        standard_broadcast_run.process_shreds_stats.receive_elapsed = 10;
        // Broadcast stats should exist, and 2 batches should have been sent,
        // one for data, one for coding
        assert_eq!(
            standard_broadcast_run
                .transmit_shreds_stats
                .lock()
                .unwrap()
                .get(unfinished_slot.slot)
                .unwrap()
                .num_batches(),
            2
        );
        assert_eq!(
            standard_broadcast_run
                .insert_shreds_stats
                .lock()
                .unwrap()
                .get(unfinished_slot.slot)
                .unwrap()
                .num_batches(),
            2
        );
        // Try to fetch ticks from blockstore, nothing should break
        assert_eq!(blockstore.get_slot_entries(0, 0).unwrap(), ticks0);
        assert_eq!(
            blockstore.get_slot_entries(0, num_shreds_per_slot).unwrap(),
            vec![],
        );

        // Step 2: Make a transmission for another bank that interrupts the transmission for
        // slot 0
        let bank2 = Arc::new(Bank::new_from_parent(&bank0, &leader_keypair.pubkey(), 2));
        let interrupted_slot = unfinished_slot.slot;
        // Interrupting the slot should cause the unfinished_slot and stats to reset
        let num_shreds = 1;
        assert!(num_shreds < num_shreds_per_slot);
        let ticks1 = create_ticks(
            max_ticks_per_n_shreds(num_shreds, None),
            0,
            genesis_config.hash(),
        );
        let receive_results = ReceiveResults {
            entries: ticks1.clone(),
            time_elapsed: Duration::new(2, 0),
            bank: bank2,
            last_tick_height: (ticks1.len() - 1) as u64,
        };
        standard_broadcast_run
            .test_process_receive_results(
                &leader_keypair,
                &cluster_info,
                &socket,
                &blockstore,
                receive_results,
                &bank_forks,
            )
            .unwrap();
        let unfinished_slot = standard_broadcast_run.unfinished_slot.as_ref().unwrap();

        // The shred index should have reset to 0, which makes it possible for the
        // index < the previous shred index for slot 0
        assert_eq!(unfinished_slot.next_shred_index as u64, num_shreds);
        assert_eq!(unfinished_slot.slot, 2);
        assert_eq!(unfinished_slot.parent, 0);

        // Check that the stats were reset as well
        assert_eq!(
            standard_broadcast_run.process_shreds_stats.receive_elapsed,
            0
        );

        // Broadcast stats for interrupted slot should be cleared
        assert!(standard_broadcast_run
            .transmit_shreds_stats
            .lock()
            .unwrap()
            .get(interrupted_slot)
            .is_none());
        assert!(standard_broadcast_run
            .insert_shreds_stats
            .lock()
            .unwrap()
            .get(interrupted_slot)
            .is_none());

        // Try to fetch the incomplete ticks from blockstore, should succeed
        assert_eq!(blockstore.get_slot_entries(0, 0).unwrap(), ticks0);
        assert_eq!(
            blockstore.get_slot_entries(0, num_shreds_per_slot).unwrap(),
            vec![],
        );
    }

    #[test]
    fn test_buffer_data_shreds() {
        let num_shreds_per_slot = 2;
        let (blockstore, genesis_config, _cluster_info, bank, leader_keypair, _socket, _bank_forks) =
            setup(num_shreds_per_slot);
        let (bsend, brecv) = channel();
        let (ssend, _srecv) = channel();
        let mut last_tick_height = 0;
        let mut standard_broadcast_run = StandardBroadcastRun::new(0);
        let mut process_ticks = |num_ticks| {
            let ticks = create_ticks(num_ticks, 0, genesis_config.hash());
            last_tick_height += (ticks.len() - 1) as u64;
            let receive_results = ReceiveResults {
                entries: ticks,
                time_elapsed: Duration::new(1, 0),
                bank: bank.clone(),
                last_tick_height,
            };
            standard_broadcast_run
                .process_receive_results(
                    &leader_keypair,
                    &blockstore,
                    &ssend,
                    &bsend,
                    receive_results,
                )
                .unwrap();
        };
        for i in 0..3 {
            process_ticks((i + 1) * 100);
        }
        let mut shreds = Vec::<Shred>::new();
        while let Ok((recv_shreds, _)) = brecv.recv_timeout(Duration::from_secs(1)) {
            shreds.extend(recv_shreds.deref().clone());
        }
        assert!(shreds.len() < 32, "shreds.len(): {}", shreds.len());
        assert!(shreds.iter().all(|shred| shred.is_data()));
        process_ticks(75);
        while let Ok((recv_shreds, _)) = brecv.recv_timeout(Duration::from_secs(1)) {
            shreds.extend(recv_shreds.deref().clone());
        }
        assert!(shreds.len() > 64, "shreds.len(): {}", shreds.len());
        let num_coding_shreds = shreds.iter().filter(|shred| shred.is_code()).count();
        assert_eq!(
            num_coding_shreds, 32,
            "num coding shreds: {}",
            num_coding_shreds
        );
    }

    #[test]
    fn test_slot_finish() {
        // Setup
        let num_shreds_per_slot = 2;
        let (blockstore, genesis_config, cluster_info, bank0, leader_keypair, socket, bank_forks) =
            setup(num_shreds_per_slot);

        // Insert complete slot of ticks needed to finish the slot
        let ticks = create_ticks(genesis_config.ticks_per_slot, 0, genesis_config.hash());
        let receive_results = ReceiveResults {
            entries: ticks.clone(),
            time_elapsed: Duration::new(3, 0),
            bank: bank0,
            last_tick_height: ticks.len() as u64,
        };

        let mut standard_broadcast_run = StandardBroadcastRun::new(0);
        standard_broadcast_run
            .test_process_receive_results(
                &leader_keypair,
                &cluster_info,
                &socket,
                &blockstore,
                receive_results,
                &bank_forks,
            )
            .unwrap();
        assert!(standard_broadcast_run.unfinished_slot.is_none())
    }
}
