//! The `repair_service` module implements the tools necessary to generate a thread which
//! regularly finds missing shreds in the ledger and sends repair requests for those shreds
use crate::{
    cluster_info::ClusterInfo,
    result::Result,
    serve_repair::{RepairType, ServeRepair},
};
use solana_ledger::{
    bank_forks::BankForks,
    blockstore::{Blockstore, CompletedSlotsReceiver, SlotMeta},
};
use solana_sdk::clock::DEFAULT_SLOTS_PER_EPOCH;
use solana_sdk::{clock::Slot, epoch_schedule::EpochSchedule, pubkey::Pubkey};
use std::{
    collections::BTreeSet,
    net::UdpSocket,
    ops::Bound::{Excluded, Included, Unbounded},
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, RwLock},
    thread::sleep,
    thread::{self, Builder, JoinHandle},
    time::Duration,
};

pub const MAX_REPAIR_LENGTH: usize = 512;
pub const REPAIR_MS: u64 = 100;
pub const MAX_ORPHANS: usize = 5;

pub enum RepairStrategy {
    RepairRange(RepairSlotRange),
    RepairAll {
        bank_forks: Arc<RwLock<BankForks>>,
        completed_slots_receiver: CompletedSlotsReceiver,
        epoch_schedule: EpochSchedule,
    },
}

pub struct RepairSlotRange {
    pub start: Slot,
    pub end: Slot,
}

impl Default for RepairSlotRange {
    fn default() -> Self {
        RepairSlotRange {
            start: 0,
            end: std::u64::MAX,
        }
    }
}

pub struct RepairService {
    t_repair: JoinHandle<()>,
}

impl RepairService {
    pub fn new(
        blockstore: Arc<Blockstore>,
        exit: Arc<AtomicBool>,
        repair_socket: Arc<UdpSocket>,
        cluster_info: Arc<RwLock<ClusterInfo>>,
        repair_strategy: RepairStrategy,
    ) -> Self {
        let t_repair = Builder::new()
            .name("solana-repair-service".to_string())
            .spawn(move || {
                Self::run(
                    &blockstore,
                    &exit,
                    &repair_socket,
                    &cluster_info,
                    repair_strategy,
                )
            })
            .unwrap();

        RepairService { t_repair }
    }

    fn run(
        blockstore: &Arc<Blockstore>,
        exit: &Arc<AtomicBool>,
        repair_socket: &Arc<UdpSocket>,
        cluster_info: &Arc<RwLock<ClusterInfo>>,
        repair_strategy: RepairStrategy,
    ) {
        let serve_repair = ServeRepair::new(cluster_info.clone());
        let mut epoch_slots: BTreeSet<Slot> = BTreeSet::new();
        let mut old_incomplete_slots: BTreeSet<Slot> = BTreeSet::new();
        let id = cluster_info.read().unwrap().id();
        let mut current_root = 0;
        if let RepairStrategy::RepairAll {
            ref epoch_schedule, ..
        } = repair_strategy
        {
            current_root = blockstore.last_root();
            Self::initialize_epoch_slots(
                id,
                blockstore,
                &mut epoch_slots,
                &old_incomplete_slots,
                current_root,
                epoch_schedule,
                cluster_info,
            );
        }
        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            let repairs = {
                match repair_strategy {
                    RepairStrategy::RepairRange(ref repair_slot_range) => {
                        // Strategy used by archivers
                        Self::generate_repairs_in_range(
                            blockstore,
                            MAX_REPAIR_LENGTH,
                            repair_slot_range,
                        )
                    }

                    RepairStrategy::RepairAll {
                        ref completed_slots_receiver,
                        ..
                    } => {
                        let new_root = blockstore.last_root();
                        let lowest_slot = blockstore.lowest_slot();
                        Self::update_epoch_slots(
                            id,
                            new_root,
                            lowest_slot,
                            &mut current_root,
                            &mut epoch_slots,
                            &mut old_incomplete_slots,
                            &cluster_info,
                            completed_slots_receiver,
                        );
                        Self::generate_repairs(blockstore, new_root, MAX_REPAIR_LENGTH)
                    }
                }
            };

            if let Ok(repairs) = repairs {
                let reqs: Vec<_> = repairs
                    .into_iter()
                    .filter_map(|repair_request| {
                        serve_repair
                            .repair_request(&repair_request)
                            .map(|result| (result, repair_request))
                            .ok()
                    })
                    .collect();

                for ((to, req), _) in reqs {
                    repair_socket.send_to(&req, to).unwrap_or_else(|e| {
                        info!("{} repair req send_to({}) error {:?}", id, to, e);
                        0
                    });
                }
            }
            sleep(Duration::from_millis(REPAIR_MS));
        }
    }

    // Generate repairs for all slots `x` in the repair_range.start <= x <= repair_range.end
    pub fn generate_repairs_in_range(
        blockstore: &Blockstore,
        max_repairs: usize,
        repair_range: &RepairSlotRange,
    ) -> Result<Vec<RepairType>> {
        // Slot height and shred indexes for shreds we want to repair
        let mut repairs: Vec<RepairType> = vec![];
        for slot in repair_range.start..=repair_range.end {
            if repairs.len() >= max_repairs {
                break;
            }

            let meta = blockstore
                .meta(slot)
                .expect("Unable to lookup slot meta")
                .unwrap_or(SlotMeta {
                    slot,
                    ..SlotMeta::default()
                });

            let new_repairs = Self::generate_repairs_for_slot(
                blockstore,
                slot,
                &meta,
                max_repairs - repairs.len(),
            );
            repairs.extend(new_repairs);
        }

        Ok(repairs)
    }

    fn generate_repairs(
        blockstore: &Blockstore,
        root: Slot,
        max_repairs: usize,
    ) -> Result<Vec<RepairType>> {
        // Slot height and shred indexes for shreds we want to repair
        let mut repairs: Vec<RepairType> = vec![];
        Self::generate_repairs_for_fork(blockstore, &mut repairs, max_repairs, root);

        // TODO: Incorporate gossip to determine priorities for repair?

        // Try to resolve orphans in blockstore
        let mut orphans = blockstore.get_orphans(Some(MAX_ORPHANS));
        orphans.retain(|x| *x > root);

        Self::generate_repairs_for_orphans(&orphans[..], &mut repairs);
        Ok(repairs)
    }

    fn generate_repairs_for_slot(
        blockstore: &Blockstore,
        slot: Slot,
        slot_meta: &SlotMeta,
        max_repairs: usize,
    ) -> Vec<RepairType> {
        if slot_meta.is_full() {
            vec![]
        } else if slot_meta.consumed == slot_meta.received {
            vec![RepairType::HighestShred(slot, slot_meta.received)]
        } else {
            let reqs = blockstore.find_missing_data_indexes(
                slot,
                slot_meta.first_shred_timestamp,
                slot_meta.consumed,
                slot_meta.received,
                max_repairs,
            );
            reqs.into_iter()
                .map(|i| RepairType::Shred(slot, i))
                .collect()
        }
    }

    fn generate_repairs_for_orphans(orphans: &[u64], repairs: &mut Vec<RepairType>) {
        repairs.extend(orphans.iter().map(|h| RepairType::Orphan(*h)));
    }

    /// Repairs any fork starting at the input slot
    fn generate_repairs_for_fork(
        blockstore: &Blockstore,
        repairs: &mut Vec<RepairType>,
        max_repairs: usize,
        slot: Slot,
    ) {
        let mut pending_slots = vec![slot];
        while repairs.len() < max_repairs && !pending_slots.is_empty() {
            let slot = pending_slots.pop().unwrap();
            if let Some(slot_meta) = blockstore.meta(slot).unwrap() {
                let new_repairs = Self::generate_repairs_for_slot(
                    blockstore,
                    slot,
                    &slot_meta,
                    max_repairs - repairs.len(),
                );
                repairs.extend(new_repairs);
                let next_slots = slot_meta.next_slots;
                pending_slots.extend(next_slots);
            } else {
                break;
            }
        }
    }

    fn get_completed_slots_past_root(
        blockstore: &Blockstore,
        slots_in_gossip: &mut BTreeSet<Slot>,
        root: Slot,
        epoch_schedule: &EpochSchedule,
    ) {
        let last_confirmed_epoch = epoch_schedule.get_leader_schedule_epoch(root);
        let last_epoch_slot = epoch_schedule.get_last_slot_in_epoch(last_confirmed_epoch);

        let meta_iter = blockstore
            .slot_meta_iterator(root + 1)
            .expect("Couldn't get db iterator");

        for (current_slot, meta) in meta_iter {
            if current_slot > last_epoch_slot {
                break;
            }
            if meta.is_full() {
                slots_in_gossip.insert(current_slot);
            }
        }
    }

    fn initialize_epoch_slots(
        id: Pubkey,
        blockstore: &Blockstore,
        slots_in_gossip: &mut BTreeSet<Slot>,
        old_incomplete_slots: &BTreeSet<Slot>,
        root: Slot,
        epoch_schedule: &EpochSchedule,
        cluster_info: &RwLock<ClusterInfo>,
    ) {
        Self::get_completed_slots_past_root(blockstore, slots_in_gossip, root, epoch_schedule);

        // Safe to set into gossip because by this time, the leader schedule cache should
        // also be updated with the latest root (done in blockstore_processor) and thus
        // will provide a schedule to window_service for any incoming shreds up to the
        // last_confirmed_epoch.
        cluster_info.write().unwrap().push_epoch_slots(
            id,
            root,
            blockstore.lowest_slot(),
            slots_in_gossip.clone(),
            old_incomplete_slots,
        );
    }

    // Update the gossiped structure used for the "Repairmen" repair protocol. See book
    // for details.
    fn update_epoch_slots(
        id: Pubkey,
        latest_known_root: Slot,
        lowest_slot: Slot,
        prev_root: &mut Slot,
        slots_in_gossip: &mut BTreeSet<Slot>,
        old_incomplete_slots: &mut BTreeSet<Slot>,
        cluster_info: &RwLock<ClusterInfo>,
        completed_slots_receiver: &CompletedSlotsReceiver,
    ) {
        // If the latest known root is different, update gossip.
        let mut should_update = latest_known_root != *prev_root;
        while let Ok(completed_slots) = completed_slots_receiver.try_recv() {
            for slot in completed_slots {
                old_incomplete_slots.remove(&slot);
                // If the newly completed slot > root, and the set did not contain this value
                // before, we should update gossip.
                if slot > latest_known_root {
                    should_update |= slots_in_gossip.insert(slot);
                }
            }
        }

        if should_update {
            // Filter out everything <= root
            if latest_known_root != *prev_root {
                Self::retain_old_incomplete_slots(
                    slots_in_gossip,
                    *prev_root,
                    latest_known_root,
                    old_incomplete_slots,
                );
                *prev_root = latest_known_root;
                Self::retain_slots_greater_than_root(slots_in_gossip, latest_known_root);
            }

            cluster_info.write().unwrap().push_epoch_slots(
                id,
                latest_known_root,
                lowest_slot,
                slots_in_gossip.clone(),
                old_incomplete_slots,
            );
        }
    }

    fn retain_old_incomplete_slots(
        slots_in_gossip: &BTreeSet<Slot>,
        prev_root: Slot,
        new_root: Slot,
        old_incomplete_slots: &mut BTreeSet<Slot>,
    ) {
        // Prev root and new root are not included in incomplete slot list.
        (prev_root + 1..new_root).for_each(|slot| {
            if !slots_in_gossip.contains(&slot) {
                old_incomplete_slots.insert(slot);
            }
        });
        if let Some(oldest_incomplete_slot) = old_incomplete_slots.iter().next() {
            // Prune old slots
            // Prune in batches to reduce overhead. Pruning starts when oldest slot is 1.5 epochs
            // earlier than the new root. But, we prune all the slots that are older than 1 epoch.
            // So slots in a batch of half epoch are getting pruned
            if oldest_incomplete_slot + DEFAULT_SLOTS_PER_EPOCH + DEFAULT_SLOTS_PER_EPOCH / 2
                < new_root
            {
                let oldest_slot_to_retain = new_root.saturating_sub(DEFAULT_SLOTS_PER_EPOCH);
                *old_incomplete_slots = old_incomplete_slots
                    .range((Included(&oldest_slot_to_retain), Unbounded))
                    .cloned()
                    .collect();
            }
        }
    }

    fn retain_slots_greater_than_root(slot_set: &mut BTreeSet<Slot>, root: Slot) {
        *slot_set = slot_set
            .range((Excluded(&root), Unbounded))
            .cloned()
            .collect();
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_repair.join()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::cluster_info::Node;
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use rand::{thread_rng, Rng};
    use solana_ledger::blockstore::{
        make_chaining_slot_entries, make_many_slot_entries, make_slot_entries,
    };
    use solana_ledger::shred::max_ticks_per_n_shreds;
    use solana_ledger::{blockstore::Blockstore, get_tmp_ledger_path};
    use std::sync::mpsc::channel;
    use std::thread::Builder;

    #[test]
    pub fn test_repair_orphan() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            // Create some orphan slots
            let (mut shreds, _) = make_slot_entries(1, 0, 1);
            let (shreds2, _) = make_slot_entries(5, 2, 1);
            shreds.extend(shreds2);
            blockstore.insert_shreds(shreds, None, false).unwrap();
            assert_eq!(
                RepairService::generate_repairs(&blockstore, 0, 2).unwrap(),
                vec![RepairType::HighestShred(0, 0), RepairType::Orphan(2)]
            );
        }

        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_empty_slot() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let (shreds, _) = make_slot_entries(2, 0, 1);

            // Write this shred to slot 2, should chain to slot 0, which we haven't received
            // any shreds for
            blockstore.insert_shreds(shreds, None, false).unwrap();

            // Check that repair tries to patch the empty slot
            assert_eq!(
                RepairService::generate_repairs(&blockstore, 0, 2).unwrap(),
                vec![RepairType::HighestShred(0, 0)]
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_generate_repairs() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let nth = 3;
            let num_slots = 2;

            // Create some shreds
            let (mut shreds, _) = make_many_slot_entries(0, num_slots as u64, 150 as u64);
            let num_shreds = shreds.len() as u64;
            let num_shreds_per_slot = num_shreds / num_slots;

            // write every nth shred
            let mut shreds_to_write = vec![];
            let mut missing_indexes_per_slot = vec![];
            for i in (0..num_shreds).rev() {
                let index = i % num_shreds_per_slot;
                if index % nth == 0 {
                    shreds_to_write.insert(0, shreds.remove(i as usize));
                } else if i < num_shreds_per_slot {
                    missing_indexes_per_slot.insert(0, index);
                }
            }
            blockstore
                .insert_shreds(shreds_to_write, None, false)
                .unwrap();
            // sleep so that the holes are ready for repair
            sleep(Duration::from_secs(1));
            let expected: Vec<RepairType> = (0..num_slots)
                .flat_map(|slot| {
                    missing_indexes_per_slot
                        .iter()
                        .map(move |shred_index| RepairType::Shred(slot as u64, *shred_index))
                })
                .collect();

            assert_eq!(
                RepairService::generate_repairs(&blockstore, 0, std::usize::MAX).unwrap(),
                expected
            );

            assert_eq!(
                RepairService::generate_repairs(&blockstore, 0, expected.len() - 2).unwrap()[..],
                expected[0..expected.len() - 2]
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_generate_highest_repair() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let num_entries_per_slot = 100;

            // Create some shreds
            let (mut shreds, _) = make_slot_entries(0, 0, num_entries_per_slot as u64);
            let num_shreds_per_slot = shreds.len() as u64;

            // Remove last shred (which is also last in slot) so that slot is not complete
            shreds.pop();

            blockstore.insert_shreds(shreds, None, false).unwrap();

            // We didn't get the last shred for this slot, so ask for the highest shred for that slot
            let expected: Vec<RepairType> =
                vec![RepairType::HighestShred(0, num_shreds_per_slot - 1)];

            assert_eq!(
                RepairService::generate_repairs(&blockstore, 0, std::usize::MAX).unwrap(),
                expected
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_range() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let slots: Vec<u64> = vec![1, 3, 5, 7, 8];
            let num_entries_per_slot = max_ticks_per_n_shreds(1) + 1;

            let shreds = make_chaining_slot_entries(&slots, num_entries_per_slot);
            for (mut slot_shreds, _) in shreds.into_iter() {
                slot_shreds.remove(0);
                blockstore.insert_shreds(slot_shreds, None, false).unwrap();
            }
            // sleep to make slot eligible for repair
            sleep(Duration::from_secs(1));
            // Iterate through all possible combinations of start..end (inclusive on both
            // sides of the range)
            for start in 0..slots.len() {
                for end in start..slots.len() {
                    let mut repair_slot_range = RepairSlotRange::default();
                    repair_slot_range.start = slots[start];
                    repair_slot_range.end = slots[end];
                    let expected: Vec<RepairType> = (repair_slot_range.start
                        ..=repair_slot_range.end)
                        .map(|slot_index| {
                            if slots.contains(&(slot_index as u64)) {
                                RepairType::Shred(slot_index as u64, 0)
                            } else {
                                RepairType::HighestShred(slot_index as u64, 0)
                            }
                        })
                        .collect();

                    assert_eq!(
                        RepairService::generate_repairs_in_range(
                            &blockstore,
                            std::usize::MAX,
                            &repair_slot_range
                        )
                        .unwrap(),
                        expected
                    );
                }
            }
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_repair_range_highest() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();

            let num_entries_per_slot = 10;

            let num_slots = 1;
            let start = 5;

            // Create some shreds in slots 0..num_slots
            for i in start..start + num_slots {
                let parent = if i > 0 { i - 1 } else { 0 };
                let (shreds, _) = make_slot_entries(i, parent, num_entries_per_slot as u64);

                blockstore.insert_shreds(shreds, None, false).unwrap();
            }

            let end = 4;
            let expected: Vec<RepairType> = vec![
                RepairType::HighestShred(end - 2, 0),
                RepairType::HighestShred(end - 1, 0),
                RepairType::HighestShred(end, 0),
            ];

            let mut repair_slot_range = RepairSlotRange::default();
            repair_slot_range.start = 2;
            repair_slot_range.end = end;

            assert_eq!(
                RepairService::generate_repairs_in_range(
                    &blockstore,
                    std::usize::MAX,
                    &repair_slot_range
                )
                .unwrap(),
                expected
            );
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_get_completed_slots_past_root() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&blockstore_path).unwrap();
            let num_entries_per_slot = 10;
            let root = 10;

            let fork1 = vec![5, 7, root, 15, 20, 21];
            let fork1_shreds: Vec<_> = make_chaining_slot_entries(&fork1, num_entries_per_slot)
                .into_iter()
                .flat_map(|(shreds, _)| shreds)
                .collect();
            let fork2 = vec![8, 12];
            let fork2_shreds = make_chaining_slot_entries(&fork2, num_entries_per_slot);

            // Remove the last shred from each slot to make an incomplete slot
            let fork2_incomplete_shreds: Vec<_> = fork2_shreds
                .into_iter()
                .flat_map(|(mut shreds, _)| {
                    shreds.pop();
                    shreds
                })
                .collect();
            let mut full_slots = BTreeSet::new();

            blockstore.insert_shreds(fork1_shreds, None, false).unwrap();
            blockstore
                .insert_shreds(fork2_incomplete_shreds, None, false)
                .unwrap();

            // Test that only slots > root from fork1 were included
            let epoch_schedule = EpochSchedule::custom(32, 32, false);

            RepairService::get_completed_slots_past_root(
                &blockstore,
                &mut full_slots,
                root,
                &epoch_schedule,
            );

            let mut expected: BTreeSet<_> = fork1.into_iter().filter(|x| *x > root).collect();
            assert_eq!(full_slots, expected);

            // Test that slots past the last confirmed epoch boundary don't get included
            let last_epoch = epoch_schedule.get_leader_schedule_epoch(root);
            let last_slot = epoch_schedule.get_last_slot_in_epoch(last_epoch);
            let fork3 = vec![last_slot, last_slot + 1];
            let fork3_shreds: Vec<_> = make_chaining_slot_entries(&fork3, num_entries_per_slot)
                .into_iter()
                .flat_map(|(shreds, _)| shreds)
                .collect();
            blockstore.insert_shreds(fork3_shreds, None, false).unwrap();
            RepairService::get_completed_slots_past_root(
                &blockstore,
                &mut full_slots,
                root,
                &epoch_schedule,
            );
            expected.insert(last_slot);
            assert_eq!(full_slots, expected);
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    pub fn test_update_epoch_slots() {
        let blockstore_path = get_tmp_ledger_path!();
        {
            // Create blockstore
            let (blockstore, _, completed_slots_receiver) =
                Blockstore::open_with_signal(&blockstore_path).unwrap();

            let blockstore = Arc::new(blockstore);

            let mut root = 0;
            let num_slots = 100;
            let entries_per_slot = 5;
            let blockstore_ = blockstore.clone();

            // Spin up thread to write to blockstore
            let writer = Builder::new()
                .name("writer".to_string())
                .spawn(move || {
                    let slots: Vec<_> = (1..num_slots + 1).collect();
                    let mut shreds: Vec<_> = make_chaining_slot_entries(&slots, entries_per_slot)
                        .into_iter()
                        .flat_map(|(shreds, _)| shreds)
                        .collect();
                    shreds.shuffle(&mut thread_rng());
                    let mut i = 0;
                    let max_step = entries_per_slot * 4;
                    let repair_interval_ms = 10;
                    let mut rng = rand::thread_rng();
                    let num_shreds = shreds.len();
                    while i < num_shreds {
                        let step = rng.gen_range(1, max_step + 1) as usize;
                        let step = std::cmp::min(step, num_shreds - i);
                        let shreds_to_insert = shreds.drain(..step).collect_vec();
                        blockstore_
                            .insert_shreds(shreds_to_insert, None, false)
                            .unwrap();
                        sleep(Duration::from_millis(repair_interval_ms));
                        i += step;
                    }
                })
                .unwrap();

            let mut completed_slots = BTreeSet::new();
            let node_info = Node::new_localhost_with_pubkey(&Pubkey::default());
            let cluster_info = RwLock::new(ClusterInfo::new_with_invalid_keypair(
                node_info.info.clone(),
            ));

            let mut old_incomplete_slots: BTreeSet<Slot> = BTreeSet::new();
            while completed_slots.len() < num_slots as usize {
                RepairService::update_epoch_slots(
                    Pubkey::default(),
                    root,
                    blockstore.lowest_slot(),
                    &mut root.clone(),
                    &mut completed_slots,
                    &mut old_incomplete_slots,
                    &cluster_info,
                    &completed_slots_receiver,
                );
            }

            let mut expected: BTreeSet<_> = (1..num_slots + 1).collect();
            assert_eq!(completed_slots, expected);

            // Update with new root, should filter out the slots <= root
            root = num_slots / 2;
            let (shreds, _) = make_slot_entries(num_slots + 2, num_slots + 1, entries_per_slot);
            blockstore.insert_shreds(shreds, None, false).unwrap();
            RepairService::update_epoch_slots(
                Pubkey::default(),
                root,
                0,
                &mut 0,
                &mut completed_slots,
                &mut old_incomplete_slots,
                &cluster_info,
                &completed_slots_receiver,
            );
            expected.insert(num_slots + 2);
            RepairService::retain_slots_greater_than_root(&mut expected, root);
            assert_eq!(completed_slots, expected);
            writer.join().unwrap();
        }
        Blockstore::destroy(&blockstore_path).expect("Expected successful database destruction");
    }

    #[test]
    fn test_update_epoch_slots_new_root() {
        let mut current_root = 0;

        let mut completed_slots = BTreeSet::new();
        let node_info = Node::new_localhost_with_pubkey(&Pubkey::default());
        let cluster_info = RwLock::new(ClusterInfo::new_with_invalid_keypair(
            node_info.info.clone(),
        ));
        let my_pubkey = Pubkey::new_rand();
        let (completed_slots_sender, completed_slots_receiver) = channel();

        let mut old_incomplete_slots: BTreeSet<Slot> = BTreeSet::new();
        // Send a new slot before the root is updated
        let newly_completed_slot = 63;
        completed_slots_sender
            .send(vec![newly_completed_slot])
            .unwrap();
        RepairService::update_epoch_slots(
            my_pubkey.clone(),
            current_root,
            0,
            &mut current_root.clone(),
            &mut completed_slots,
            &mut old_incomplete_slots,
            &cluster_info,
            &completed_slots_receiver,
        );

        // We should see epoch state update
        let (my_epoch_slots_in_gossip, updated_ts) = {
            let r_cluster_info = cluster_info.read().unwrap();

            let (my_epoch_slots_in_gossip, updated_ts) = r_cluster_info
                .get_epoch_state_for_node(&my_pubkey, None)
                .clone()
                .unwrap();

            (my_epoch_slots_in_gossip.clone(), updated_ts)
        };

        assert_eq!(my_epoch_slots_in_gossip.root, 0);
        assert_eq!(current_root, 0);
        assert_eq!(my_epoch_slots_in_gossip.slots.len(), 1);
        assert!(my_epoch_slots_in_gossip
            .slots
            .contains(&newly_completed_slot));

        // Calling update again with no updates to either the roots or set of completed slots
        // should not update gossip
        RepairService::update_epoch_slots(
            my_pubkey.clone(),
            current_root,
            0,
            &mut current_root,
            &mut completed_slots,
            &mut old_incomplete_slots,
            &cluster_info,
            &completed_slots_receiver,
        );

        assert!(cluster_info
            .read()
            .unwrap()
            .get_epoch_state_for_node(&my_pubkey, Some(updated_ts))
            .is_none());

        sleep(Duration::from_millis(10));
        // Updating just the root again should update gossip (simulates replay stage updating root
        // after a slot has been signaled as completed)
        RepairService::update_epoch_slots(
            my_pubkey.clone(),
            current_root + 1,
            0,
            &mut current_root,
            &mut completed_slots,
            &mut old_incomplete_slots,
            &cluster_info,
            &completed_slots_receiver,
        );

        let r_cluster_info = cluster_info.read().unwrap();

        let (my_epoch_slots_in_gossip, _) = r_cluster_info
            .get_epoch_state_for_node(&my_pubkey, Some(updated_ts))
            .clone()
            .unwrap();

        // Check the root was updated correctly
        assert_eq!(my_epoch_slots_in_gossip.root, 1);
        assert_eq!(current_root, 1);
        assert_eq!(my_epoch_slots_in_gossip.slots.len(), 1);
        assert!(my_epoch_slots_in_gossip
            .slots
            .contains(&newly_completed_slot));
    }

    #[test]
    fn test_retain_old_incomplete_slots() {
        let mut old_incomplete_slots: BTreeSet<Slot> = BTreeSet::new();
        let mut slots_in_gossip: BTreeSet<Slot> = BTreeSet::new();

        // When slots_in_gossip is empty. All slots between prev and new root
        // should be incomplete
        RepairService::retain_old_incomplete_slots(
            &slots_in_gossip,
            10,
            100,
            &mut old_incomplete_slots,
        );

        assert!(!old_incomplete_slots.contains(&10));
        assert!(!old_incomplete_slots.contains(&100));
        (11..100u64)
            .into_iter()
            .for_each(|i| assert!(old_incomplete_slots.contains(&i)));

        // Insert some slots in slots_in_gossip.
        slots_in_gossip.insert(101);
        slots_in_gossip.insert(102);
        slots_in_gossip.insert(104);
        slots_in_gossip.insert(105);

        RepairService::retain_old_incomplete_slots(
            &slots_in_gossip,
            100,
            105,
            &mut old_incomplete_slots,
        );

        assert!(!old_incomplete_slots.contains(&10));
        assert!(!old_incomplete_slots.contains(&100));
        (11..100u64)
            .into_iter()
            .for_each(|i| assert!(old_incomplete_slots.contains(&i)));
        assert!(!old_incomplete_slots.contains(&101));
        assert!(!old_incomplete_slots.contains(&102));
        assert!(old_incomplete_slots.contains(&103));
        assert!(!old_incomplete_slots.contains(&104));
        assert!(!old_incomplete_slots.contains(&105));

        // Insert some slots that are 1 epoch away. It should not trigger
        // pruning, as we wait 1.5 epoch.
        slots_in_gossip.insert(105 + DEFAULT_SLOTS_PER_EPOCH);

        RepairService::retain_old_incomplete_slots(
            &slots_in_gossip,
            100 + DEFAULT_SLOTS_PER_EPOCH,
            105 + DEFAULT_SLOTS_PER_EPOCH,
            &mut old_incomplete_slots,
        );

        assert!(!old_incomplete_slots.contains(&10));
        assert!(!old_incomplete_slots.contains(&100));
        (11..100u64)
            .into_iter()
            .for_each(|i| assert!(old_incomplete_slots.contains(&i)));
        assert!(!old_incomplete_slots.contains(&101));
        assert!(!old_incomplete_slots.contains(&102));
        assert!(old_incomplete_slots.contains(&103));
        assert!(!old_incomplete_slots.contains(&104));
        assert!(!old_incomplete_slots.contains(&105));
        assert!(old_incomplete_slots.contains(&(101 + DEFAULT_SLOTS_PER_EPOCH)));
        assert!(old_incomplete_slots.contains(&(102 + DEFAULT_SLOTS_PER_EPOCH)));
        assert!(old_incomplete_slots.contains(&(103 + DEFAULT_SLOTS_PER_EPOCH)));
        assert!(old_incomplete_slots.contains(&(104 + DEFAULT_SLOTS_PER_EPOCH)));

        // Insert some slots that are 1.5 epoch away. It should trigger
        // pruning, as we wait 1.5 epoch.
        let one_and_half_epoch_slots = DEFAULT_SLOTS_PER_EPOCH + DEFAULT_SLOTS_PER_EPOCH / 2;
        slots_in_gossip.insert(100 + one_and_half_epoch_slots);

        RepairService::retain_old_incomplete_slots(
            &slots_in_gossip,
            99 + one_and_half_epoch_slots,
            100 + one_and_half_epoch_slots,
            &mut old_incomplete_slots,
        );

        assert!(!old_incomplete_slots.contains(&10));
        assert!(!old_incomplete_slots.contains(&100));
        (11..100u64)
            .into_iter()
            .for_each(|i| assert!(!old_incomplete_slots.contains(&i)));
        assert!(!old_incomplete_slots.contains(&101));
        assert!(!old_incomplete_slots.contains(&102));
        assert!(!old_incomplete_slots.contains(&103));
        assert!(!old_incomplete_slots.contains(&104));
        assert!(!old_incomplete_slots.contains(&105));
        assert!(old_incomplete_slots.contains(&(101 + DEFAULT_SLOTS_PER_EPOCH)));
        assert!(old_incomplete_slots.contains(&(102 + DEFAULT_SLOTS_PER_EPOCH)));
        assert!(old_incomplete_slots.contains(&(103 + DEFAULT_SLOTS_PER_EPOCH)));
        assert!(old_incomplete_slots.contains(&(104 + DEFAULT_SLOTS_PER_EPOCH)));
    }
}
