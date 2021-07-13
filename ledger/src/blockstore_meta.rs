use crate::erasure::ErasureConfig;
use serde::{Deserialize, Serialize};
use solana_sdk::{clock::Slot, hash::Hash};
use std::{collections::BTreeSet, ops::RangeBounds};

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
// The Meta column family
pub struct SlotMeta {
    // The number of slots above the root (the genesis block). The first
    // slot has slot 0.
    pub slot: Slot,
    // The total number of consecutive shreds starting from index 0
    // we have received for this slot.
    pub consumed: u64,
    // The index *plus one* of the highest shred received for this slot.  Useful
    // for checking if the slot has received any shreds yet, and to calculate the
    // range where there is one or more holes: `(consumed..received)`.
    pub received: u64,
    // The timestamp of the first time a shred was added for this slot
    pub first_shred_timestamp: u64,
    // The index of the shred that is flagged as the last shred for this slot.
    pub last_index: u64,
    // The slot height of the block this one derives from.
    pub parent_slot: Slot,
    // The list of slots, each of which contains a block that derives
    // from this one.
    pub next_slots: Vec<Slot>,
    // True if this slot is full (consumed == last_index + 1) and if every
    // slot that is a parent of this slot is also connected.
    pub is_connected: bool,
    // List of start indexes for completed data slots
    pub completed_data_indexes: Vec<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
/// Index recording presence/absence of shreds
pub struct Index {
    pub slot: Slot,
    data: ShredIndex,
    coding: ShredIndex,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ShredIndex {
    /// Map representing presence/absence of shreds
    index: BTreeSet<u64>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
/// Erasure coding information
pub struct ErasureMeta {
    /// Which erasure set in the slot this is
    pub set_index: u64,
    /// Deprecated field.
    #[serde(rename = "first_coding_index")]
    __unused: u64,
    /// Size of shards in this erasure set
    pub size: usize,
    /// Erasure configuration for this erasure set
    pub config: ErasureConfig,
}

#[derive(Deserialize, Serialize)]
pub struct DuplicateSlotProof {
    #[serde(with = "serde_bytes")]
    pub shred1: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub shred2: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub enum ErasureMetaStatus {
    CanRecover,
    DataFull,
    StillNeed(usize),
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub enum FrozenHashVersioned {
    Current(FrozenHashStatus),
}

impl FrozenHashVersioned {
    pub fn frozen_hash(&self) -> Hash {
        match self {
            FrozenHashVersioned::Current(frozen_hash_status) => frozen_hash_status.frozen_hash,
        }
    }

    pub fn is_duplicate_confirmed(&self) -> bool {
        match self {
            FrozenHashVersioned::Current(frozen_hash_status) => {
                frozen_hash_status.is_duplicate_confirmed
            }
        }
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct FrozenHashStatus {
    pub frozen_hash: Hash,
    pub is_duplicate_confirmed: bool,
}

impl Index {
    pub(crate) fn new(slot: Slot) -> Self {
        Index {
            slot,
            data: ShredIndex::default(),
            coding: ShredIndex::default(),
        }
    }

    pub fn data(&self) -> &ShredIndex {
        &self.data
    }
    pub fn coding(&self) -> &ShredIndex {
        &self.coding
    }

    pub fn data_mut(&mut self) -> &mut ShredIndex {
        &mut self.data
    }
    pub fn coding_mut(&mut self) -> &mut ShredIndex {
        &mut self.coding
    }
}

impl ShredIndex {
    pub fn num_shreds(&self) -> usize {
        self.index.len()
    }

    pub fn present_in_bounds(&self, bounds: impl RangeBounds<u64>) -> usize {
        self.index.range(bounds).count()
    }

    pub fn is_present(&self, index: u64) -> bool {
        self.index.contains(&index)
    }

    pub fn set_present(&mut self, index: u64, presence: bool) {
        if presence {
            self.index.insert(index);
        } else {
            self.index.remove(&index);
        }
    }

    pub fn set_many_present(&mut self, presence: impl IntoIterator<Item = (u64, bool)>) {
        for (idx, present) in presence.into_iter() {
            self.set_present(idx, present);
        }
    }

    pub fn largest(&self) -> Option<u64> {
        self.index.iter().rev().next().copied()
    }
}

impl SlotMeta {
    pub fn is_full(&self) -> bool {
        // last_index is std::u64::MAX when it has no information about how
        // many shreds will fill this slot.
        // Note: A full slot with zero shreds is not possible.
        if self.last_index == std::u64::MAX {
            return false;
        }

        // Should never happen
        if self.consumed > self.last_index + 1 {
            datapoint_error!(
                "blockstore_error",
                (
                    "error",
                    format!(
                        "Observed a slot meta with consumed: {} > meta.last_index + 1: {}",
                        self.consumed,
                        self.last_index + 1
                    ),
                    String
                )
            );
        }

        self.consumed == self.last_index + 1
    }

    pub fn is_parent_set(&self) -> bool {
        self.parent_slot != std::u64::MAX
    }

    pub fn clear_unconfirmed_slot(&mut self) {
        let mut new_self = SlotMeta::new_orphan(self.slot);
        std::mem::swap(&mut new_self.next_slots, &mut self.next_slots);
        std::mem::swap(self, &mut new_self);
    }

    pub(crate) fn new(slot: Slot, parent_slot: Slot) -> Self {
        SlotMeta {
            slot,
            consumed: 0,
            received: 0,
            first_shred_timestamp: 0,
            parent_slot,
            next_slots: vec![],
            is_connected: slot == 0,
            last_index: std::u64::MAX,
            completed_data_indexes: vec![],
        }
    }

    pub(crate) fn new_orphan(slot: Slot) -> Self {
        Self::new(slot, std::u64::MAX)
    }
}

impl ErasureMeta {
    pub fn new(set_index: u64, config: ErasureConfig) -> ErasureMeta {
        ErasureMeta {
            set_index,
            config,
            ..Self::default()
        }
    }

    pub fn status(&self, index: &Index) -> ErasureMetaStatus {
        use ErasureMetaStatus::*;

        let coding_indices = self.set_index..self.set_index + self.config.num_coding() as u64;
        let num_coding = index.coding().present_in_bounds(coding_indices);
        let num_data = index
            .data()
            .present_in_bounds(self.set_index..self.set_index + self.config.num_data() as u64);

        let (data_missing, num_needed) = (
            self.config.num_data().saturating_sub(num_data),
            self.config.num_data().saturating_sub(num_data + num_coding),
        );

        if data_missing == 0 {
            DataFull
        } else if num_needed == 0 {
            CanRecover
        } else {
            StillNeed(num_needed)
        }
    }

    pub fn set_size(&mut self, size: usize) {
        self.size = size;
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

impl DuplicateSlotProof {
    pub(crate) fn new(shred1: Vec<u8>, shred2: Vec<u8>) -> Self {
        DuplicateSlotProof { shred1, shred2 }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TransactionStatusIndexMeta {
    pub max_slot: Slot,
    pub frozen: bool,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct AddressSignatureMeta {
    pub writeable: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct PerfSample {
    pub num_transactions: u64,
    pub num_slots: u64,
    pub sample_period_secs: u16,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ProgramCost {
    pub cost: u64,
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{seq::SliceRandom, thread_rng};
    use std::iter::repeat;

    #[test]
    fn test_erasure_meta_status() {
        use ErasureMetaStatus::*;

        let set_index = 0;
        let erasure_config = ErasureConfig::default();

        let mut e_meta = ErasureMeta::new(set_index, erasure_config);
        let mut rng = thread_rng();
        let mut index = Index::new(0);
        e_meta.size = 1;

        let data_indexes = 0..erasure_config.num_data() as u64;
        let coding_indexes = 0..erasure_config.num_coding() as u64;

        assert_eq!(e_meta.status(&index), StillNeed(erasure_config.num_data()));

        index
            .data_mut()
            .set_many_present(data_indexes.clone().zip(repeat(true)));

        assert_eq!(e_meta.status(&index), DataFull);

        index
            .coding_mut()
            .set_many_present(coding_indexes.clone().zip(repeat(true)));

        for &idx in data_indexes
            .clone()
            .collect::<Vec<_>>()
            .choose_multiple(&mut rng, erasure_config.num_data())
        {
            index.data_mut().set_present(idx, false);

            assert_eq!(e_meta.status(&index), CanRecover);
        }

        index
            .data_mut()
            .set_many_present(data_indexes.zip(repeat(true)));

        for &idx in coding_indexes
            .collect::<Vec<_>>()
            .choose_multiple(&mut rng, erasure_config.num_coding())
        {
            index.coding_mut().set_present(idx, false);

            assert_eq!(e_meta.status(&index), DataFull);
        }
    }

    #[test]
    fn test_clear_unconfirmed_slot() {
        let mut slot_meta = SlotMeta::new_orphan(5);
        slot_meta.consumed = 5;
        slot_meta.received = 5;
        slot_meta.next_slots = vec![6, 7];
        slot_meta.clear_unconfirmed_slot();

        let mut expected = SlotMeta::new_orphan(5);
        expected.next_slots = vec![6, 7];
        assert_eq!(slot_meta, expected);
    }
}
