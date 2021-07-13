use crate::{
    ancestors::Ancestors,
    contains::Contains,
    inline_spl_token_v2_0::{self, SPL_TOKEN_ACCOUNT_MINT_OFFSET, SPL_TOKEN_ACCOUNT_OWNER_OFFSET},
    secondary_index::*,
};
use bv::BitVec;
use log::*;
use ouroboros::self_referencing;
use solana_measure::measure::Measure;
use solana_sdk::{
    clock::{BankId, Slot},
    pubkey::{Pubkey, PUBKEY_BYTES},
};
use std::{
    collections::{
        btree_map::{self, BTreeMap, Entry},
        HashSet,
    },
    ops::{
        Bound,
        Bound::{Excluded, Included, Unbounded},
        Range, RangeBounds,
    },
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
    },
};
use thiserror::Error;

pub const ITER_BATCH_SIZE: usize = 1000;
const BINS: usize = 16;
pub type ScanResult<T> = Result<T, ScanError>;
pub type SlotList<T> = Vec<(Slot, T)>;
pub type SlotSlice<'s, T> = &'s [(Slot, T)];
pub type RefCount = u64;
pub type AccountMap<K, V> = BTreeMap<K, V>;

type AccountMapEntry<T> = Arc<AccountMapEntryInner<T>>;

pub trait IsCached {
    fn is_cached(&self) -> bool;
}

impl IsCached for bool {
    fn is_cached(&self) -> bool {
        false
    }
}

impl IsCached for u64 {
    fn is_cached(&self) -> bool {
        false
    }
}

#[derive(Error, Debug, PartialEq)]
pub enum ScanError {
    #[error("Node detected it replayed bad version of slot {slot:?} with id {bank_id:?}, thus the scan on said slot was aborted")]
    SlotRemoved { slot: Slot, bank_id: BankId },
}

enum ScanTypes<R: RangeBounds<Pubkey>> {
    Unindexed(Option<R>),
    Indexed(IndexKey),
}

#[derive(Debug, Clone, Copy)]
pub enum IndexKey {
    ProgramId(Pubkey),
    SplTokenMint(Pubkey),
    SplTokenOwner(Pubkey),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AccountIndex {
    ProgramId,
    SplTokenMint,
    SplTokenOwner,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AccountSecondaryIndexesIncludeExclude {
    pub exclude: bool,
    pub keys: HashSet<Pubkey>,
}

#[derive(Debug, Default, Clone)]
pub struct AccountSecondaryIndexes {
    pub keys: Option<AccountSecondaryIndexesIncludeExclude>,
    pub indexes: HashSet<AccountIndex>,
}

impl AccountSecondaryIndexes {
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }
    pub fn contains(&self, index: &AccountIndex) -> bool {
        self.indexes.contains(index)
    }
    pub fn include_key(&self, key: &Pubkey) -> bool {
        match &self.keys {
            Some(options) => options.exclude ^ options.keys.contains(key),
            None => true, // include all keys
        }
    }
}

#[derive(Debug)]
pub struct AccountMapEntryInner<T> {
    ref_count: AtomicU64,
    pub slot_list: RwLock<SlotList<T>>,
}

impl<T> AccountMapEntryInner<T> {
    pub fn ref_count(&self) -> u64 {
        self.ref_count.load(Ordering::Relaxed)
    }
}

pub enum AccountIndexGetResult<'a, T: 'static> {
    Found(ReadAccountMapEntry<T>, usize),
    NotFoundOnFork,
    Missing(AccountMapsReadLock<'a, T>),
}

#[self_referencing]
pub struct ReadAccountMapEntry<T: 'static> {
    owned_entry: AccountMapEntry<T>,
    #[borrows(owned_entry)]
    #[covariant]
    slot_list_guard: RwLockReadGuard<'this, SlotList<T>>,
}

impl<T: Clone> ReadAccountMapEntry<T> {
    pub fn from_account_map_entry(account_map_entry: AccountMapEntry<T>) -> Self {
        ReadAccountMapEntryBuilder {
            owned_entry: account_map_entry,
            slot_list_guard_builder: |lock| lock.slot_list.read().unwrap(),
        }
        .build()
    }

    pub fn slot_list(&self) -> &SlotList<T> {
        &*self.borrow_slot_list_guard()
    }

    pub fn ref_count(&self) -> &AtomicU64 {
        &self.borrow_owned_entry().ref_count
    }

    pub fn unref(&self) {
        self.ref_count().fetch_sub(1, Ordering::Relaxed);
    }

    pub fn addref(&self) {
        self.ref_count().fetch_add(1, Ordering::Relaxed);
    }
}

#[self_referencing]
pub struct WriteAccountMapEntry<T: 'static> {
    owned_entry: AccountMapEntry<T>,
    #[borrows(owned_entry)]
    #[covariant]
    slot_list_guard: RwLockWriteGuard<'this, SlotList<T>>,
}

impl<T: 'static + Clone + IsCached> WriteAccountMapEntry<T> {
    pub fn from_account_map_entry(account_map_entry: AccountMapEntry<T>) -> Self {
        WriteAccountMapEntryBuilder {
            owned_entry: account_map_entry,
            slot_list_guard_builder: |lock| lock.slot_list.write().unwrap(),
        }
        .build()
    }

    pub fn slot_list(&mut self) -> &SlotList<T> {
        &*self.borrow_slot_list_guard()
    }

    pub fn slot_list_mut<RT>(
        &mut self,
        user: impl for<'this> FnOnce(&mut RwLockWriteGuard<'this, SlotList<T>>) -> RT,
    ) -> RT {
        self.with_slot_list_guard_mut(user)
    }

    pub fn ref_count(&self) -> &AtomicU64 {
        &self.borrow_owned_entry().ref_count
    }

    // create an entry that is equivalent to this process:
    // 1. new empty (refcount=0, slot_list={})
    // 2. update(slot, account_info)
    // This code is called when the first entry [ie. (slot,account_info)] for a pubkey is inserted into the index.
    pub fn new_entry_after_update(slot: Slot, account_info: T) -> AccountMapEntry<T> {
        let ref_count = if account_info.is_cached() { 0 } else { 1 };
        Arc::new(AccountMapEntryInner {
            ref_count: AtomicU64::new(ref_count),
            slot_list: RwLock::new(vec![(slot, account_info)]),
        })
    }

    // Try to update an item in the slot list the given `slot` If an item for the slot
    // already exists in the list, remove the older item, add it to `reclaims`, and insert
    // the new item.
    pub fn update(&mut self, slot: Slot, account_info: T, reclaims: &mut SlotList<T>) {
        let mut addref = !account_info.is_cached();
        self.slot_list_mut(|list| {
            // find other dirty entries from the same slot
            for list_index in 0..list.len() {
                let (s, previous_update_value) = &list[list_index];
                if *s == slot {
                    addref = addref && previous_update_value.is_cached();

                    let mut new_item = (slot, account_info);
                    std::mem::swap(&mut new_item, &mut list[list_index]);
                    reclaims.push(new_item);
                    list[(list_index + 1)..]
                        .iter()
                        .for_each(|item| assert!(item.0 != slot));
                    return; // this returns from self.slot_list_mut above
                }
            }

            // if we make it here, we did not find the slot in the list
            list.push((slot, account_info));
        });
        if addref {
            // If it's the first non-cache insert, also bump the stored ref count
            self.ref_count().fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Default, AbiExample, Clone)]
pub struct RollingBitField {
    max_width: u64,
    min: u64,
    max: u64, // exclusive
    bits: BitVec,
    count: usize,
    // These are items that are true and lower than min.
    // They would cause us to exceed max_width if we stored them in our bit field.
    // We only expect these items in conditions where there is some other bug in the system
    //  or in testing when large ranges are created.
    excess: HashSet<u64>,
}

impl PartialEq<RollingBitField> for RollingBitField {
    fn eq(&self, other: &Self) -> bool {
        // 2 instances could have different internal data for the same values,
        // so we have to compare data.
        self.len() == other.len() && {
            for item in self.get_all() {
                if !other.contains(&item) {
                    return false;
                }
            }
            true
        }
    }
}

// functionally similar to a hashset
// Relies on there being a sliding window of key values. The key values continue to increase.
// Old key values are removed from the lesser values and do not accumulate.
impl RollingBitField {
    pub fn new(max_width: u64) -> Self {
        assert!(max_width > 0);
        assert!(max_width.is_power_of_two()); // power of 2 to make dividing a shift
        let bits = BitVec::new_fill(false, max_width);
        Self {
            max_width,
            bits,
            count: 0,
            min: 0,
            max: 0,
            excess: HashSet::new(),
        }
    }

    // find the array index
    fn get_address(&self, key: &u64) -> u64 {
        key % self.max_width
    }

    pub fn range_width(&self) -> u64 {
        // note that max isn't updated on remove, so it can be above the current max
        self.max - self.min
    }

    pub fn min(&self) -> Option<u64> {
        if self.is_empty() {
            None
        } else if self.excess.is_empty() {
            Some(self.min)
        } else {
            let mut min = if self.all_items_in_excess() {
                u64::MAX
            } else {
                self.min
            };
            for item in &self.excess {
                min = std::cmp::min(min, *item);
            }
            Some(min)
        }
    }

    pub fn insert(&mut self, key: u64) {
        let mut bits_empty = self.count == 0 || self.all_items_in_excess();
        let update_bits = if bits_empty {
            true // nothing in bits, so in range
        } else if key < self.min {
            // bits not empty and this insert is before min, so add to excess
            if self.excess.insert(key) {
                self.count += 1;
            }
            false
        } else if key < self.max {
            true // fits current bit field range
        } else {
            // key is >= max
            let new_max = key + 1;
            loop {
                let new_width = new_max.saturating_sub(self.min);
                if new_width <= self.max_width {
                    // this key will fit the max range
                    break;
                }

                // move the min item from bits to excess and then purge from min to make room for this new max
                let inserted = self.excess.insert(self.min);
                assert!(inserted);

                let key = self.min;
                let address = self.get_address(&key);
                self.bits.set(address, false);
                self.purge(&key);

                if self.all_items_in_excess() {
                    // if we moved the last existing item to excess, then we are ready to insert the new item in the bits
                    bits_empty = true;
                    break;
                }
            }

            true // moved things to excess if necessary, so update bits with the new entry
        };

        if update_bits {
            let address = self.get_address(&key);
            let value = self.bits.get(address);
            if !value {
                self.bits.set(address, true);
                if bits_empty {
                    self.min = key;
                    self.max = key + 1;
                } else {
                    self.min = std::cmp::min(self.min, key);
                    self.max = std::cmp::max(self.max, key + 1);
                    assert!(
                        self.min + self.max_width >= self.max,
                        "min: {}, max: {}, max_width: {}",
                        self.min,
                        self.max,
                        self.max_width
                    );
                }
                self.count += 1;
            }
        }
    }

    pub fn remove(&mut self, key: &u64) -> bool {
        if key >= &self.min {
            // if asked to remove something bigger than max, then no-op
            if key < &self.max {
                let address = self.get_address(key);
                let get = self.bits.get(address);
                if get {
                    self.count -= 1;
                    self.bits.set(address, false);
                    self.purge(key);
                }
                get
            } else {
                false
            }
        } else {
            // asked to remove something < min. would be in excess if it exists
            let remove = self.excess.remove(key);
            if remove {
                self.count -= 1;
            }
            remove
        }
    }

    fn all_items_in_excess(&self) -> bool {
        self.excess.len() == self.count
    }

    // after removing 'key' where 'key' = min, make min the correct new min value
    fn purge(&mut self, key: &u64) {
        if self.count > 0 && !self.all_items_in_excess() {
            if key == &self.min {
                let start = self.min + 1; // min just got removed
                for key in start..self.max {
                    if self.contains_assume_in_range(&key) {
                        self.min = key;
                        break;
                    }
                }
            }
        } else {
            // The idea is that there are no items in the bitfield anymore.
            // But, there MAY be items in excess. The model works such that items < min go into excess.
            // So, after purging all items from bitfield, we hold max to be what it previously was, but set min to max.
            // Thus, if we lookup >= max, answer is always false without having to look in excess.
            // If we changed max here to 0, we would lose the ability to know the range of items in excess (if any).
            // So, now, with min updated = max:
            // If we lookup < max, then we first check min.
            // If >= min, then we look in bitfield.
            // Otherwise, we look in excess since the request is < min.
            // So, resetting min like this after a remove results in the correct behavior for the model.
            // Later, if we insert and there are 0 items total (excess + bitfield), then we reset min/max to reflect the new item only.
            self.min = self.max;
        }
    }

    fn contains_assume_in_range(&self, key: &u64) -> bool {
        // the result may be aliased. Caller is responsible for determining key is in range.
        let address = self.get_address(key);
        self.bits.get(address)
    }

    // This is the 99% use case.
    // This needs be fast for the most common case of asking for key >= min.
    pub fn contains(&self, key: &u64) -> bool {
        if key < &self.max {
            if key >= &self.min {
                // in the bitfield range
                self.contains_assume_in_range(key)
            } else {
                self.excess.contains(key)
            }
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&mut self) {
        let mut n = Self::new(self.max_width);
        std::mem::swap(&mut n, self);
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    pub fn get_all(&self) -> Vec<u64> {
        let mut all = Vec::with_capacity(self.count);
        self.excess.iter().for_each(|slot| all.push(*slot));
        for key in self.min..self.max {
            if self.contains_assume_in_range(&key) {
                all.push(key);
            }
        }
        all
    }
}

#[derive(Debug)]
pub struct RootsTracker {
    roots: RollingBitField,
    max_root: Slot,
    uncleaned_roots: HashSet<Slot>,
    previous_uncleaned_roots: HashSet<Slot>,
}

impl Default for RootsTracker {
    fn default() -> Self {
        // we expect to keep a rolling set of 400k slots around at a time
        // 4M gives us plenty of extra(?!) room to handle a width 10x what we should need.
        // cost is 4M bits of memory, which is .5MB
        RootsTracker::new(4194304)
    }
}

impl RootsTracker {
    pub fn new(max_width: u64) -> Self {
        Self {
            roots: RollingBitField::new(max_width),
            max_root: 0,
            uncleaned_roots: HashSet::new(),
            previous_uncleaned_roots: HashSet::new(),
        }
    }

    pub fn min_root(&self) -> Option<Slot> {
        self.roots.min()
    }
}

#[derive(Debug, Default)]
pub struct AccountsIndexRootsStats {
    pub roots_len: usize,
    pub uncleaned_roots_len: usize,
    pub previous_uncleaned_roots_len: usize,
    pub roots_range: u64,
    pub rooted_cleaned_count: usize,
    pub unrooted_cleaned_count: usize,
}

pub struct AccountsIndexIterator<'a, T> {
    account_maps: &'a LockMapTypeSlice<T>,
    start_bound: Bound<Pubkey>,
    end_bound: Bound<Pubkey>,
    is_finished: bool,
}

impl<'a, T> AccountsIndexIterator<'a, T> {
    fn clone_bound(bound: Bound<&Pubkey>) -> Bound<Pubkey> {
        match bound {
            Unbounded => Unbounded,
            Included(k) => Included(*k),
            Excluded(k) => Excluded(*k),
        }
    }

    pub fn new<R>(account_maps: &'a LockMapTypeSlice<T>, range: Option<R>) -> Self
    where
        R: RangeBounds<Pubkey>,
    {
        Self {
            start_bound: range
                .as_ref()
                .map(|r| Self::clone_bound(r.start_bound()))
                .unwrap_or(Unbounded),
            end_bound: range
                .as_ref()
                .map(|r| Self::clone_bound(r.end_bound()))
                .unwrap_or(Unbounded),
            account_maps,
            is_finished: false,
        }
    }
}

impl<'a, T: 'static + Clone> Iterator for AccountsIndexIterator<'a, T> {
    type Item = Vec<(Pubkey, AccountMapEntry<T>)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.is_finished {
            return None;
        }

        let mut chunk: Vec<(Pubkey, AccountMapEntry<T>)> = Vec::with_capacity(ITER_BATCH_SIZE);
        'outer: for i in self.account_maps.iter() {
            for (pubkey, account_map_entry) in
                i.read().unwrap().range((self.start_bound, self.end_bound))
            {
                if chunk.len() >= ITER_BATCH_SIZE {
                    break 'outer;
                }
                let item = (*pubkey, account_map_entry.clone());
                chunk.push(item);
            }
        }

        if chunk.is_empty() {
            self.is_finished = true;
            return None;
        }

        self.start_bound = Excluded(chunk.last().unwrap().0);
        Some(chunk)
    }
}

pub trait ZeroLamport {
    fn is_zero_lamport(&self) -> bool;
}

fn get_bin_pubkey(pubkey: &Pubkey) -> usize {
    let byte_of_pubkey_to_bin = 0; // TODO: this should not be 0. For now it needs to be due to requests for in-order pubkeys
    (pubkey.as_ref()[byte_of_pubkey_to_bin] as usize) * BINS / ((u8::MAX as usize) + 1)
}

type MapType<T> = AccountMap<Pubkey, AccountMapEntry<T>>;
type LockMapType<T> = Vec<RwLock<MapType<T>>>;
type LockMapTypeSlice<T> = [RwLock<MapType<T>>];
type AccountMapsWriteLock<'a, T> = RwLockWriteGuard<'a, MapType<T>>;
type AccountMapsReadLock<'a, T> = RwLockReadGuard<'a, MapType<T>>;

#[derive(Debug, Default)]
pub struct ScanSlotTracker {
    is_removed: bool,
    ref_count: u64,
}

impl ScanSlotTracker {
    pub fn is_removed(&self) -> bool {
        self.is_removed
    }

    pub fn mark_removed(&mut self) {
        self.is_removed = true;
    }
}

#[derive(Debug)]
pub struct AccountsIndex<T> {
    pub account_maps: LockMapType<T>,
    program_id_index: SecondaryIndex<DashMapSecondaryIndexEntry>,
    spl_token_mint_index: SecondaryIndex<DashMapSecondaryIndexEntry>,
    spl_token_owner_index: SecondaryIndex<RwLockSecondaryIndexEntry>,
    roots_tracker: RwLock<RootsTracker>,
    ongoing_scan_roots: RwLock<BTreeMap<Slot, u64>>,
    // Each scan has some latest slot `S` that is the tip of the fork the scan
    // is iterating over. The unique id of that slot `S` is recorded here (note we don't use
    // `S` as the id because there can be more than one version of a slot `S`). If a fork
    // is abandoned, all of the slots on that fork up to `S` will be removed via
    // `AccountsDb::remove_unrooted_slots()`. When the scan finishes, it'll realize that the
    // results of the scan may have been corrupted by `remove_unrooted_slots` and abort its results.
    //
    // `removed_bank_ids` tracks all the slot ids that were removed via `remove_unrooted_slots()` so any attempted scans
    // on any of these slots fails. This is safe to purge once the associated Bank is dropped and
    // scanning the fork with that Bank at the tip is no longer possible.
    pub removed_bank_ids: Mutex<HashSet<BankId>>,
}

impl<T> Default for AccountsIndex<T> {
    fn default() -> Self {
        Self {
            account_maps: (0..BINS)
                .into_iter()
                .map(|_| RwLock::new(AccountMap::<Pubkey, AccountMapEntry<T>>::default()))
                .collect::<Vec<_>>(),
            program_id_index: SecondaryIndex::<DashMapSecondaryIndexEntry>::new(
                "program_id_index_stats",
            ),
            spl_token_mint_index: SecondaryIndex::<DashMapSecondaryIndexEntry>::new(
                "spl_token_mint_index_stats",
            ),
            spl_token_owner_index: SecondaryIndex::<RwLockSecondaryIndexEntry>::new(
                "spl_token_owner_index_stats",
            ),
            roots_tracker: RwLock::<RootsTracker>::default(),
            ongoing_scan_roots: RwLock::<BTreeMap<Slot, u64>>::default(),
            removed_bank_ids: Mutex::<HashSet<BankId>>::default(),
        }
    }
}

impl<T: 'static + Clone + IsCached + ZeroLamport + std::marker::Sync + std::marker::Send>
    AccountsIndex<T>
{
    fn iter<R>(&self, range: Option<R>) -> AccountsIndexIterator<T>
    where
        R: RangeBounds<Pubkey>,
    {
        AccountsIndexIterator::new(&self.account_maps, range)
    }

    fn do_checked_scan_accounts<F, R>(
        &self,
        metric_name: &'static str,
        ancestors: &Ancestors,
        scan_bank_id: BankId,
        func: F,
        scan_type: ScanTypes<R>,
    ) -> Result<(), ScanError>
    where
        F: FnMut(&Pubkey, (&T, Slot)),
        R: RangeBounds<Pubkey>,
    {
        {
            let locked_removed_bank_ids = self.removed_bank_ids.lock().unwrap();
            if locked_removed_bank_ids.contains(&scan_bank_id) {
                return Err(ScanError::SlotRemoved {
                    slot: ancestors.max_slot(),
                    bank_id: scan_bank_id,
                });
            }
        }

        let max_root = {
            let mut w_ongoing_scan_roots = self
                // This lock is also grabbed by clean_accounts(), so clean
                // has at most cleaned up to the current `max_root` (since
                // clean only happens *after* BankForks::set_root() which sets
                // the `max_root`)
                .ongoing_scan_roots
                .write()
                .unwrap();
            // `max_root()` grabs a lock while
            // the `ongoing_scan_roots` lock is held,
            // make sure inverse doesn't happen to avoid
            // deadlock
            let max_root = self.max_root();
            *w_ongoing_scan_roots.entry(max_root).or_default() += 1;

            max_root
        };

        // First we show that for any bank `B` that is a descendant of
        // the current `max_root`, it must be true that and `B.ancestors.contains(max_root)`,
        // regardless of the pattern of `squash()` behavior, where `ancestors` is the set
        // of ancestors that is tracked in each bank.
        //
        // Proof: At startup, if starting from a snapshot, generate_index() adds all banks
        // in the snapshot to the index via `add_root()` and so `max_root` will be the
        // greatest of these. Thus, so the claim holds at startup since there are no
        // descendants of `max_root`.
        //
        // Now we proceed by induction on each `BankForks::set_root()`.
        // Assume the claim holds when the `max_root` is `R`. Call the set of
        // descendants of `R` present in BankForks `R_descendants`.
        //
        // Then for any banks `B` in `R_descendants`, it must be that `B.ancestors.contains(S)`,
        // where `S` is any ancestor of `B` such that `S >= R`.
        //
        // For example:
        //          `R` -> `A` -> `C` -> `B`
        // Then `B.ancestors == {R, A, C}`
        //
        // Next we call `BankForks::set_root()` at some descendant of `R`, `R_new`,
        // where `R_new > R`.
        //
        // When we squash `R_new`, `max_root` in the AccountsIndex here is now set to `R_new`,
        // and all nondescendants of `R_new` are pruned.
        //
        // Now consider any outstanding references to banks in the system that are descended from
        // `max_root == R_new`. Take any one of these references and call it `B`. Because `B` is
        // a descendant of `R_new`, this means `B` was also a descendant of `R`. Thus `B`
        // must be a member of `R_descendants` because `B` was constructed and added to
        // BankForks before the `set_root`.
        //
        // This means by the guarantees of `R_descendants` described above, because
        // `R_new` is an ancestor of `B`, and `R < R_new < B`, then `B.ancestors.contains(R_new)`.
        //
        // Now until the next `set_root`, any new banks constructed from `new_from_parent` will
        // also have `max_root == R_new` in their ancestor set, so the claim holds for those descendants
        // as well. Once the next `set_root` happens, we once again update `max_root` and the same
        // inductive argument can be applied again to show the claim holds.

        // Check that the `max_root` is present in `ancestors`. From the proof above, if
        // `max_root` is not present in `ancestors`, this means the bank `B` with the
        // given `ancestors` is not descended from `max_root, which means
        // either:
        // 1) `B` is on a different fork or
        // 2) `B` is an ancestor of `max_root`.
        // In both cases we can ignore the given ancestors and instead just rely on the roots
        // present as `max_root` indicates the roots present in the index are more up to date
        // than the ancestors given.
        let empty = Ancestors::default();
        let ancestors = if ancestors.contains_key(&max_root) {
            ancestors
        } else {
            /*
            This takes of edge cases like:

            Diagram 1:

                        slot 0
                          |
                        slot 1
                      /        \
                 slot 2         |
                    |       slot 3 (max root)
            slot 4 (scan)

            By the time the scan on slot 4 is called, slot 2 may already have been
            cleaned by a clean on slot 3, but slot 4 may not have been cleaned.
            The state in slot 2 would have been purged and is not saved in any roots.
            In this case, a scan on slot 4 wouldn't accurately reflect the state when bank 4
            was frozen. In cases like this, we default to a scan on the latest roots by
            removing all `ancestors`.
            */
            &empty
        };

        /*
        Now there are two cases, either `ancestors` is empty or nonempty:

        1) If ancestors is empty, then this is the same as a scan on a rooted bank,
        and `ongoing_scan_roots` provides protection against cleanup of roots necessary
        for the scan, and  passing `Some(max_root)` to `do_scan_accounts()` ensures newer
        roots don't appear in the scan.

        2) If ancestors is non-empty, then from the `ancestors_contains(&max_root)` above, we know
        that the fork structure must look something like:

        Diagram 2:

                Build fork structure:
                        slot 0
                          |
                    slot 1 (max_root)
                    /            \
             slot 2              |
                |            slot 3 (potential newer max root)
              slot 4
                |
             slot 5 (scan)

        Consider both types of ancestors, ancestor <= `max_root` and
        ancestor > `max_root`, where `max_root == 1` as illustrated above.

        a) The set of `ancestors <= max_root` are all rooted, which means their state
        is protected by the same guarantees as 1).

        b) As for the `ancestors > max_root`, those banks have at least one reference discoverable
        through the chain of `Bank::BankRc::parent` starting from the calling bank. For instance
        bank 5's parent reference keeps bank 4 alive, which will prevent the `Bank::drop()` from
        running and cleaning up bank 4. Furthermore, no cleans can happen past the saved max_root == 1,
        so a potential newer max root at 3 will not clean up any of the ancestors > 1, so slot 4
        will not be cleaned in the middle of the scan either. (NOTE similar reasoning is employed for
        assert!() justification in AccountsDb::retry_to_get_account_accessor)
        */
        match scan_type {
            ScanTypes::Unindexed(range) => {
                // Pass "" not to log metrics, so RPC doesn't get spammy
                self.do_scan_accounts(metric_name, ancestors, func, range, Some(max_root));
            }
            ScanTypes::Indexed(IndexKey::ProgramId(program_id)) => {
                self.do_scan_secondary_index(
                    ancestors,
                    func,
                    &self.program_id_index,
                    &program_id,
                    Some(max_root),
                );
            }
            ScanTypes::Indexed(IndexKey::SplTokenMint(mint_key)) => {
                self.do_scan_secondary_index(
                    ancestors,
                    func,
                    &self.spl_token_mint_index,
                    &mint_key,
                    Some(max_root),
                );
            }
            ScanTypes::Indexed(IndexKey::SplTokenOwner(owner_key)) => {
                self.do_scan_secondary_index(
                    ancestors,
                    func,
                    &self.spl_token_owner_index,
                    &owner_key,
                    Some(max_root),
                );
            }
        }

        {
            let mut ongoing_scan_roots = self.ongoing_scan_roots.write().unwrap();
            let count = ongoing_scan_roots.get_mut(&max_root).unwrap();
            *count -= 1;
            if *count == 0 {
                ongoing_scan_roots.remove(&max_root);
            }
        }

        // If the fork with tip at bank `scan_bank_id` was removed during our scan, then the scan
        // may have been corrupted, so abort the results.
        let was_scan_corrupted = self
            .removed_bank_ids
            .lock()
            .unwrap()
            .contains(&scan_bank_id);

        if was_scan_corrupted {
            Err(ScanError::SlotRemoved {
                slot: ancestors.max_slot(),
                bank_id: scan_bank_id,
            })
        } else {
            Ok(())
        }
    }

    fn do_unchecked_scan_accounts<F, R>(
        &self,
        metric_name: &'static str,
        ancestors: &Ancestors,
        func: F,
        range: Option<R>,
    ) where
        F: FnMut(&Pubkey, (&T, Slot)),
        R: RangeBounds<Pubkey>,
    {
        self.do_scan_accounts(metric_name, ancestors, func, range, None);
    }

    // Scan accounts and return latest version of each account that is either:
    // 1) rooted or
    // 2) present in ancestors
    fn do_scan_accounts<F, R>(
        &self,
        metric_name: &'static str,
        ancestors: &Ancestors,
        mut func: F,
        range: Option<R>,
        max_root: Option<Slot>,
    ) where
        F: FnMut(&Pubkey, (&T, Slot)),
        R: RangeBounds<Pubkey>,
    {
        // TODO: expand to use mint index to find the `pubkey_list` below more efficiently
        // instead of scanning the entire range
        let mut total_elapsed_timer = Measure::start("total");
        let mut num_keys_iterated = 0;
        let mut latest_slot_elapsed = 0;
        let mut load_account_elapsed = 0;
        let mut read_lock_elapsed = 0;
        let mut iterator_elapsed = 0;
        let mut iterator_timer = Measure::start("iterator_elapsed");
        for pubkey_list in self.iter(range) {
            iterator_timer.stop();
            iterator_elapsed += iterator_timer.as_us();
            for (pubkey, list) in pubkey_list {
                num_keys_iterated += 1;
                let mut read_lock_timer = Measure::start("read_lock");
                let list_r = &list.slot_list.read().unwrap();
                read_lock_timer.stop();
                read_lock_elapsed += read_lock_timer.as_us();
                let mut latest_slot_timer = Measure::start("latest_slot");
                if let Some(index) = self.latest_slot(Some(ancestors), list_r, max_root) {
                    latest_slot_timer.stop();
                    latest_slot_elapsed += latest_slot_timer.as_us();
                    let mut load_account_timer = Measure::start("load_account");
                    func(&pubkey, (&list_r[index].1, list_r[index].0));
                    load_account_timer.stop();
                    load_account_elapsed += load_account_timer.as_us();
                }
            }
            iterator_timer = Measure::start("iterator_elapsed");
        }

        total_elapsed_timer.stop();
        if !metric_name.is_empty() {
            datapoint_info!(
                metric_name,
                ("total_elapsed", total_elapsed_timer.as_us(), i64),
                ("latest_slot_elapsed", latest_slot_elapsed, i64),
                ("read_lock_elapsed", read_lock_elapsed, i64),
                ("load_account_elapsed", load_account_elapsed, i64),
                ("iterator_elapsed", iterator_elapsed, i64),
                ("num_keys_iterated", num_keys_iterated, i64),
            )
        }
    }

    fn do_scan_secondary_index<
        F,
        SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send,
    >(
        &self,
        ancestors: &Ancestors,
        mut func: F,
        index: &SecondaryIndex<SecondaryIndexEntryType>,
        index_key: &Pubkey,
        max_root: Option<Slot>,
    ) where
        F: FnMut(&Pubkey, (&T, Slot)),
    {
        for pubkey in index.get(index_key) {
            // Maybe these reads from the AccountsIndex can be batched every time it
            // grabs the read lock as well...
            if let AccountIndexGetResult::Found(list_r, index) =
                self.get(&pubkey, Some(ancestors), max_root)
            {
                func(
                    &pubkey,
                    (&list_r.slot_list()[index].1, list_r.slot_list()[index].0),
                );
            }
        }
    }

    pub fn get_account_read_entry(&self, pubkey: &Pubkey) -> Option<ReadAccountMapEntry<T>> {
        let lock = self.get_account_maps_read_lock(pubkey);
        self.get_account_read_entry_with_lock(pubkey, &lock)
    }

    pub fn get_account_read_entry_with_lock(
        &self,
        pubkey: &Pubkey,
        lock: &AccountMapsReadLock<'_, T>,
    ) -> Option<ReadAccountMapEntry<T>> {
        lock.get(pubkey)
            .cloned()
            .map(ReadAccountMapEntry::from_account_map_entry)
    }

    fn get_account_write_entry(&self, pubkey: &Pubkey) -> Option<WriteAccountMapEntry<T>> {
        self.account_maps[get_bin_pubkey(pubkey)]
            .read()
            .unwrap()
            .get(pubkey)
            .cloned()
            .map(WriteAccountMapEntry::from_account_map_entry)
    }

    fn insert_new_entry_if_missing(
        &self,
        pubkey: &Pubkey,
        slot: Slot,
        info: T,
        w_account_maps: Option<&mut AccountMapsWriteLock<T>>,
    ) -> Option<(WriteAccountMapEntry<T>, T)> {
        let new_entry = WriteAccountMapEntry::new_entry_after_update(slot, info);
        match w_account_maps {
            Some(w_account_maps) => {
                self.insert_new_entry_if_missing_with_lock(*pubkey, w_account_maps, new_entry)
            }
            None => {
                let mut w_account_maps = self.get_account_maps_write_lock(pubkey);
                self.insert_new_entry_if_missing_with_lock(*pubkey, &mut w_account_maps, new_entry)
            }
        }
        .map(|x| (x.0, x.1))
    }

    // return None if item was created new
    // if entry for pubkey already existed, return Some(entry). Caller needs to call entry.update.
    fn insert_new_entry_if_missing_with_lock(
        &self,
        pubkey: Pubkey,
        w_account_maps: &mut AccountMapsWriteLock<T>,
        new_entry: AccountMapEntry<T>,
    ) -> Option<(WriteAccountMapEntry<T>, T, Pubkey)> {
        let account_entry = w_account_maps.entry(pubkey);
        match account_entry {
            Entry::Occupied(account_entry) => Some((
                WriteAccountMapEntry::from_account_map_entry(account_entry.get().clone()),
                // extract the new account_info from the unused 'new_entry'
                new_entry.slot_list.write().unwrap().remove(0).1,
                *account_entry.key(),
            )),
            Entry::Vacant(account_entry) => {
                account_entry.insert(new_entry);
                None
            }
        }
    }

    fn get_account_write_entry_else_create(
        &self,
        pubkey: &Pubkey,
        slot: Slot,
        info: T,
    ) -> Option<(WriteAccountMapEntry<T>, T)> {
        match self.get_account_write_entry(pubkey) {
            Some(w_account_entry) => Some((w_account_entry, info)),
            None => self.insert_new_entry_if_missing(pubkey, slot, info, None),
        }
    }

    pub fn handle_dead_keys(
        &self,
        dead_keys: &[&Pubkey],
        account_indexes: &AccountSecondaryIndexes,
    ) {
        if !dead_keys.is_empty() {
            for key in dead_keys.iter() {
                let mut w_index = self.get_account_maps_write_lock(key);
                if let btree_map::Entry::Occupied(index_entry) = w_index.entry(**key) {
                    if index_entry.get().slot_list.read().unwrap().is_empty() {
                        index_entry.remove();

                        // Note it's only safe to remove all the entries for this key
                        // because we have the lock for this key's entry in the AccountsIndex,
                        // so no other thread is also updating the index
                        self.purge_secondary_indexes_by_inner_key(key, account_indexes);
                    }
                }
            }
        }
    }

    /// call func with every pubkey and index visible from a given set of ancestors
    pub(crate) fn scan_accounts<F>(
        &self,
        ancestors: &Ancestors,
        scan_bank_id: BankId,
        func: F,
    ) -> Result<(), ScanError>
    where
        F: FnMut(&Pubkey, (&T, Slot)),
    {
        // Pass "" not to log metrics, so RPC doesn't get spammy
        self.do_checked_scan_accounts(
            "",
            ancestors,
            scan_bank_id,
            func,
            ScanTypes::Unindexed(None::<Range<Pubkey>>),
        )
    }

    pub(crate) fn unchecked_scan_accounts<F>(
        &self,
        metric_name: &'static str,
        ancestors: &Ancestors,
        func: F,
    ) where
        F: FnMut(&Pubkey, (&T, Slot)),
    {
        self.do_unchecked_scan_accounts(metric_name, ancestors, func, None::<Range<Pubkey>>);
    }

    /// call func with every pubkey and index visible from a given set of ancestors with range
    pub(crate) fn range_scan_accounts<F, R>(
        &self,
        metric_name: &'static str,
        ancestors: &Ancestors,
        range: R,
        func: F,
    ) where
        F: FnMut(&Pubkey, (&T, Slot)),
        R: RangeBounds<Pubkey>,
    {
        // Only the rent logic should be calling this, which doesn't need the safety checks
        self.do_unchecked_scan_accounts(metric_name, ancestors, func, Some(range));
    }

    /// call func with every pubkey and index visible from a given set of ancestors
    pub(crate) fn index_scan_accounts<F>(
        &self,
        ancestors: &Ancestors,
        scan_bank_id: BankId,
        index_key: IndexKey,
        func: F,
    ) -> Result<(), ScanError>
    where
        F: FnMut(&Pubkey, (&T, Slot)),
    {
        // Pass "" not to log metrics, so RPC doesn't get spammy
        self.do_checked_scan_accounts(
            "",
            ancestors,
            scan_bank_id,
            func,
            ScanTypes::<Range<Pubkey>>::Indexed(index_key),
        )
    }

    pub fn get_rooted_entries(&self, slice: SlotSlice<T>, max: Option<Slot>) -> SlotList<T> {
        let max = max.unwrap_or(Slot::MAX);
        let lock = &self.roots_tracker.read().unwrap().roots;
        slice
            .iter()
            .filter(|(slot, _)| *slot <= max && lock.contains(slot))
            .cloned()
            .collect()
    }

    // returns the rooted entries and the storage ref count
    pub fn roots_and_ref_count(
        &self,
        locked_account_entry: &ReadAccountMapEntry<T>,
        max: Option<Slot>,
    ) -> (SlotList<T>, RefCount) {
        (
            self.get_rooted_entries(locked_account_entry.slot_list(), max),
            locked_account_entry.ref_count().load(Ordering::Relaxed),
        )
    }

    pub fn purge_exact<'a, C>(
        &'a self,
        pubkey: &Pubkey,
        slots_to_purge: &'a C,
        reclaims: &mut SlotList<T>,
    ) -> bool
    where
        C: Contains<'a, Slot>,
    {
        if let Some(mut write_account_map_entry) = self.get_account_write_entry(pubkey) {
            write_account_map_entry.slot_list_mut(|slot_list| {
                slot_list.retain(|(slot, item)| {
                    let should_purge = slots_to_purge.contains(slot);
                    if should_purge {
                        reclaims.push((*slot, item.clone()));
                        false
                    } else {
                        true
                    }
                });
                slot_list.is_empty()
            })
        } else {
            true
        }
    }

    pub fn min_ongoing_scan_root(&self) -> Option<Slot> {
        self.ongoing_scan_roots
            .read()
            .unwrap()
            .keys()
            .next()
            .cloned()
    }

    // Given a SlotSlice `L`, a list of ancestors and a maximum slot, find the latest element
    // in `L`, where the slot `S` is an ancestor or root, and if `S` is a root, then `S <= max_root`
    fn latest_slot(
        &self,
        ancestors: Option<&Ancestors>,
        slice: SlotSlice<T>,
        max_root: Option<Slot>,
    ) -> Option<usize> {
        let mut current_max = 0;
        let mut rv = None;
        if let Some(ancestors) = ancestors {
            if !ancestors.is_empty() {
                for (i, (slot, _t)) in slice.iter().rev().enumerate() {
                    if (rv.is_none() || *slot > current_max) && ancestors.contains_key(slot) {
                        rv = Some(i);
                        current_max = *slot;
                    }
                }
            }
        }

        let max_root = max_root.unwrap_or(Slot::MAX);
        let mut tracker = None;

        for (i, (slot, _t)) in slice.iter().rev().enumerate() {
            if (rv.is_none() || *slot > current_max) && *slot <= max_root {
                let lock = match tracker {
                    Some(inner) => inner,
                    None => self.roots_tracker.read().unwrap(),
                };
                if lock.roots.contains(slot) {
                    rv = Some(i);
                    current_max = *slot;
                }
                tracker = Some(lock);
            }
        }

        rv.map(|index| slice.len() - 1 - index)
    }

    /// Get an account
    /// The latest account that appears in `ancestors` or `roots` is returned.
    pub(crate) fn get(
        &self,
        pubkey: &Pubkey,
        ancestors: Option<&Ancestors>,
        max_root: Option<Slot>,
    ) -> AccountIndexGetResult<'_, T> {
        let read_lock = self.account_maps[get_bin_pubkey(pubkey)].read().unwrap();
        let account = read_lock
            .get(pubkey)
            .cloned()
            .map(ReadAccountMapEntry::from_account_map_entry);

        match account {
            Some(locked_entry) => {
                drop(read_lock);
                let slot_list = locked_entry.slot_list();
                let found_index = self.latest_slot(ancestors, slot_list, max_root);
                match found_index {
                    Some(found_index) => AccountIndexGetResult::Found(locked_entry, found_index),
                    None => AccountIndexGetResult::NotFoundOnFork,
                }
            }
            None => AccountIndexGetResult::Missing(read_lock),
        }
    }

    // Get the maximum root <= `max_allowed_root` from the given `slice`
    fn get_newest_root_in_slot_list(
        roots: &RollingBitField,
        slice: SlotSlice<T>,
        max_allowed_root: Option<Slot>,
    ) -> Slot {
        let mut max_root = 0;
        for (f, _) in slice.iter() {
            if let Some(max_allowed_root) = max_allowed_root {
                if *f > max_allowed_root {
                    continue;
                }
            }
            if *f > max_root && roots.contains(f) {
                max_root = *f;
            }
        }
        max_root
    }

    pub(crate) fn update_secondary_indexes(
        &self,
        pubkey: &Pubkey,
        account_owner: &Pubkey,
        account_data: &[u8],
        account_indexes: &AccountSecondaryIndexes,
    ) {
        if account_indexes.is_empty() {
            return;
        }

        if account_indexes.contains(&AccountIndex::ProgramId)
            && account_indexes.include_key(account_owner)
        {
            self.program_id_index.insert(account_owner, pubkey);
        }
        // Note because of the below check below on the account data length, when an
        // account hits zero lamports and is reset to AccountSharedData::Default, then we skip
        // the below updates to the secondary indexes.
        //
        // Skipping means not updating secondary index to mark the account as missing.
        // This doesn't introduce false positives during a scan because the caller to scan
        // provides the ancestors to check. So even if a zero-lamport account is not yet
        // removed from the secondary index, the scan function will:
        // 1) consult the primary index via `get(&pubkey, Some(ancestors), max_root)`
        // and find the zero-lamport version
        // 2) When the fetch from storage occurs, it will return AccountSharedData::Default
        // (as persisted tombstone for snapshots). This will then ultimately be
        // filtered out by post-scan filters, like in `get_filtered_spl_token_accounts_by_owner()`.
        if *account_owner == inline_spl_token_v2_0::id()
            && account_data.len() == inline_spl_token_v2_0::state::Account::get_packed_len()
        {
            if account_indexes.contains(&AccountIndex::SplTokenOwner) {
                let owner_key = Pubkey::new(
                    &account_data[SPL_TOKEN_ACCOUNT_OWNER_OFFSET
                        ..SPL_TOKEN_ACCOUNT_OWNER_OFFSET + PUBKEY_BYTES],
                );
                if account_indexes.include_key(&owner_key) {
                    self.spl_token_owner_index.insert(&owner_key, pubkey);
                }
            }

            if account_indexes.contains(&AccountIndex::SplTokenMint) {
                let mint_key = Pubkey::new(
                    &account_data[SPL_TOKEN_ACCOUNT_MINT_OFFSET
                        ..SPL_TOKEN_ACCOUNT_MINT_OFFSET + PUBKEY_BYTES],
                );
                if account_indexes.include_key(&mint_key) {
                    self.spl_token_mint_index.insert(&mint_key, pubkey);
                }
            }
        }
    }

    fn get_account_maps_write_lock(&self, pubkey: &Pubkey) -> AccountMapsWriteLock<T> {
        self.account_maps[get_bin_pubkey(pubkey)].write().unwrap()
    }

    pub(crate) fn get_account_maps_read_lock(&self, pubkey: &Pubkey) -> AccountMapsReadLock<T> {
        self.account_maps[get_bin_pubkey(pubkey)].read().unwrap()
    }

    // Same functionally to upsert, but:
    // 1. operates on a batch of items
    // 2. holds the write lock for the duration of adding the items
    // Can save time when inserting lots of new keys.
    // But, does NOT update secondary index
    // This is designed to be called at startup time.
    #[allow(clippy::needless_collect)]
    pub(crate) fn insert_new_if_missing_into_primary_index(
        &self,
        slot: Slot,
        item_len: usize,
        items: impl Iterator<Item = (Pubkey, T)>,
    ) -> (Vec<Pubkey>, u64) {
        // big enough so not likely to re-allocate, small enough to not over-allocate by too much
        // this assumes the largest bin contains twice the expected amount of the average size per bin
        let expected_items_per_bin = item_len * 2 / BINS;
        let mut binned = (0..BINS)
            .into_iter()
            .map(|pubkey_bin| (pubkey_bin, Vec::with_capacity(expected_items_per_bin)))
            .collect::<Vec<_>>();
        items.for_each(|(pubkey, account_info)| {
            let bin = get_bin_pubkey(&pubkey);
            // this value is equivalent to what update() below would have created if we inserted a new item
            let info = WriteAccountMapEntry::new_entry_after_update(slot, account_info);
            binned[bin].1.push((pubkey, info));
        });
        binned.retain(|x| !x.1.is_empty());

        let insertion_time = AtomicU64::new(0);

        let duplicate_keys = binned
            .into_iter()
            .map(|(pubkey_bin, items)| {
                let mut _reclaims = SlotList::new();

                // big enough so not likely to re-allocate, small enough to not over-allocate by too much
                // this assumes 10% of keys are duplicates. This vector will be flattened below.
                let mut duplicate_keys = Vec::with_capacity(items.len() / 10);
                let mut w_account_maps = self.account_maps[pubkey_bin].write().unwrap();
                let mut insert_time = Measure::start("insert_into_primary_index");
                items.into_iter().for_each(|(pubkey, new_item)| {
                    let already_exists = self.insert_new_entry_if_missing_with_lock(
                        pubkey,
                        &mut w_account_maps,
                        new_item,
                    );
                    if let Some((mut w_account_entry, account_info, pubkey)) = already_exists {
                        w_account_entry.update(slot, account_info, &mut _reclaims);
                        duplicate_keys.push(pubkey);
                    }
                });
                insert_time.stop();
                insertion_time.fetch_add(insert_time.as_us(), Ordering::Relaxed);
                duplicate_keys
            })
            .flatten()
            .collect::<Vec<_>>();

        (duplicate_keys, insertion_time.load(Ordering::Relaxed))
    }

    // Updates the given pubkey at the given slot with the new account information.
    // Returns true if the pubkey was newly inserted into the index, otherwise, if the
    // pubkey updates an existing entry in the index, returns false.
    pub fn upsert(
        &self,
        slot: Slot,
        pubkey: &Pubkey,
        account_owner: &Pubkey,
        account_data: &[u8],
        account_indexes: &AccountSecondaryIndexes,
        account_info: T,
        reclaims: &mut SlotList<T>,
    ) -> bool {
        let is_newly_inserted = {
            // We don't atomically update both primary index and secondary index together.
            // This certainly creates small time window with inconsistent state across the two indexes.
            // However, this is acceptable because:
            //
            //  - A strict consistent view at any given moment of time is not necessary, because the only
            //  use case for the secondary index is `scan`, and `scans` are only supported/require consistency
            //  on frozen banks, and this inconsistency is only possible on working banks.
            //
            //  - The secondary index is never consulted as primary source of truth for gets/stores.
            //  So, what the accounts_index sees alone is sufficient as a source of truth for other non-scan
            //  account operations.
            if let Some((mut w_account_entry, account_info)) =
                self.get_account_write_entry_else_create(pubkey, slot, account_info)
            {
                w_account_entry.update(slot, account_info, reclaims);
                false
            } else {
                true
            }
        };
        self.update_secondary_indexes(pubkey, account_owner, account_data, account_indexes);
        is_newly_inserted
    }

    pub fn unref_from_storage(&self, pubkey: &Pubkey) {
        if let Some(locked_entry) = self.get_account_read_entry(pubkey) {
            locked_entry.unref();
        }
    }

    pub fn ref_count_from_storage(&self, pubkey: &Pubkey) -> RefCount {
        if let Some(locked_entry) = self.get_account_read_entry(pubkey) {
            locked_entry.ref_count().load(Ordering::Relaxed)
        } else {
            0
        }
    }

    fn purge_secondary_indexes_by_inner_key<'a>(
        &'a self,
        inner_key: &Pubkey,
        account_indexes: &AccountSecondaryIndexes,
    ) {
        if account_indexes.contains(&AccountIndex::ProgramId) {
            self.program_id_index.remove_by_inner_key(inner_key);
        }

        if account_indexes.contains(&AccountIndex::SplTokenOwner) {
            self.spl_token_owner_index.remove_by_inner_key(inner_key);
        }

        if account_indexes.contains(&AccountIndex::SplTokenMint) {
            self.spl_token_mint_index.remove_by_inner_key(inner_key);
        }
    }

    fn purge_older_root_entries(
        &self,
        slot_list: &mut SlotList<T>,
        reclaims: &mut SlotList<T>,
        max_clean_root: Option<Slot>,
    ) {
        let roots_tracker = &self.roots_tracker.read().unwrap();
        let newest_root_in_slot_list =
            Self::get_newest_root_in_slot_list(&roots_tracker.roots, slot_list, max_clean_root);
        let max_clean_root = max_clean_root.unwrap_or(roots_tracker.max_root);

        slot_list.retain(|(slot, value)| {
            let should_purge =
                Self::can_purge_older_entries(max_clean_root, newest_root_in_slot_list, *slot)
                    && !value.is_cached();
            if should_purge {
                reclaims.push((*slot, value.clone()));
            }
            !should_purge
        });
    }

    pub fn clean_rooted_entries(
        &self,
        pubkey: &Pubkey,
        reclaims: &mut SlotList<T>,
        max_clean_root: Option<Slot>,
    ) {
        let mut is_slot_list_empty = false;
        if let Some(mut locked_entry) = self.get_account_write_entry(pubkey) {
            locked_entry.slot_list_mut(|slot_list| {
                self.purge_older_root_entries(slot_list, reclaims, max_clean_root);
                is_slot_list_empty = slot_list.is_empty();
            });
        }

        // If the slot list is empty, remove the pubkey from `account_maps`.  Make sure to grab the
        // lock and double check the slot list is still empty, because another writer could have
        // locked and inserted the pubkey inbetween when `is_slot_list_empty=true` and the call to
        // remove() below.
        if is_slot_list_empty {
            let mut w_maps = self.get_account_maps_write_lock(pubkey);
            if let Some(x) = w_maps.get(pubkey) {
                if x.slot_list.read().unwrap().is_empty() {
                    w_maps.remove(pubkey);
                }
            }
        }
    }

    /// When can an entry be purged?
    ///
    /// If we get a slot update where slot != newest_root_in_slot_list for an account where slot <
    /// max_clean_root, then we know it's safe to delete because:
    ///
    /// a) If slot < newest_root_in_slot_list, then we know the update is outdated by a later rooted
    /// update, namely the one in newest_root_in_slot_list
    ///
    /// b) If slot > newest_root_in_slot_list, then because slot < max_clean_root and we know there are
    /// no roots in the slot list between newest_root_in_slot_list and max_clean_root, (otherwise there
    /// would be a bigger newest_root_in_slot_list, which is a contradiction), then we know slot must be
    /// an unrooted slot less than max_clean_root and thus safe to clean as well.
    fn can_purge_older_entries(
        max_clean_root: Slot,
        newest_root_in_slot_list: Slot,
        slot: Slot,
    ) -> bool {
        slot < max_clean_root && slot != newest_root_in_slot_list
    }

    /// Given a list of slots, return a new list of only the slots that are rooted
    pub fn get_rooted_from_list<'a>(&self, slots: impl Iterator<Item = &'a Slot>) -> Vec<Slot> {
        let roots_tracker = self.roots_tracker.read().unwrap();
        slots
            .filter_map(|s| {
                if roots_tracker.roots.contains(s) {
                    Some(*s)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn is_root(&self, slot: Slot) -> bool {
        self.roots_tracker.read().unwrap().roots.contains(&slot)
    }

    pub fn add_root(&self, slot: Slot, caching_enabled: bool) {
        let mut w_roots_tracker = self.roots_tracker.write().unwrap();
        w_roots_tracker.roots.insert(slot);
        // we delay cleaning until flushing!
        if !caching_enabled {
            w_roots_tracker.uncleaned_roots.insert(slot);
        }
        // `AccountsDb::flush_accounts_cache()` relies on roots being added in order
        assert!(slot >= w_roots_tracker.max_root);
        w_roots_tracker.max_root = slot;
    }

    pub fn add_uncleaned_roots<I>(&self, roots: I)
    where
        I: IntoIterator<Item = Slot>,
    {
        let mut w_roots_tracker = self.roots_tracker.write().unwrap();
        w_roots_tracker.uncleaned_roots.extend(roots);
    }

    pub fn max_root(&self) -> Slot {
        self.roots_tracker.read().unwrap().max_root
    }

    /// Remove the slot when the storage for the slot is freed
    /// Accounts no longer reference this slot.
    pub fn clean_dead_slot(&self, slot: Slot) -> Option<AccountsIndexRootsStats> {
        let (roots_len, uncleaned_roots_len, previous_uncleaned_roots_len, roots_range) = {
            let mut w_roots_tracker = self.roots_tracker.write().unwrap();
            let removed_from_unclean_roots = w_roots_tracker.uncleaned_roots.remove(&slot);
            let removed_from_previous_uncleaned_roots =
                w_roots_tracker.previous_uncleaned_roots.remove(&slot);
            if !w_roots_tracker.roots.remove(&slot) {
                if removed_from_unclean_roots {
                    error!("clean_dead_slot-removed_from_unclean_roots: {}", slot);
                    inc_new_counter_error!("clean_dead_slot-removed_from_unclean_roots", 1, 1);
                }
                if removed_from_previous_uncleaned_roots {
                    error!(
                        "clean_dead_slot-removed_from_previous_uncleaned_roots: {}",
                        slot
                    );
                    inc_new_counter_error!(
                        "clean_dead_slot-removed_from_previous_uncleaned_roots",
                        1,
                        1
                    );
                }
                return None;
            }
            (
                w_roots_tracker.roots.len(),
                w_roots_tracker.uncleaned_roots.len(),
                w_roots_tracker.previous_uncleaned_roots.len(),
                w_roots_tracker.roots.range_width(),
            )
        };
        Some(AccountsIndexRootsStats {
            roots_len,
            uncleaned_roots_len,
            previous_uncleaned_roots_len,
            roots_range,
            rooted_cleaned_count: 0,
            unrooted_cleaned_count: 0,
        })
    }

    pub fn min_root(&self) -> Option<Slot> {
        self.roots_tracker.read().unwrap().min_root()
    }

    pub fn reset_uncleaned_roots(&self, max_clean_root: Option<Slot>) -> HashSet<Slot> {
        let mut cleaned_roots = HashSet::new();
        let mut w_roots_tracker = self.roots_tracker.write().unwrap();
        w_roots_tracker.uncleaned_roots.retain(|root| {
            let is_cleaned = max_clean_root
                .map(|max_clean_root| *root <= max_clean_root)
                .unwrap_or(true);
            if is_cleaned {
                cleaned_roots.insert(*root);
            }
            // Only keep the slots that have yet to be cleaned
            !is_cleaned
        });
        std::mem::replace(&mut w_roots_tracker.previous_uncleaned_roots, cleaned_roots)
    }

    #[cfg(test)]
    pub fn clear_uncleaned_roots(&self, max_clean_root: Option<Slot>) -> HashSet<Slot> {
        let mut cleaned_roots = HashSet::new();
        let mut w_roots_tracker = self.roots_tracker.write().unwrap();
        w_roots_tracker.uncleaned_roots.retain(|root| {
            let is_cleaned = max_clean_root
                .map(|max_clean_root| *root <= max_clean_root)
                .unwrap_or(true);
            if is_cleaned {
                cleaned_roots.insert(*root);
            }
            // Only keep the slots that have yet to be cleaned
            !is_cleaned
        });
        cleaned_roots
    }

    pub fn is_uncleaned_root(&self, slot: Slot) -> bool {
        self.roots_tracker
            .read()
            .unwrap()
            .uncleaned_roots
            .contains(&slot)
    }

    pub fn num_roots(&self) -> usize {
        self.roots_tracker.read().unwrap().roots.len()
    }

    pub fn all_roots(&self) -> Vec<Slot> {
        let tracker = self.roots_tracker.read().unwrap();
        tracker.roots.get_all()
    }

    #[cfg(test)]
    pub fn clear_roots(&self) {
        self.roots_tracker.write().unwrap().roots.clear()
    }

    #[cfg(test)]
    pub fn uncleaned_roots_len(&self) -> usize {
        self.roots_tracker.read().unwrap().uncleaned_roots.len()
    }

    #[cfg(test)]
    // filter any rooted entries and return them along with a bool that indicates
    // if this account has no more entries. Note this does not update the secondary
    // indexes!
    pub fn purge_roots(&self, pubkey: &Pubkey) -> (SlotList<T>, bool) {
        let mut write_account_map_entry = self.get_account_write_entry(pubkey).unwrap();
        write_account_map_entry.slot_list_mut(|slot_list| {
            let reclaims = self.get_rooted_entries(slot_list, None);
            slot_list.retain(|(slot, _)| !self.is_root(*slot));
            (reclaims, slot_list.is_empty())
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use solana_sdk::signature::{Keypair, Signer};

    pub enum SecondaryIndexTypes<'a> {
        RwLock(&'a SecondaryIndex<RwLockSecondaryIndexEntry>),
        DashMap(&'a SecondaryIndex<DashMapSecondaryIndexEntry>),
    }

    pub fn spl_token_mint_index_enabled() -> AccountSecondaryIndexes {
        let mut account_indexes = HashSet::new();
        account_indexes.insert(AccountIndex::SplTokenMint);
        AccountSecondaryIndexes {
            indexes: account_indexes,
            keys: None,
        }
    }

    pub fn spl_token_owner_index_enabled() -> AccountSecondaryIndexes {
        let mut account_indexes = HashSet::new();
        account_indexes.insert(AccountIndex::SplTokenOwner);
        AccountSecondaryIndexes {
            indexes: account_indexes,
            keys: None,
        }
    }

    impl<'a, T: 'static> AccountIndexGetResult<'a, T> {
        pub fn unwrap(self) -> (ReadAccountMapEntry<T>, usize) {
            match self {
                AccountIndexGetResult::Found(lock, size) => (lock, size),
                _ => {
                    panic!("trying to unwrap AccountIndexGetResult with non-Success result");
                }
            }
        }

        pub fn is_none(&self) -> bool {
            !self.is_some()
        }

        pub fn is_some(&self) -> bool {
            matches!(self, AccountIndexGetResult::Found(_lock, _size))
        }

        pub fn map<V, F: FnOnce((ReadAccountMapEntry<T>, usize)) -> V>(self, f: F) -> Option<V> {
            match self {
                AccountIndexGetResult::Found(lock, size) => Some(f((lock, size))),
                _ => None,
            }
        }
    }

    fn create_dashmap_secondary_index_state() -> (usize, usize, AccountSecondaryIndexes) {
        {
            // Check that we're actually testing the correct variant
            let index = AccountsIndex::<bool>::default();
            let _type_check = SecondaryIndexTypes::DashMap(&index.spl_token_mint_index);
        }

        (0, PUBKEY_BYTES, spl_token_mint_index_enabled())
    }

    fn create_rwlock_secondary_index_state() -> (usize, usize, AccountSecondaryIndexes) {
        {
            // Check that we're actually testing the correct variant
            let index = AccountsIndex::<bool>::default();
            let _type_check = SecondaryIndexTypes::RwLock(&index.spl_token_owner_index);
        }

        (
            SPL_TOKEN_ACCOUNT_OWNER_OFFSET,
            SPL_TOKEN_ACCOUNT_OWNER_OFFSET + PUBKEY_BYTES,
            spl_token_owner_index_enabled(),
        )
    }

    #[test]
    fn test_bitfield_delete_non_excess() {
        solana_logger::setup();
        let len = 16;
        let mut bitfield = RollingBitField::new(len);
        assert_eq!(bitfield.min(), None);

        bitfield.insert(0);
        assert_eq!(bitfield.min(), Some(0));
        let too_big = len + 1;
        bitfield.insert(too_big);
        assert!(bitfield.contains(&0));
        assert!(bitfield.contains(&too_big));
        assert_eq!(bitfield.len(), 2);
        assert_eq!(bitfield.excess.len(), 1);
        assert_eq!(bitfield.min, too_big);
        assert_eq!(bitfield.min(), Some(0));
        assert_eq!(bitfield.max, too_big + 1);

        // delete the thing that is NOT in excess
        bitfield.remove(&too_big);
        assert_eq!(bitfield.min, too_big + 1);
        assert_eq!(bitfield.max, too_big + 1);
        let too_big_times_2 = too_big * 2;
        bitfield.insert(too_big_times_2);
        assert!(bitfield.contains(&0));
        assert!(bitfield.contains(&too_big_times_2));
        assert_eq!(bitfield.len(), 2);
        assert_eq!(bitfield.excess.len(), 1);
        assert_eq!(bitfield.min(), bitfield.excess.iter().min().copied());
        assert_eq!(bitfield.min, too_big_times_2);
        assert_eq!(bitfield.max, too_big_times_2 + 1);

        bitfield.remove(&0);
        bitfield.remove(&too_big_times_2);
        assert!(bitfield.is_empty());
        let other = 5;
        bitfield.insert(other);
        assert!(bitfield.contains(&other));
        assert!(bitfield.excess.is_empty());
        assert_eq!(bitfield.min, other);
        assert_eq!(bitfield.max, other + 1);
    }

    #[test]
    fn test_bitfield_insert_excess() {
        solana_logger::setup();
        let len = 16;
        let mut bitfield = RollingBitField::new(len);

        bitfield.insert(0);
        let too_big = len + 1;
        bitfield.insert(too_big);
        assert!(bitfield.contains(&0));
        assert!(bitfield.contains(&too_big));
        assert_eq!(bitfield.len(), 2);
        assert_eq!(bitfield.excess.len(), 1);
        assert!(bitfield.excess.contains(&0));
        assert_eq!(bitfield.min, too_big);
        assert_eq!(bitfield.max, too_big + 1);

        // delete the thing that IS in excess
        // this does NOT affect min/max
        bitfield.remove(&0);
        assert_eq!(bitfield.min, too_big);
        assert_eq!(bitfield.max, too_big + 1);
        // re-add to excess
        bitfield.insert(0);
        assert!(bitfield.contains(&0));
        assert!(bitfield.contains(&too_big));
        assert_eq!(bitfield.len(), 2);
        assert_eq!(bitfield.excess.len(), 1);
        assert_eq!(bitfield.min, too_big);
        assert_eq!(bitfield.max, too_big + 1);
    }

    #[test]
    fn test_bitfield_permutations() {
        solana_logger::setup();
        let mut bitfield = RollingBitField::new(2097152);
        let mut hash = HashSet::new();

        let min = 101_000;
        let width = 400_000;
        let dead = 19;

        let mut slot = min;
        while hash.len() < width {
            slot += 1;
            if slot % dead == 0 {
                continue;
            }
            hash.insert(slot);
            bitfield.insert(slot);
        }
        compare(&hash, &bitfield);

        let max = slot + 1;

        let mut time = Measure::start("");
        let mut count = 0;
        for slot in (min - 10)..max + 100 {
            if hash.contains(&slot) {
                count += 1;
            }
        }
        time.stop();

        let mut time2 = Measure::start("");
        let mut count2 = 0;
        for slot in (min - 10)..max + 100 {
            if bitfield.contains(&slot) {
                count2 += 1;
            }
        }
        time2.stop();
        info!(
            "{}ms, {}ms, {} ratio",
            time.as_ms(),
            time2.as_ms(),
            time.as_ns() / time2.as_ns()
        );
        assert_eq!(count, count2);
    }

    #[test]
    #[should_panic(expected = "assertion failed: max_width.is_power_of_two()")]
    fn test_bitfield_power_2() {
        let _ = RollingBitField::new(3);
    }

    #[test]
    #[should_panic(expected = "assertion failed: max_width > 0")]
    fn test_bitfield_0() {
        let _ = RollingBitField::new(0);
    }

    fn setup_empty(width: u64) -> RollingBitFieldTester {
        let bitfield = RollingBitField::new(width);
        let hash_set = HashSet::new();
        RollingBitFieldTester { bitfield, hash_set }
    }

    struct RollingBitFieldTester {
        pub bitfield: RollingBitField,
        pub hash_set: HashSet<u64>,
    }

    impl RollingBitFieldTester {
        fn insert(&mut self, slot: u64) {
            self.bitfield.insert(slot);
            self.hash_set.insert(slot);
            assert!(self.bitfield.contains(&slot));
            compare(&self.hash_set, &self.bitfield);
        }
        fn remove(&mut self, slot: &u64) -> bool {
            let result = self.bitfield.remove(slot);
            assert_eq!(result, self.hash_set.remove(slot));
            assert!(!self.bitfield.contains(slot));
            self.compare();
            result
        }
        fn compare(&self) {
            compare(&self.hash_set, &self.bitfield);
        }
    }

    fn setup_wide(width: u64, start: u64) -> RollingBitFieldTester {
        let mut tester = setup_empty(width);

        tester.compare();
        tester.insert(start);
        tester.insert(start + 1);
        tester
    }

    #[test]
    fn test_bitfield_insert_wide() {
        solana_logger::setup();
        let width = 16;
        let start = 0;
        let mut tester = setup_wide(width, start);

        let slot = start + width;
        let all = tester.bitfield.get_all();
        // higher than max range by 1
        tester.insert(slot);
        let bitfield = tester.bitfield;
        for slot in all {
            assert!(bitfield.contains(&slot));
        }
        assert_eq!(bitfield.excess.len(), 1);
        assert_eq!(bitfield.count, 3);
    }

    #[test]
    fn test_bitfield_insert_wide_before() {
        solana_logger::setup();
        let width = 16;
        let start = 100;
        let mut bitfield = setup_wide(width, start).bitfield;

        let slot = start + 1 - width;
        // assert here - would make min too low, causing too wide of a range
        bitfield.insert(slot);
        assert_eq!(1, bitfield.excess.len());
        assert_eq!(3, bitfield.count);
        assert!(bitfield.contains(&slot));
    }

    #[test]
    fn test_bitfield_insert_wide_before_ok() {
        solana_logger::setup();
        let width = 16;
        let start = 100;
        let mut bitfield = setup_wide(width, start).bitfield;

        let slot = start + 2 - width; // this item would make our width exactly equal to what is allowed, but it is also inserting prior to min
        bitfield.insert(slot);
        assert_eq!(1, bitfield.excess.len());
        assert!(bitfield.contains(&slot));
        assert_eq!(3, bitfield.count);
    }

    #[test]
    fn test_bitfield_contains_wide_no_assert() {
        {
            let width = 16;
            let start = 0;
            let bitfield = setup_wide(width, start).bitfield;

            let mut slot = width;
            assert!(!bitfield.contains(&slot));
            slot += 1;
            assert!(!bitfield.contains(&slot));
        }
        {
            let width = 16;
            let start = 100;
            let bitfield = setup_wide(width, start).bitfield;

            // too large
            let mut slot = width;
            assert!(!bitfield.contains(&slot));
            slot += 1;
            assert!(!bitfield.contains(&slot));
            // too small, before min
            slot = 0;
            assert!(!bitfield.contains(&slot));
        }
    }

    #[test]
    fn test_bitfield_remove_wide() {
        let width = 16;
        let start = 0;
        let mut tester = setup_wide(width, start);
        let slot = width;
        assert!(!tester.remove(&slot));
    }

    #[test]
    fn test_bitfield_excess2() {
        solana_logger::setup();
        let width = 16;
        let mut tester = setup_empty(width);
        let slot = 100;
        // insert 1st slot
        tester.insert(slot);
        assert!(tester.bitfield.excess.is_empty());

        // insert a slot before the previous one. this is 'excess' since we don't use this pattern in normal operation
        let slot2 = slot - 1;
        tester.insert(slot2);
        assert_eq!(tester.bitfield.excess.len(), 1);

        // remove the 1st slot. we will be left with only excess
        tester.remove(&slot);
        assert!(tester.bitfield.contains(&slot2));
        assert_eq!(tester.bitfield.excess.len(), 1);

        // re-insert at valid range, making sure we don't insert into excess
        tester.insert(slot);
        assert_eq!(tester.bitfield.excess.len(), 1);

        // remove the excess slot.
        tester.remove(&slot2);
        assert!(tester.bitfield.contains(&slot));
        assert!(tester.bitfield.excess.is_empty());

        // re-insert the excess slot
        tester.insert(slot2);
        assert_eq!(tester.bitfield.excess.len(), 1);
    }

    #[test]
    fn test_bitfield_excess() {
        solana_logger::setup();
        // start at slot 0 or a separate, higher slot
        for width in [16, 4194304].iter() {
            let width = *width;
            let mut tester = setup_empty(width);
            for start in [0, width * 5].iter().cloned() {
                // recreate means create empty bitfield with each iteration, otherwise re-use
                for recreate in [false, true].iter().cloned() {
                    let max = start + 3;
                    // first root to add
                    for slot in start..max {
                        // subsequent roots to add
                        for slot2 in (slot + 1)..max {
                            // reverse_slots = 1 means add slots in reverse order (max to min). This causes us to add second and later slots to excess.
                            for reverse_slots in [false, true].iter().cloned() {
                                let maybe_reverse = |slot| {
                                    if reverse_slots {
                                        max - slot
                                    } else {
                                        slot
                                    }
                                };
                                if recreate {
                                    let recreated = setup_empty(width);
                                    tester = recreated;
                                }

                                // insert
                                for slot in slot..=slot2 {
                                    let slot_use = maybe_reverse(slot);
                                    tester.insert(slot_use);
                                    debug!(
                                    "slot: {}, bitfield: {:?}, reverse: {}, len: {}, excess: {:?}",
                                    slot_use,
                                    tester.bitfield,
                                    reverse_slots,
                                    tester.bitfield.len(),
                                    tester.bitfield.excess
                                );
                                    assert!(
                                        (reverse_slots && tester.bitfield.len() > 1)
                                            ^ tester.bitfield.excess.is_empty()
                                    );
                                }
                                if start > width * 2 {
                                    assert!(!tester.bitfield.contains(&(start - width * 2)));
                                }
                                assert!(!tester.bitfield.contains(&(start + width * 2)));
                                let len = (slot2 - slot + 1) as usize;
                                assert_eq!(tester.bitfield.len(), len);
                                assert_eq!(tester.bitfield.count, len);

                                // remove
                                for slot in slot..=slot2 {
                                    let slot_use = maybe_reverse(slot);
                                    assert!(tester.remove(&slot_use));
                                    assert!(
                                        (reverse_slots && !tester.bitfield.is_empty())
                                            ^ tester.bitfield.excess.is_empty()
                                    );
                                }
                                assert!(tester.bitfield.is_empty());
                                assert_eq!(tester.bitfield.count, 0);
                                if start > width * 2 {
                                    assert!(!tester.bitfield.contains(&(start - width * 2)));
                                }
                                assert!(!tester.bitfield.contains(&(start + width * 2)));
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_bitfield_remove_wide_before() {
        let width = 16;
        let start = 100;
        let mut tester = setup_wide(width, start);
        let slot = start + 1 - width;
        assert!(!tester.remove(&slot));
    }

    fn compare_internal(hashset: &HashSet<u64>, bitfield: &RollingBitField) {
        assert_eq!(hashset.len(), bitfield.len());
        assert_eq!(hashset.is_empty(), bitfield.is_empty());
        if !bitfield.is_empty() {
            let mut min = Slot::MAX;
            let mut overall_min = Slot::MAX;
            let mut max = Slot::MIN;
            for item in bitfield.get_all() {
                assert!(hashset.contains(&item));
                if !bitfield.excess.contains(&item) {
                    min = std::cmp::min(min, item);
                    max = std::cmp::max(max, item);
                }
                overall_min = std::cmp::min(overall_min, item);
            }
            assert_eq!(bitfield.min(), Some(overall_min));
            assert_eq!(bitfield.get_all().len(), hashset.len());
            // range isn't tracked for excess items
            if bitfield.excess.len() != bitfield.len() {
                let width = if bitfield.is_empty() {
                    0
                } else {
                    max + 1 - min
                };
                assert!(
                    bitfield.range_width() >= width,
                    "hashset: {:?}, bitfield: {:?}, bitfield.range_width: {}, width: {}",
                    hashset,
                    bitfield.get_all(),
                    bitfield.range_width(),
                    width,
                );
            }
        } else {
            assert_eq!(bitfield.min(), None);
        }
    }

    fn compare(hashset: &HashSet<u64>, bitfield: &RollingBitField) {
        compare_internal(hashset, bitfield);
        let clone = bitfield.clone();
        compare_internal(hashset, &clone);
        assert!(clone.eq(bitfield));
        assert_eq!(clone, *bitfield);
    }

    #[test]
    fn test_bitfield_functionality() {
        solana_logger::setup();

        // bitfield sizes are powers of 2, cycle through values of 1, 2, 4, .. 2^9
        for power in 0..10 {
            let max_bitfield_width = 2u64.pow(power) as u64;
            let width_iteration_max = if max_bitfield_width > 1 {
                // add up to 2 items so we can test out multiple items
                3
            } else {
                // 0 or 1 items is all we can fit with a width of 1 item
                2
            };
            for width in 0..width_iteration_max {
                let mut tester = setup_empty(max_bitfield_width);

                let min = 101_000;
                let dead = 19;

                let mut slot = min;
                while tester.hash_set.len() < width {
                    slot += 1;
                    if max_bitfield_width > 2 && slot % dead == 0 {
                        // with max_bitfield_width of 1 and 2, there is no room for dead slots
                        continue;
                    }
                    tester.insert(slot);
                }
                let max = slot + 1;

                for slot in (min - 10)..max + 100 {
                    assert_eq!(
                        tester.bitfield.contains(&slot),
                        tester.hash_set.contains(&slot)
                    );
                }

                if width > 0 {
                    assert!(tester.remove(&slot));
                    assert!(!tester.remove(&slot));
                }

                let all = tester.bitfield.get_all();

                // remove the rest, including a call that removes slot again
                for item in all.iter() {
                    assert!(tester.remove(item));
                    assert!(!tester.remove(item));
                }

                let min = max + ((width * 2) as u64) + 3;
                let slot = min; // several widths past previous min
                let max = slot + 1;
                tester.insert(slot);

                for slot in (min - 10)..max + 100 {
                    assert_eq!(
                        tester.bitfield.contains(&slot),
                        tester.hash_set.contains(&slot)
                    );
                }
            }
        }
    }

    fn bitfield_insert_and_test(bitfield: &mut RollingBitField, slot: Slot) {
        let len = bitfield.len();
        let old_all = bitfield.get_all();
        let (new_min, new_max) = if bitfield.is_empty() {
            (slot, slot + 1)
        } else {
            (
                std::cmp::min(bitfield.min, slot),
                std::cmp::max(bitfield.max, slot + 1),
            )
        };
        bitfield.insert(slot);
        assert_eq!(bitfield.min, new_min);
        assert_eq!(bitfield.max, new_max);
        assert_eq!(bitfield.len(), len + 1);
        assert!(!bitfield.is_empty());
        assert!(bitfield.contains(&slot));
        // verify aliasing is what we expect
        assert!(bitfield.contains_assume_in_range(&(slot + bitfield.max_width)));
        let get_all = bitfield.get_all();
        old_all
            .into_iter()
            .for_each(|slot| assert!(get_all.contains(&slot)));
        assert!(get_all.contains(&slot));
        assert!(get_all.len() == len + 1);
    }

    #[test]
    fn test_bitfield_clear() {
        let mut bitfield = RollingBitField::new(4);
        assert_eq!(bitfield.len(), 0);
        assert!(bitfield.is_empty());
        bitfield_insert_and_test(&mut bitfield, 0);
        bitfield.clear();
        assert_eq!(bitfield.len(), 0);
        assert!(bitfield.is_empty());
        assert!(bitfield.get_all().is_empty());
        bitfield_insert_and_test(&mut bitfield, 1);
        bitfield.clear();
        assert_eq!(bitfield.len(), 0);
        assert!(bitfield.is_empty());
        assert!(bitfield.get_all().is_empty());
        bitfield_insert_and_test(&mut bitfield, 4);
    }

    #[test]
    fn test_bitfield_wrapping() {
        let mut bitfield = RollingBitField::new(4);
        assert_eq!(bitfield.len(), 0);
        assert!(bitfield.is_empty());
        bitfield_insert_and_test(&mut bitfield, 0);
        assert_eq!(bitfield.get_all(), vec![0]);
        bitfield_insert_and_test(&mut bitfield, 2);
        assert_eq!(bitfield.get_all(), vec![0, 2]);
        bitfield_insert_and_test(&mut bitfield, 3);
        bitfield.insert(3); // redundant insert
        assert_eq!(bitfield.get_all(), vec![0, 2, 3]);
        assert!(bitfield.remove(&0));
        assert!(!bitfield.remove(&0));
        assert_eq!(bitfield.min, 2);
        assert_eq!(bitfield.max, 4);
        assert_eq!(bitfield.len(), 2);
        assert!(!bitfield.remove(&0)); // redundant remove
        assert_eq!(bitfield.len(), 2);
        assert_eq!(bitfield.get_all(), vec![2, 3]);
        bitfield.insert(4); // wrapped around value - same bit as '0'
        assert_eq!(bitfield.min, 2);
        assert_eq!(bitfield.max, 5);
        assert_eq!(bitfield.len(), 3);
        assert_eq!(bitfield.get_all(), vec![2, 3, 4]);
        assert!(bitfield.remove(&2));
        assert_eq!(bitfield.min, 3);
        assert_eq!(bitfield.max, 5);
        assert_eq!(bitfield.len(), 2);
        assert_eq!(bitfield.get_all(), vec![3, 4]);
        assert!(bitfield.remove(&3));
        assert_eq!(bitfield.min, 4);
        assert_eq!(bitfield.max, 5);
        assert_eq!(bitfield.len(), 1);
        assert_eq!(bitfield.get_all(), vec![4]);
        assert!(bitfield.remove(&4));
        assert_eq!(bitfield.len(), 0);
        assert!(bitfield.is_empty());
        assert!(bitfield.get_all().is_empty());
        bitfield_insert_and_test(&mut bitfield, 8);
        assert!(bitfield.remove(&8));
        assert_eq!(bitfield.len(), 0);
        assert!(bitfield.is_empty());
        assert!(bitfield.get_all().is_empty());
        bitfield_insert_and_test(&mut bitfield, 9);
        assert!(bitfield.remove(&9));
        assert_eq!(bitfield.len(), 0);
        assert!(bitfield.is_empty());
        assert!(bitfield.get_all().is_empty());
    }

    #[test]
    fn test_bitfield_smaller() {
        // smaller bitfield, fewer entries, including 0
        solana_logger::setup();

        for width in 0..34 {
            let mut bitfield = RollingBitField::new(4096);
            let mut hash_set = HashSet::new();

            let min = 1_010_000;
            let dead = 19;

            let mut slot = min;
            while hash_set.len() < width {
                slot += 1;
                if slot % dead == 0 {
                    continue;
                }
                hash_set.insert(slot);
                bitfield.insert(slot);
            }

            let max = slot + 1;

            let mut time = Measure::start("");
            let mut count = 0;
            for slot in (min - 10)..max + 100 {
                if hash_set.contains(&slot) {
                    count += 1;
                }
            }
            time.stop();

            let mut time2 = Measure::start("");
            let mut count2 = 0;
            for slot in (min - 10)..max + 100 {
                if bitfield.contains(&slot) {
                    count2 += 1;
                }
            }
            time2.stop();
            info!(
                "{}, {}, {}",
                time.as_ms(),
                time2.as_ms(),
                time.as_ns() / time2.as_ns()
            );
            assert_eq!(count, count2);
        }
    }

    #[test]
    fn test_get_empty() {
        let key = Keypair::new();
        let index = AccountsIndex::<bool>::default();
        let ancestors = Ancestors::default();
        assert!(index.get(&key.pubkey(), Some(&ancestors), None).is_none());
        assert!(index.get(&key.pubkey(), None, None).is_none());

        let mut num = 0;
        index.unchecked_scan_accounts("", &ancestors, |_pubkey, _index| num += 1);
        assert_eq!(num, 0);
    }

    #[test]
    fn test_secondary_index_include_exclude() {
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();
        let mut index = AccountSecondaryIndexes::default();

        assert!(!index.contains(&AccountIndex::ProgramId));
        index.indexes.insert(AccountIndex::ProgramId);
        assert!(index.contains(&AccountIndex::ProgramId));
        assert!(index.include_key(&pk1));
        assert!(index.include_key(&pk2));

        let exclude = false;
        index.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [pk1].iter().cloned().collect::<HashSet<_>>(),
            exclude,
        });
        assert!(index.include_key(&pk1));
        assert!(!index.include_key(&pk2));

        let exclude = true;
        index.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [pk1].iter().cloned().collect::<HashSet<_>>(),
            exclude,
        });
        assert!(!index.include_key(&pk1));
        assert!(index.include_key(&pk2));

        let exclude = true;
        index.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [pk1, pk2].iter().cloned().collect::<HashSet<_>>(),
            exclude,
        });
        assert!(!index.include_key(&pk1));
        assert!(!index.include_key(&pk2));

        let exclude = false;
        index.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [pk1, pk2].iter().cloned().collect::<HashSet<_>>(),
            exclude,
        });
        assert!(index.include_key(&pk1));
        assert!(index.include_key(&pk2));
    }

    #[test]
    fn test_insert_no_ancestors() {
        let key = Keypair::new();
        let index = AccountsIndex::<bool>::default();
        let mut gc = Vec::new();
        index.upsert(
            0,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            true,
            &mut gc,
        );
        assert!(gc.is_empty());

        let ancestors = Ancestors::default();
        assert!(index.get(&key.pubkey(), Some(&ancestors), None).is_none());
        assert!(index.get(&key.pubkey(), None, None).is_none());

        let mut num = 0;
        index.unchecked_scan_accounts("", &ancestors, |_pubkey, _index| num += 1);
        assert_eq!(num, 0);
    }

    type AccountInfoTest = f64;

    impl IsCached for AccountInfoTest {
        fn is_cached(&self) -> bool {
            true
        }
    }

    impl ZeroLamport for AccountInfoTest {
        fn is_zero_lamport(&self) -> bool {
            true
        }
    }
    #[test]
    fn test_insert_new_with_lock_no_ancestors() {
        let key = Keypair::new();
        let pubkey = &key.pubkey();
        let slot = 0;

        let index = AccountsIndex::<bool>::default();
        let account_info = true;
        let items = vec![(*pubkey, account_info)];
        index.insert_new_if_missing_into_primary_index(slot, items.len(), items.into_iter());

        let mut ancestors = Ancestors::default();
        assert!(index.get(pubkey, Some(&ancestors), None).is_none());
        assert!(index.get(pubkey, None, None).is_none());

        let mut num = 0;
        index.unchecked_scan_accounts("", &ancestors, |_pubkey, _index| num += 1);
        assert_eq!(num, 0);
        ancestors.insert(slot, 0);
        assert!(index.get(pubkey, Some(&ancestors), None).is_some());
        assert_eq!(index.ref_count_from_storage(pubkey), 1);
        index.unchecked_scan_accounts("", &ancestors, |_pubkey, _index| num += 1);
        assert_eq!(num, 1);

        // not zero lamports
        let index = AccountsIndex::<AccountInfoTest>::default();
        let account_info: AccountInfoTest = 0 as AccountInfoTest;
        let items = vec![(*pubkey, account_info)];
        index.insert_new_if_missing_into_primary_index(slot, items.len(), items.into_iter());

        let mut ancestors = Ancestors::default();
        assert!(index.get(pubkey, Some(&ancestors), None).is_none());
        assert!(index.get(pubkey, None, None).is_none());

        let mut num = 0;
        index.unchecked_scan_accounts("", &ancestors, |_pubkey, _index| num += 1);
        assert_eq!(num, 0);
        ancestors.insert(slot, 0);
        assert!(index.get(pubkey, Some(&ancestors), None).is_some());
        assert_eq!(index.ref_count_from_storage(pubkey), 0); // cached, so 0
        index.unchecked_scan_accounts("", &ancestors, |_pubkey, _index| num += 1);
        assert_eq!(num, 1);
    }

    #[test]
    fn test_new_entry() {
        let slot = 0;
        // account_info type that IS cached
        let account_info = AccountInfoTest::default();

        let new_entry = WriteAccountMapEntry::new_entry_after_update(slot, account_info);
        assert_eq!(new_entry.ref_count.load(Ordering::Relaxed), 0);
        assert_eq!(new_entry.slot_list.read().unwrap().capacity(), 1);
        assert_eq!(
            new_entry.slot_list.read().unwrap().to_vec(),
            vec![(slot, account_info)]
        );

        // account_info type that is NOT cached
        let account_info = true;

        let new_entry = WriteAccountMapEntry::new_entry_after_update(slot, account_info);
        assert_eq!(new_entry.ref_count.load(Ordering::Relaxed), 1);
        assert_eq!(new_entry.slot_list.read().unwrap().capacity(), 1);
        assert_eq!(
            new_entry.slot_list.read().unwrap().to_vec(),
            vec![(slot, account_info)]
        );
    }

    #[test]
    fn test_batch_insert() {
        let slot0 = 0;
        let key0 = Keypair::new().pubkey();
        let key1 = Keypair::new().pubkey();

        let index = AccountsIndex::<bool>::default();
        let account_infos = [true, false];

        let items = vec![(key0, account_infos[0]), (key1, account_infos[1])];
        index.insert_new_if_missing_into_primary_index(slot0, items.len(), items.into_iter());

        for (i, key) in [key0, key1].iter().enumerate() {
            let entry = index.get_account_read_entry(key).unwrap();
            assert_eq!(entry.ref_count().load(Ordering::Relaxed), 1);
            assert_eq!(entry.slot_list().to_vec(), vec![(slot0, account_infos[i]),]);
        }
    }

    fn test_new_entry_code_paths_helper<
        T: 'static
            + Sync
            + Send
            + Clone
            + IsCached
            + ZeroLamport
            + std::cmp::PartialEq
            + std::fmt::Debug,
    >(
        account_infos: [T; 2],
        is_cached: bool,
        upsert: bool,
    ) {
        let slot0 = 0;
        let slot1 = 1;
        let key = Keypair::new().pubkey();

        let index = AccountsIndex::<T>::default();
        let mut gc = Vec::new();

        if upsert {
            // insert first entry for pubkey. This will use new_entry_after_update and not call update.
            index.upsert(
                slot0,
                &key,
                &Pubkey::default(),
                &[],
                &AccountSecondaryIndexes::default(),
                account_infos[0].clone(),
                &mut gc,
            );
        } else {
            let items = vec![(key, account_infos[0].clone())];
            index.insert_new_if_missing_into_primary_index(slot0, items.len(), items.into_iter());
        }
        assert!(gc.is_empty());

        // verify the added entry matches expected
        {
            let entry = index.get_account_read_entry(&key).unwrap();
            assert_eq!(
                entry.ref_count().load(Ordering::Relaxed),
                if is_cached { 0 } else { 1 }
            );
            let expected = vec![(slot0, account_infos[0].clone())];
            assert_eq!(entry.slot_list().to_vec(), expected);
            let new_entry =
                WriteAccountMapEntry::new_entry_after_update(slot0, account_infos[0].clone());
            assert_eq!(
                entry.slot_list().to_vec(),
                new_entry.slot_list.read().unwrap().to_vec(),
            );
        }

        // insert second entry for pubkey. This will use update and NOT use new_entry_after_update.
        if upsert {
            index.upsert(
                slot1,
                &key,
                &Pubkey::default(),
                &[],
                &AccountSecondaryIndexes::default(),
                account_infos[1].clone(),
                &mut gc,
            );
        } else {
            let items = vec![(key, account_infos[1].clone())];
            index.insert_new_if_missing_into_primary_index(slot1, items.len(), items.into_iter());
        }
        assert!(gc.is_empty());

        for lock in &[false, true] {
            let read_lock = if *lock {
                Some(index.get_account_maps_read_lock(&key))
            } else {
                None
            };

            let entry = if *lock {
                index
                    .get_account_read_entry_with_lock(&key, read_lock.as_ref().unwrap())
                    .unwrap()
            } else {
                index.get_account_read_entry(&key).unwrap()
            };

            assert_eq!(
                entry.ref_count().load(Ordering::Relaxed),
                if is_cached { 0 } else { 2 }
            );
            assert_eq!(
                entry.slot_list().to_vec(),
                vec![
                    (slot0, account_infos[0].clone()),
                    (slot1, account_infos[1].clone())
                ]
            );

            let new_entry =
                WriteAccountMapEntry::new_entry_after_update(slot1, account_infos[1].clone());
            assert_eq!(entry.slot_list()[1], new_entry.slot_list.read().unwrap()[0],);
        }
    }

    #[test]
    fn test_new_entry_and_update_code_paths() {
        for is_upsert in &[false, true] {
            // account_info type that IS cached
            test_new_entry_code_paths_helper([1.0, 2.0], true, *is_upsert);

            // account_info type that is NOT cached
            test_new_entry_code_paths_helper([true, false], false, *is_upsert);
        }
    }

    #[test]
    fn test_insert_with_lock_no_ancestors() {
        let key = Keypair::new();
        let index = AccountsIndex::<bool>::default();
        let slot = 0;
        let account_info = true;

        let new_entry = WriteAccountMapEntry::new_entry_after_update(slot, account_info);
        let mut w_account_maps = index.get_account_maps_write_lock(&key.pubkey());
        let write = index.insert_new_entry_if_missing_with_lock(
            key.pubkey(),
            &mut w_account_maps,
            new_entry,
        );
        assert!(write.is_none());
        drop(w_account_maps);

        let mut ancestors = Ancestors::default();
        assert!(index.get(&key.pubkey(), Some(&ancestors), None).is_none());
        assert!(index.get(&key.pubkey(), None, None).is_none());

        let mut num = 0;
        index.unchecked_scan_accounts("", &ancestors, |_pubkey, _index| num += 1);
        assert_eq!(num, 0);
        ancestors.insert(slot, 0);
        assert!(index.get(&key.pubkey(), Some(&ancestors), None).is_some());
        index.unchecked_scan_accounts("", &ancestors, |_pubkey, _index| num += 1);
        assert_eq!(num, 1);
    }

    #[test]
    fn test_insert_wrong_ancestors() {
        let key = Keypair::new();
        let index = AccountsIndex::<bool>::default();
        let mut gc = Vec::new();
        index.upsert(
            0,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            true,
            &mut gc,
        );
        assert!(gc.is_empty());

        let ancestors = vec![(1, 1)].into_iter().collect();
        assert!(index.get(&key.pubkey(), Some(&ancestors), None).is_none());

        let mut num = 0;
        index.unchecked_scan_accounts("", &ancestors, |_pubkey, _index| num += 1);
        assert_eq!(num, 0);
    }

    #[test]
    fn test_insert_with_ancestors() {
        let key = Keypair::new();
        let index = AccountsIndex::<bool>::default();
        let mut gc = Vec::new();
        index.upsert(
            0,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            true,
            &mut gc,
        );
        assert!(gc.is_empty());

        let ancestors = vec![(0, 0)].into_iter().collect();
        let (list, idx) = index.get(&key.pubkey(), Some(&ancestors), None).unwrap();
        assert_eq!(list.slot_list()[idx], (0, true));

        let mut num = 0;
        let mut found_key = false;
        index.unchecked_scan_accounts("", &ancestors, |pubkey, _index| {
            if pubkey == &key.pubkey() {
                found_key = true
            };
            num += 1
        });
        assert_eq!(num, 1);
        assert!(found_key);
    }

    fn setup_accounts_index_keys(num_pubkeys: usize) -> (AccountsIndex<bool>, Vec<Pubkey>) {
        let index = AccountsIndex::<bool>::default();
        let root_slot = 0;

        let mut pubkeys: Vec<Pubkey> = std::iter::repeat_with(|| {
            let new_pubkey = solana_sdk::pubkey::new_rand();
            index.upsert(
                root_slot,
                &new_pubkey,
                &Pubkey::default(),
                &[],
                &AccountSecondaryIndexes::default(),
                true,
                &mut vec![],
            );
            new_pubkey
        })
        .take(num_pubkeys.saturating_sub(1))
        .collect();

        if num_pubkeys != 0 {
            pubkeys.push(Pubkey::default());
            index.upsert(
                root_slot,
                &Pubkey::default(),
                &Pubkey::default(),
                &[],
                &AccountSecondaryIndexes::default(),
                true,
                &mut vec![],
            );
        }

        index.add_root(root_slot, false);

        (index, pubkeys)
    }

    fn run_test_range(
        index: &AccountsIndex<bool>,
        pubkeys: &[Pubkey],
        start_bound: Bound<usize>,
        end_bound: Bound<usize>,
    ) {
        // Exclusive `index_start`
        let (pubkey_start, index_start) = match start_bound {
            Unbounded => (Unbounded, 0),
            Included(i) => (Included(pubkeys[i]), i),
            Excluded(i) => (Excluded(pubkeys[i]), i + 1),
        };

        // Exclusive `index_end`
        let (pubkey_end, index_end) = match end_bound {
            Unbounded => (Unbounded, pubkeys.len()),
            Included(i) => (Included(pubkeys[i]), i + 1),
            Excluded(i) => (Excluded(pubkeys[i]), i),
        };
        let pubkey_range = (pubkey_start, pubkey_end);

        let ancestors = Ancestors::default();
        let mut scanned_keys = HashSet::new();
        index.range_scan_accounts("", &ancestors, pubkey_range, |pubkey, _index| {
            scanned_keys.insert(*pubkey);
        });

        let mut expected_len = 0;
        for key in &pubkeys[index_start..index_end] {
            expected_len += 1;
            assert!(scanned_keys.contains(key));
        }

        assert_eq!(scanned_keys.len(), expected_len);
    }

    fn run_test_range_indexes(
        index: &AccountsIndex<bool>,
        pubkeys: &[Pubkey],
        start: Option<usize>,
        end: Option<usize>,
    ) {
        let start_options = start
            .map(|i| vec![Included(i), Excluded(i)])
            .unwrap_or_else(|| vec![Unbounded]);
        let end_options = end
            .map(|i| vec![Included(i), Excluded(i)])
            .unwrap_or_else(|| vec![Unbounded]);

        for start in &start_options {
            for end in &end_options {
                run_test_range(index, pubkeys, *start, *end);
            }
        }
    }

    #[test]
    fn test_range_scan_accounts() {
        let (index, mut pubkeys) = setup_accounts_index_keys(3 * ITER_BATCH_SIZE);
        pubkeys.sort();

        run_test_range_indexes(&index, &pubkeys, None, None);

        run_test_range_indexes(&index, &pubkeys, Some(ITER_BATCH_SIZE), None);

        run_test_range_indexes(&index, &pubkeys, None, Some(2 * ITER_BATCH_SIZE as usize));

        run_test_range_indexes(
            &index,
            &pubkeys,
            Some(ITER_BATCH_SIZE as usize),
            Some(2 * ITER_BATCH_SIZE as usize),
        );

        run_test_range_indexes(
            &index,
            &pubkeys,
            Some(ITER_BATCH_SIZE as usize),
            Some(2 * ITER_BATCH_SIZE as usize - 1),
        );

        run_test_range_indexes(
            &index,
            &pubkeys,
            Some(ITER_BATCH_SIZE - 1_usize),
            Some(2 * ITER_BATCH_SIZE as usize + 1),
        );
    }

    fn run_test_scan_accounts(num_pubkeys: usize) {
        let (index, _) = setup_accounts_index_keys(num_pubkeys);
        let ancestors = Ancestors::default();

        let mut scanned_keys = HashSet::new();
        index.unchecked_scan_accounts("", &ancestors, |pubkey, _index| {
            scanned_keys.insert(*pubkey);
        });
        assert_eq!(scanned_keys.len(), num_pubkeys);
    }

    #[test]
    fn test_scan_accounts() {
        run_test_scan_accounts(0);
        run_test_scan_accounts(1);
        run_test_scan_accounts(ITER_BATCH_SIZE * 10);
        run_test_scan_accounts(ITER_BATCH_SIZE * 10 - 1);
        run_test_scan_accounts(ITER_BATCH_SIZE * 10 + 1);
    }

    #[test]
    fn test_accounts_iter_finished() {
        let (index, _) = setup_accounts_index_keys(0);
        let mut iter = index.iter(None::<Range<Pubkey>>);
        assert!(iter.next().is_none());
        let mut gc = vec![];
        index.upsert(
            0,
            &solana_sdk::pubkey::new_rand(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            true,
            &mut gc,
        );
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_is_root() {
        let index = AccountsIndex::<bool>::default();
        assert!(!index.is_root(0));
        index.add_root(0, false);
        assert!(index.is_root(0));
    }

    #[test]
    fn test_insert_with_root() {
        let key = Keypair::new();
        let index = AccountsIndex::<bool>::default();
        let mut gc = Vec::new();
        index.upsert(
            0,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            true,
            &mut gc,
        );
        assert!(gc.is_empty());

        index.add_root(0, false);
        let (list, idx) = index.get(&key.pubkey(), None, None).unwrap();
        assert_eq!(list.slot_list()[idx], (0, true));
    }

    #[test]
    fn test_clean_first() {
        let index = AccountsIndex::<bool>::default();
        index.add_root(0, false);
        index.add_root(1, false);
        index.clean_dead_slot(0);
        assert!(index.is_root(1));
        assert!(!index.is_root(0));
    }

    #[test]
    fn test_clean_last() {
        //this behavior might be undefined, clean up should only occur on older slots
        let index = AccountsIndex::<bool>::default();
        index.add_root(0, false);
        index.add_root(1, false);
        index.clean_dead_slot(1);
        assert!(!index.is_root(1));
        assert!(index.is_root(0));
    }

    #[test]
    fn test_clean_and_unclean_slot() {
        let index = AccountsIndex::<bool>::default();
        assert_eq!(0, index.roots_tracker.read().unwrap().uncleaned_roots.len());
        index.add_root(0, false);
        index.add_root(1, false);
        assert_eq!(2, index.roots_tracker.read().unwrap().uncleaned_roots.len());

        assert_eq!(
            0,
            index
                .roots_tracker
                .read()
                .unwrap()
                .previous_uncleaned_roots
                .len()
        );
        index.reset_uncleaned_roots(None);
        assert_eq!(2, index.roots_tracker.read().unwrap().roots.len());
        assert_eq!(0, index.roots_tracker.read().unwrap().uncleaned_roots.len());
        assert_eq!(
            2,
            index
                .roots_tracker
                .read()
                .unwrap()
                .previous_uncleaned_roots
                .len()
        );

        index.add_root(2, false);
        index.add_root(3, false);
        assert_eq!(4, index.roots_tracker.read().unwrap().roots.len());
        assert_eq!(2, index.roots_tracker.read().unwrap().uncleaned_roots.len());
        assert_eq!(
            2,
            index
                .roots_tracker
                .read()
                .unwrap()
                .previous_uncleaned_roots
                .len()
        );

        index.clean_dead_slot(1);
        assert_eq!(3, index.roots_tracker.read().unwrap().roots.len());
        assert_eq!(2, index.roots_tracker.read().unwrap().uncleaned_roots.len());
        assert_eq!(
            1,
            index
                .roots_tracker
                .read()
                .unwrap()
                .previous_uncleaned_roots
                .len()
        );

        index.clean_dead_slot(2);
        assert_eq!(2, index.roots_tracker.read().unwrap().roots.len());
        assert_eq!(1, index.roots_tracker.read().unwrap().uncleaned_roots.len());
        assert_eq!(
            1,
            index
                .roots_tracker
                .read()
                .unwrap()
                .previous_uncleaned_roots
                .len()
        );
    }

    #[test]
    fn test_update_last_wins() {
        let key = Keypair::new();
        let index = AccountsIndex::<bool>::default();
        let ancestors = vec![(0, 0)].into_iter().collect();
        let mut gc = Vec::new();
        index.upsert(
            0,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            true,
            &mut gc,
        );
        assert!(gc.is_empty());
        let (list, idx) = index.get(&key.pubkey(), Some(&ancestors), None).unwrap();
        assert_eq!(list.slot_list()[idx], (0, true));
        drop(list);

        let mut gc = Vec::new();
        index.upsert(
            0,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            false,
            &mut gc,
        );
        assert_eq!(gc, vec![(0, true)]);
        let (list, idx) = index.get(&key.pubkey(), Some(&ancestors), None).unwrap();
        assert_eq!(list.slot_list()[idx], (0, false));
    }

    #[test]
    fn test_update_new_slot() {
        solana_logger::setup();
        let key = Keypair::new();
        let index = AccountsIndex::<bool>::default();
        let ancestors = vec![(0, 0)].into_iter().collect();
        let mut gc = Vec::new();
        index.upsert(
            0,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            true,
            &mut gc,
        );
        assert!(gc.is_empty());
        index.upsert(
            1,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            false,
            &mut gc,
        );
        assert!(gc.is_empty());
        let (list, idx) = index.get(&key.pubkey(), Some(&ancestors), None).unwrap();
        assert_eq!(list.slot_list()[idx], (0, true));
        let ancestors = vec![(1, 0)].into_iter().collect();
        let (list, idx) = index.get(&key.pubkey(), Some(&ancestors), None).unwrap();
        assert_eq!(list.slot_list()[idx], (1, false));
    }

    #[test]
    fn test_update_gc_purged_slot() {
        let key = Keypair::new();
        let index = AccountsIndex::<bool>::default();
        let mut gc = Vec::new();
        index.upsert(
            0,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            true,
            &mut gc,
        );
        assert!(gc.is_empty());
        index.upsert(
            1,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            false,
            &mut gc,
        );
        index.upsert(
            2,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            true,
            &mut gc,
        );
        index.upsert(
            3,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            true,
            &mut gc,
        );
        index.add_root(0, false);
        index.add_root(1, false);
        index.add_root(3, false);
        index.upsert(
            4,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            true,
            &mut gc,
        );

        // Updating index should not purge older roots, only purges
        // previous updates within the same slot
        assert_eq!(gc, vec![]);
        let (list, idx) = index.get(&key.pubkey(), None, None).unwrap();
        assert_eq!(list.slot_list()[idx], (3, true));

        let mut num = 0;
        let mut found_key = false;
        index.unchecked_scan_accounts("", &Ancestors::default(), |pubkey, _index| {
            if pubkey == &key.pubkey() {
                found_key = true;
                assert_eq!(_index, (&true, 3));
            };
            num += 1
        });
        assert_eq!(num, 1);
        assert!(found_key);
    }

    #[test]
    fn test_purge() {
        let key = Keypair::new();
        let index = AccountsIndex::<u64>::default();
        let mut gc = Vec::new();
        assert!(index.upsert(
            1,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            12,
            &mut gc
        ));

        assert!(!index.upsert(
            1,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            10,
            &mut gc
        ));

        let purges = index.purge_roots(&key.pubkey());
        assert_eq!(purges, (vec![], false));
        index.add_root(1, false);

        let purges = index.purge_roots(&key.pubkey());
        assert_eq!(purges, (vec![(1, 10)], true));

        assert!(!index.upsert(
            1,
            &key.pubkey(),
            &Pubkey::default(),
            &[],
            &AccountSecondaryIndexes::default(),
            9,
            &mut gc
        ));
    }

    #[test]
    fn test_latest_slot() {
        let slot_slice = vec![(0, true), (5, true), (3, true), (7, true)];
        let index = AccountsIndex::<bool>::default();

        // No ancestors, no root, should return None
        assert!(index.latest_slot(None, &slot_slice, None).is_none());

        // Given a root, should return the root
        index.add_root(5, false);
        assert_eq!(index.latest_slot(None, &slot_slice, None).unwrap(), 1);

        // Given a max_root == root, should still return the root
        assert_eq!(index.latest_slot(None, &slot_slice, Some(5)).unwrap(), 1);

        // Given a max_root < root, should filter out the root
        assert!(index.latest_slot(None, &slot_slice, Some(4)).is_none());

        // Given a max_root, should filter out roots < max_root, but specified
        // ancestors should not be affected
        let ancestors = vec![(3, 1), (7, 1)].into_iter().collect();
        assert_eq!(
            index
                .latest_slot(Some(&ancestors), &slot_slice, Some(4))
                .unwrap(),
            3
        );
        assert_eq!(
            index
                .latest_slot(Some(&ancestors), &slot_slice, Some(7))
                .unwrap(),
            3
        );

        // Given no max_root, should just return the greatest ancestor or root
        assert_eq!(
            index
                .latest_slot(Some(&ancestors), &slot_slice, None)
                .unwrap(),
            3
        );
    }

    fn run_test_purge_exact_secondary_index<
        SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send,
    >(
        index: &AccountsIndex<bool>,
        secondary_index: &SecondaryIndex<SecondaryIndexEntryType>,
        key_start: usize,
        key_end: usize,
        secondary_indexes: &AccountSecondaryIndexes,
    ) {
        // No roots, should be no reclaims
        let slots = vec![1, 2, 5, 9];
        let index_key = Pubkey::new_unique();
        let account_key = Pubkey::new_unique();

        let mut account_data = vec![0; inline_spl_token_v2_0::state::Account::get_packed_len()];
        account_data[key_start..key_end].clone_from_slice(&(index_key.to_bytes()));

        // Insert slots into secondary index
        for slot in &slots {
            index.upsert(
                *slot,
                &account_key,
                // Make sure these accounts are added to secondary index
                &inline_spl_token_v2_0::id(),
                &account_data,
                secondary_indexes,
                true,
                &mut vec![],
            );
        }

        // Only one top level index entry exists
        assert_eq!(secondary_index.index.get(&index_key).unwrap().len(), 1);

        // In the reverse index, one account maps across multiple slots
        // to the same top level key
        assert_eq!(
            secondary_index
                .reverse_index
                .get(&account_key)
                .unwrap()
                .value()
                .read()
                .unwrap()
                .len(),
            1
        );

        index.purge_exact(
            &account_key,
            &slots.into_iter().collect::<HashSet<Slot>>(),
            &mut vec![],
        );

        index.handle_dead_keys(&[&account_key], secondary_indexes);
        assert!(secondary_index.index.is_empty());
        assert!(secondary_index.reverse_index.is_empty());
    }

    #[test]
    fn test_purge_exact_dashmap_secondary_index() {
        let (key_start, key_end, secondary_indexes) = create_dashmap_secondary_index_state();
        let index = AccountsIndex::<bool>::default();
        run_test_purge_exact_secondary_index(
            &index,
            &index.spl_token_mint_index,
            key_start,
            key_end,
            &secondary_indexes,
        );
    }

    #[test]
    fn test_purge_exact_rwlock_secondary_index() {
        let (key_start, key_end, secondary_indexes) = create_rwlock_secondary_index_state();
        let index = AccountsIndex::<bool>::default();
        run_test_purge_exact_secondary_index(
            &index,
            &index.spl_token_owner_index,
            key_start,
            key_end,
            &secondary_indexes,
        );
    }

    #[test]
    fn test_purge_older_root_entries() {
        // No roots, should be no reclaims
        let index = AccountsIndex::<bool>::default();
        let mut slot_list = vec![(1, true), (2, true), (5, true), (9, true)];
        let mut reclaims = vec![];
        index.purge_older_root_entries(&mut slot_list, &mut reclaims, None);
        assert!(reclaims.is_empty());
        assert_eq!(slot_list, vec![(1, true), (2, true), (5, true), (9, true)]);

        // Add a later root, earlier slots should be reclaimed
        slot_list = vec![(1, true), (2, true), (5, true), (9, true)];
        index.add_root(1, false);
        // Note 2 is not a root
        index.add_root(5, false);
        reclaims = vec![];
        index.purge_older_root_entries(&mut slot_list, &mut reclaims, None);
        assert_eq!(reclaims, vec![(1, true), (2, true)]);
        assert_eq!(slot_list, vec![(5, true), (9, true)]);

        // Add a later root that is not in the list, should not affect the outcome
        slot_list = vec![(1, true), (2, true), (5, true), (9, true)];
        index.add_root(6, false);
        reclaims = vec![];
        index.purge_older_root_entries(&mut slot_list, &mut reclaims, None);
        assert_eq!(reclaims, vec![(1, true), (2, true)]);
        assert_eq!(slot_list, vec![(5, true), (9, true)]);

        // Pass a max root >= than any root in the slot list, should not affect
        // outcome
        slot_list = vec![(1, true), (2, true), (5, true), (9, true)];
        reclaims = vec![];
        index.purge_older_root_entries(&mut slot_list, &mut reclaims, Some(6));
        assert_eq!(reclaims, vec![(1, true), (2, true)]);
        assert_eq!(slot_list, vec![(5, true), (9, true)]);

        // Pass a max root, earlier slots should be reclaimed
        slot_list = vec![(1, true), (2, true), (5, true), (9, true)];
        reclaims = vec![];
        index.purge_older_root_entries(&mut slot_list, &mut reclaims, Some(5));
        assert_eq!(reclaims, vec![(1, true), (2, true)]);
        assert_eq!(slot_list, vec![(5, true), (9, true)]);

        // Pass a max root 2. This means the latest root < 2 is 1 because 2 is not a root
        // so nothing will be purged
        slot_list = vec![(1, true), (2, true), (5, true), (9, true)];
        reclaims = vec![];
        index.purge_older_root_entries(&mut slot_list, &mut reclaims, Some(2));
        assert!(reclaims.is_empty());
        assert_eq!(slot_list, vec![(1, true), (2, true), (5, true), (9, true)]);

        // Pass a max root 1. This means the latest root < 3 is 1 because 2 is not a root
        // so nothing will be purged
        slot_list = vec![(1, true), (2, true), (5, true), (9, true)];
        reclaims = vec![];
        index.purge_older_root_entries(&mut slot_list, &mut reclaims, Some(1));
        assert!(reclaims.is_empty());
        assert_eq!(slot_list, vec![(1, true), (2, true), (5, true), (9, true)]);

        // Pass a max root that doesn't exist in the list but is greater than
        // some of the roots in the list, shouldn't return those smaller roots
        slot_list = vec![(1, true), (2, true), (5, true), (9, true)];
        reclaims = vec![];
        index.purge_older_root_entries(&mut slot_list, &mut reclaims, Some(7));
        assert_eq!(reclaims, vec![(1, true), (2, true)]);
        assert_eq!(slot_list, vec![(5, true), (9, true)]);
    }

    fn check_secondary_index_mapping_correct<SecondaryIndexEntryType>(
        secondary_index: &SecondaryIndex<SecondaryIndexEntryType>,
        secondary_index_keys: &[Pubkey],
        account_key: &Pubkey,
    ) where
        SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send,
    {
        // Check secondary index has unique mapping from secondary index key
        // to the account key and slot
        for secondary_index_key in secondary_index_keys {
            assert_eq!(secondary_index.index.len(), secondary_index_keys.len());
            let account_key_map = secondary_index.get(secondary_index_key);
            assert_eq!(account_key_map.len(), 1);
            assert_eq!(account_key_map, vec![*account_key]);
        }
        // Check reverse index contains all of the `secondary_index_keys`
        let secondary_index_key_map = secondary_index.reverse_index.get(account_key).unwrap();
        assert_eq!(
            &*secondary_index_key_map.value().read().unwrap(),
            secondary_index_keys
        );
    }

    fn run_test_secondary_indexes<
        SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send,
    >(
        index: &AccountsIndex<bool>,
        secondary_index: &SecondaryIndex<SecondaryIndexEntryType>,
        key_start: usize,
        key_end: usize,
        secondary_indexes: &AccountSecondaryIndexes,
    ) {
        let mut secondary_indexes = secondary_indexes.clone();
        let account_key = Pubkey::new_unique();
        let index_key = Pubkey::new_unique();
        let mut account_data = vec![0; inline_spl_token_v2_0::state::Account::get_packed_len()];
        account_data[key_start..key_end].clone_from_slice(&(index_key.to_bytes()));

        // Wrong program id
        index.upsert(
            0,
            &account_key,
            &Pubkey::default(),
            &account_data,
            &secondary_indexes,
            true,
            &mut vec![],
        );
        assert!(secondary_index.index.is_empty());
        assert!(secondary_index.reverse_index.is_empty());

        // Wrong account data size
        index.upsert(
            0,
            &account_key,
            &inline_spl_token_v2_0::id(),
            &account_data[1..],
            &secondary_indexes,
            true,
            &mut vec![],
        );
        assert!(secondary_index.index.is_empty());
        assert!(secondary_index.reverse_index.is_empty());

        secondary_indexes.keys = None;

        // Just right. Inserting the same index multiple times should be ok
        for _ in 0..2 {
            index.update_secondary_indexes(
                &account_key,
                &inline_spl_token_v2_0::id(),
                &account_data,
                &secondary_indexes,
            );
            check_secondary_index_mapping_correct(secondary_index, &[index_key], &account_key);
        }

        // included
        assert!(!secondary_index.index.is_empty());
        assert!(!secondary_index.reverse_index.is_empty());

        secondary_indexes.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [index_key].iter().cloned().collect::<HashSet<_>>(),
            exclude: false,
        });
        secondary_index.index.clear();
        secondary_index.reverse_index.clear();
        index.update_secondary_indexes(
            &account_key,
            &inline_spl_token_v2_0::id(),
            &account_data,
            &secondary_indexes,
        );
        assert!(!secondary_index.index.is_empty());
        assert!(!secondary_index.reverse_index.is_empty());
        check_secondary_index_mapping_correct(secondary_index, &[index_key], &account_key);

        // not-excluded
        secondary_indexes.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [].iter().cloned().collect::<HashSet<_>>(),
            exclude: true,
        });
        secondary_index.index.clear();
        secondary_index.reverse_index.clear();
        index.update_secondary_indexes(
            &account_key,
            &inline_spl_token_v2_0::id(),
            &account_data,
            &secondary_indexes,
        );
        assert!(!secondary_index.index.is_empty());
        assert!(!secondary_index.reverse_index.is_empty());
        check_secondary_index_mapping_correct(secondary_index, &[index_key], &account_key);

        secondary_indexes.keys = None;

        index
            .get_account_write_entry(&account_key)
            .unwrap()
            .slot_list_mut(|slot_list| slot_list.clear());

        // Everything should be deleted
        index.handle_dead_keys(&[&account_key], &secondary_indexes);
        assert!(secondary_index.index.is_empty());
        assert!(secondary_index.reverse_index.is_empty());
    }

    #[test]
    fn test_dashmap_secondary_index() {
        let (key_start, key_end, secondary_indexes) = create_dashmap_secondary_index_state();
        let index = AccountsIndex::<bool>::default();
        run_test_secondary_indexes(
            &index,
            &index.spl_token_mint_index,
            key_start,
            key_end,
            &secondary_indexes,
        );
    }

    #[test]
    fn test_rwlock_secondary_index() {
        let (key_start, key_end, secondary_indexes) = create_rwlock_secondary_index_state();
        let index = AccountsIndex::<bool>::default();
        run_test_secondary_indexes(
            &index,
            &index.spl_token_owner_index,
            key_start,
            key_end,
            &secondary_indexes,
        );
    }

    fn run_test_secondary_indexes_same_slot_and_forks<
        SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send,
    >(
        index: &AccountsIndex<bool>,
        secondary_index: &SecondaryIndex<SecondaryIndexEntryType>,
        index_key_start: usize,
        index_key_end: usize,
        secondary_indexes: &AccountSecondaryIndexes,
    ) {
        let account_key = Pubkey::new_unique();
        let secondary_key1 = Pubkey::new_unique();
        let secondary_key2 = Pubkey::new_unique();
        let slot = 1;
        let mut account_data1 = vec![0; inline_spl_token_v2_0::state::Account::get_packed_len()];
        account_data1[index_key_start..index_key_end]
            .clone_from_slice(&(secondary_key1.to_bytes()));
        let mut account_data2 = vec![0; inline_spl_token_v2_0::state::Account::get_packed_len()];
        account_data2[index_key_start..index_key_end]
            .clone_from_slice(&(secondary_key2.to_bytes()));

        // First write one mint index
        index.upsert(
            slot,
            &account_key,
            &inline_spl_token_v2_0::id(),
            &account_data1,
            secondary_indexes,
            true,
            &mut vec![],
        );

        // Now write a different mint index for the same account
        index.upsert(
            slot,
            &account_key,
            &inline_spl_token_v2_0::id(),
            &account_data2,
            secondary_indexes,
            true,
            &mut vec![],
        );

        // Both pubkeys will now be present in the index
        check_secondary_index_mapping_correct(
            secondary_index,
            &[secondary_key1, secondary_key2],
            &account_key,
        );

        // If a later slot also introduces secondary_key1, then it should still exist in the index
        let later_slot = slot + 1;
        index.upsert(
            later_slot,
            &account_key,
            &inline_spl_token_v2_0::id(),
            &account_data1,
            secondary_indexes,
            true,
            &mut vec![],
        );
        assert_eq!(secondary_index.get(&secondary_key1), vec![account_key]);

        // If we set a root at `later_slot`, and clean, then even though the account with secondary_key1
        // was outdated by the update in the later slot, the primary account key is still alive,
        // so both secondary keys will still be kept alive.
        index.add_root(later_slot, false);
        index
            .get_account_write_entry(&account_key)
            .unwrap()
            .slot_list_mut(|slot_list| {
                index.purge_older_root_entries(slot_list, &mut vec![], None)
            });

        check_secondary_index_mapping_correct(
            secondary_index,
            &[secondary_key1, secondary_key2],
            &account_key,
        );

        // Removing the remaining entry for this pubkey in the index should mark the
        // pubkey as dead and finally remove all the secondary indexes
        let mut reclaims = vec![];
        index.purge_exact(&account_key, &later_slot, &mut reclaims);
        index.handle_dead_keys(&[&account_key], secondary_indexes);
        assert!(secondary_index.index.is_empty());
        assert!(secondary_index.reverse_index.is_empty());
    }

    #[test]
    fn test_dashmap_secondary_index_same_slot_and_forks() {
        let (key_start, key_end, account_index) = create_dashmap_secondary_index_state();
        let index = AccountsIndex::<bool>::default();
        run_test_secondary_indexes_same_slot_and_forks(
            &index,
            &index.spl_token_mint_index,
            key_start,
            key_end,
            &account_index,
        );
    }

    #[test]
    fn test_rwlock_secondary_index_same_slot_and_forks() {
        let (key_start, key_end, account_index) = create_rwlock_secondary_index_state();
        let index = AccountsIndex::<bool>::default();
        run_test_secondary_indexes_same_slot_and_forks(
            &index,
            &index.spl_token_owner_index,
            key_start,
            key_end,
            &account_index,
        );
    }

    impl ZeroLamport for bool {
        fn is_zero_lamport(&self) -> bool {
            false
        }
    }

    impl ZeroLamport for u64 {
        fn is_zero_lamport(&self) -> bool {
            false
        }
    }
}
