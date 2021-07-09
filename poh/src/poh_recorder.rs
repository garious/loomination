//! The `poh_recorder` module provides an object for synchronizing with Proof of History.
//! It synchronizes PoH, bank's register_tick and the ledger
//!
//! PohRecorder will send ticks or entries to a WorkingBank, if the current range of ticks is
//! within the specified WorkingBank range.
//!
//! For Ticks:
//! * new tick_height must be > WorkingBank::min_tick_height && new tick_height must be <= WorkingBank::max_tick_height
//!
//! For Entries:
//! * recorded entry must be >= WorkingBank::min_tick_height && entry must be < WorkingBank::max_tick_height
//!
pub use solana_sdk::clock::Slot;
use {
    crate::poh_service::PohService,
    crossbeam_channel::{
        unbounded, Receiver as CrossbeamReceiver, RecvTimeoutError, Sender as CrossbeamSender,
    },
    log::*,
    solana_entry::{entry::Entry, poh::Poh},
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_runtime::bank::Bank,
    solana_sdk::{
        clock::NUM_CONSECUTIVE_LEADER_SLOTS, hash::Hash, poh_config::PohConfig, pubkey::Pubkey,
        timing, transaction::Transaction,
    },
    std::{
        cmp,
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc::{channel, Receiver, SendError, Sender, SyncSender},
            {Arc, Mutex},
        },
        time::{Duration, Instant},
    },
    thiserror::Error,
};

pub const GRACE_TICKS_FACTOR: u64 = 2;
pub const MAX_GRACE_SLOTS: u64 = 2;

#[derive(Error, Debug, Clone)]
pub enum PohRecorderError {
    #[error("invalid calling object")]
    InvalidCallingObject,

    #[error("max height reached")]
    MaxHeightReached,

    #[error("min height not reached")]
    MinHeightNotReached,

    #[error("send WorkingBankEntry error")]
    SendError(#[from] SendError<WorkingBankEntry>),
}

type Result<T> = std::result::Result<T, PohRecorderError>;

pub type WorkingBankEntry = (Arc<Bank>, (Entry, u64));
pub type BankStart = (Arc<Bank>, Arc<Instant>);

pub struct Record {
    pub mixin: Hash,
    pub transactions: Vec<Transaction>,
    pub slot: Slot,
    pub sender: CrossbeamSender<Result<()>>,
}
impl Record {
    pub fn new(
        mixin: Hash,
        transactions: Vec<Transaction>,
        slot: Slot,
        sender: CrossbeamSender<Result<()>>,
    ) -> Self {
        Self {
            mixin,
            transactions,
            slot,
            sender,
        }
    }
}

pub struct TransactionRecorder {
    // shared by all users of PohRecorder
    pub record_sender: CrossbeamSender<Record>,
    pub is_exited: Arc<AtomicBool>,
}

impl Clone for TransactionRecorder {
    fn clone(&self) -> Self {
        TransactionRecorder::new(self.record_sender.clone(), self.is_exited.clone())
    }
}

impl TransactionRecorder {
    pub fn new(record_sender: CrossbeamSender<Record>, is_exited: Arc<AtomicBool>) -> Self {
        Self {
            // shared
            record_sender,
            // shared
            is_exited,
        }
    }
    pub fn record(
        &self,
        bank_slot: Slot,
        mixin: Hash,
        transactions: Vec<Transaction>,
    ) -> Result<()> {
        // create a new channel so that there is only 1 sender and when it goes out of scope, the receiver fails
        let (result_sender, result_receiver) = unbounded();
        let res =
            self.record_sender
                .send(Record::new(mixin, transactions, bank_slot, result_sender));
        if res.is_err() {
            // If the channel is dropped, then the validator is shutting down so return that we are hitting
            //  the max tick height to stop transaction processing and flush any transactions in the pipeline.
            return Err(PohRecorderError::MaxHeightReached);
        }
        // Besides validator exit, this timeout should primarily be seen to affect test execution environments where the various pieces can be shutdown abruptly
        let mut is_exited = false;
        loop {
            let res = result_receiver.recv_timeout(Duration::from_millis(1000));
            match res {
                Err(RecvTimeoutError::Timeout) => {
                    if is_exited {
                        return Err(PohRecorderError::MaxHeightReached);
                    } else {
                        // A result may have come in between when we timed out checking this
                        // bool, so check the channel again, even if is_exited == true
                        is_exited = self.is_exited.load(Ordering::SeqCst);
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(PohRecorderError::MaxHeightReached);
                }
                Ok(result) => {
                    return result;
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct WorkingBank {
    pub bank: Arc<Bank>,
    pub start: Arc<Instant>,
    pub min_tick_height: u64,
    pub max_tick_height: u64,
}

pub struct PohRecorder {
    pub poh: Arc<Mutex<Poh>>,
    tick_height: u64,
    clear_bank_signal: Option<SyncSender<bool>>,
    start_slot: Slot,              // parent slot
    start_tick_height: u64,        // first tick_height this recorder will observe
    tick_cache: Vec<(Entry, u64)>, // cache of entry and its tick_height
    working_bank: Option<WorkingBank>,
    sender: Sender<WorkingBankEntry>,
    leader_first_tick_height: Option<u64>,
    leader_last_tick_height: u64, // zero if none
    grace_ticks: u64,
    id: Pubkey,
    blockstore: Arc<Blockstore>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    poh_config: Arc<PohConfig>,
    ticks_per_slot: u64,
    target_ns_per_tick: u64,
    record_lock_contention_us: u64,
    flush_cache_no_tick_us: u64,
    flush_cache_tick_us: u64,
    prepare_send_us: u64,
    send_us: u64,
    tick_lock_contention_us: u64,
    tick_overhead_us: u64,
    total_sleep_us: u64,
    record_us: u64,
    ticks_from_record: u64,
    last_metric: Instant,
    record_sender: CrossbeamSender<Record>,
    pub is_exited: Arc<AtomicBool>,
}

impl PohRecorder {
    fn clear_bank(&mut self) {
        if let Some(working_bank) = self.working_bank.take() {
            let bank = working_bank.bank;
            let next_leader_slot = self.leader_schedule_cache.next_leader_slot(
                &self.id,
                bank.slot(),
                &bank,
                Some(&self.blockstore),
                GRACE_TICKS_FACTOR * MAX_GRACE_SLOTS,
            );
            assert_eq!(self.ticks_per_slot, bank.ticks_per_slot());
            let (leader_first_tick_height, leader_last_tick_height, grace_ticks) =
                Self::compute_leader_slot_tick_heights(next_leader_slot, self.ticks_per_slot);
            self.grace_ticks = grace_ticks;
            self.leader_first_tick_height = leader_first_tick_height;
            self.leader_last_tick_height = leader_last_tick_height;

            datapoint_info!(
                "leader-slot-start-to-cleared-elapsed-ms",
                ("slot", bank.slot(), i64),
                ("elapsed", working_bank.start.elapsed().as_millis(), i64),
            );
        }

        if let Some(ref signal) = self.clear_bank_signal {
            let _ = signal.try_send(true);
        }
    }

    pub fn would_be_leader(&self, within_next_n_ticks: u64) -> bool {
        self.has_bank()
            || self
                .leader_first_tick_height
                .map_or(false, |leader_first_tick_height| {
                    let ideal_leader_tick_height =
                        leader_first_tick_height.saturating_sub(self.grace_ticks);
                    self.tick_height + within_next_n_ticks >= ideal_leader_tick_height
                        && self.tick_height <= self.leader_last_tick_height
                })
    }

    pub fn leader_after_n_slots(&self, slots: u64) -> Option<Pubkey> {
        let current_slot = self.tick_height.saturating_sub(1) / self.ticks_per_slot;
        self.leader_schedule_cache
            .slot_leader_at(current_slot + slots, None)
    }

    pub fn next_slot_leader(&self) -> Option<Pubkey> {
        self.leader_after_n_slots(1)
    }

    pub fn bank(&self) -> Option<Arc<Bank>> {
        self.working_bank.as_ref().map(|w| w.bank.clone())
    }

    pub fn bank_start(&self) -> Option<BankStart> {
        self.working_bank
            .as_ref()
            .map(|w| (w.bank.clone(), w.start.clone()))
    }

    pub fn has_bank(&self) -> bool {
        self.working_bank.is_some()
    }

    pub fn tick_height(&self) -> u64 {
        self.tick_height
    }

    pub fn ticks_per_slot(&self) -> u64 {
        self.ticks_per_slot
    }

    pub fn recorder(&self) -> TransactionRecorder {
        TransactionRecorder::new(self.record_sender.clone(), self.is_exited.clone())
    }

    fn is_same_fork_as_previous_leader(&self, slot: Slot) -> bool {
        (slot.saturating_sub(NUM_CONSECUTIVE_LEADER_SLOTS)..slot).any(|slot| {
            // Check if the last slot Poh reset to was any of the
            // previous leader's slots.
            // If so, PoH is currently building on the previous leader's blocks
            // If not, PoH is building on a different fork
            slot == self.start_slot
        })
    }

    fn prev_slot_was_mine(&self, current_slot: Slot) -> bool {
        if let Some(leader_id) = self
            .leader_schedule_cache
            .slot_leader_at(current_slot.saturating_sub(1), None)
        {
            leader_id == self.id
        } else {
            false
        }
    }

    fn reached_leader_tick(&self, leader_first_tick_height: u64) -> bool {
        let target_tick_height = leader_first_tick_height.saturating_sub(1);
        let ideal_target_tick_height = target_tick_height.saturating_sub(self.grace_ticks);
        let current_slot = self.tick_height / self.ticks_per_slot;
        // We've approached target_tick_height OR poh was reset to run immediately
        // Or, previous leader didn't transmit in any of its leader slots, so ignore grace ticks
        self.tick_height >= target_tick_height
            || self.start_tick_height + self.grace_ticks == leader_first_tick_height
            || (self.tick_height >= ideal_target_tick_height
                && (self.prev_slot_was_mine(current_slot)
                    || !self.is_same_fork_as_previous_leader(current_slot)))
    }

    pub fn last_reset_slot(&self) -> Slot {
        self.start_slot
    }

    /// returns if leader slot has been reached, how many grace ticks were afforded,
    ///   imputed leader_slot and self.start_slot
    /// reached_leader_slot() == true means "ready for a bank"
    pub fn reached_leader_slot(&self) -> (bool, u64, Slot, Slot) {
        trace!(
            "tick_height {}, start_tick_height {}, leader_first_tick_height {:?}, grace_ticks {}, has_bank {}",
            self.tick_height,
            self.start_tick_height,
            self.leader_first_tick_height,
            self.grace_ticks,
            self.has_bank()
        );

        let next_tick_height = self.tick_height + 1;
        let next_leader_slot = (next_tick_height - 1) / self.ticks_per_slot;
        if let Some(leader_first_tick_height) = self.leader_first_tick_height {
            let target_tick_height = leader_first_tick_height.saturating_sub(1);
            if self.reached_leader_tick(leader_first_tick_height) {
                assert!(next_tick_height >= self.start_tick_height);
                let ideal_target_tick_height = target_tick_height.saturating_sub(self.grace_ticks);

                return (
                    true,
                    self.tick_height.saturating_sub(ideal_target_tick_height),
                    next_leader_slot,
                    self.start_slot,
                );
            }
        }
        (false, 0, next_leader_slot, self.start_slot)
    }

    // returns (leader_first_tick_height, leader_last_tick_height, grace_ticks) given the next
    //  slot this recorder will lead
    fn compute_leader_slot_tick_heights(
        next_leader_slot: Option<(Slot, Slot)>,
        ticks_per_slot: u64,
    ) -> (Option<u64>, u64, u64) {
        next_leader_slot
            .map(|(first_slot, last_slot)| {
                let leader_first_tick_height = first_slot * ticks_per_slot + 1;
                let last_tick_height = (last_slot + 1) * ticks_per_slot;
                let num_slots = last_slot - first_slot + 1;
                let grace_ticks = cmp::min(
                    ticks_per_slot * MAX_GRACE_SLOTS,
                    ticks_per_slot * num_slots / GRACE_TICKS_FACTOR,
                );
                (
                    Some(leader_first_tick_height + grace_ticks),
                    last_tick_height,
                    grace_ticks,
                )
            })
            .unwrap_or((
                None,
                0,
                cmp::min(
                    ticks_per_slot * MAX_GRACE_SLOTS,
                    ticks_per_slot * NUM_CONSECUTIVE_LEADER_SLOTS / GRACE_TICKS_FACTOR,
                ),
            ))
    }

    // synchronize PoH with a bank
    pub fn reset(
        &mut self,
        blockhash: Hash,
        start_slot: Slot,
        next_leader_slot: Option<(Slot, Slot)>,
    ) {
        self.clear_bank();
        let mut cache = vec![];
        let poh_hash = {
            let mut poh = self.poh.lock().unwrap();
            poh.reset(blockhash, self.poh_config.hashes_per_tick);
            poh.hash
        };
        info!(
            "reset poh from: {},{},{} to: {},{}",
            poh_hash, self.tick_height, self.start_slot, blockhash, start_slot
        );

        std::mem::swap(&mut cache, &mut self.tick_cache);

        self.start_slot = start_slot;
        self.tick_height = (start_slot + 1) * self.ticks_per_slot;
        self.start_tick_height = self.tick_height + 1;

        let (leader_first_tick_height, leader_last_tick_height, grace_ticks) =
            Self::compute_leader_slot_tick_heights(next_leader_slot, self.ticks_per_slot);
        self.grace_ticks = grace_ticks;
        self.leader_first_tick_height = leader_first_tick_height;
        self.leader_last_tick_height = leader_last_tick_height;
    }

    pub fn set_working_bank(&mut self, working_bank: WorkingBank) {
        trace!("new working bank");
        assert_eq!(working_bank.bank.ticks_per_slot(), self.ticks_per_slot());
        self.working_bank = Some(working_bank);
        // TODO: adjust the working_bank.start time based on number of ticks
        // that have already elapsed based on current tick height.
        let _ = self.flush_cache(false);
    }

    pub fn set_bank(&mut self, bank: &Arc<Bank>) {
        let working_bank = WorkingBank {
            bank: bank.clone(),
            start: Arc::new(Instant::now()),
            min_tick_height: bank.tick_height(),
            max_tick_height: bank.max_tick_height(),
        };
        self.set_working_bank(working_bank);
    }

    // Flush cache will delay flushing the cache for a bank until it past the WorkingBank::min_tick_height
    // On a record flush will flush the cache at the WorkingBank::min_tick_height, since a record
    // occurs after the min_tick_height was generated
    fn flush_cache(&mut self, tick: bool) -> Result<()> {
        // check_tick_height is called before flush cache, so it cannot overrun the bank
        // so a bank that is so late that it's slot fully generated before it starts recording
        // will fail instead of broadcasting any ticks
        let working_bank = self
            .working_bank
            .as_ref()
            .ok_or(PohRecorderError::MaxHeightReached)?;
        if self.tick_height < working_bank.min_tick_height {
            return Err(PohRecorderError::MinHeightNotReached);
        }
        if tick && self.tick_height == working_bank.min_tick_height {
            return Err(PohRecorderError::MinHeightNotReached);
        }

        let entry_count = self
            .tick_cache
            .iter()
            .take_while(|x| x.1 <= working_bank.max_tick_height)
            .count();
        let mut send_result: std::result::Result<(), SendError<WorkingBankEntry>> = Ok(());

        if entry_count > 0 {
            trace!(
                "flush_cache: bank_slot: {} tick_height: {} max: {} sending: {}",
                working_bank.bank.slot(),
                working_bank.bank.tick_height(),
                working_bank.max_tick_height,
                entry_count,
            );

            for tick in &self.tick_cache[..entry_count] {
                working_bank.bank.register_tick(&tick.0.hash);
                send_result = self.sender.send((working_bank.bank.clone(), tick.clone()));
                if send_result.is_err() {
                    break;
                }
            }
        }
        if self.tick_height >= working_bank.max_tick_height {
            info!(
                "poh_record: max_tick_height {} reached, clearing working_bank {}",
                working_bank.max_tick_height,
                working_bank.bank.slot()
            );
            let working_slot =
                (working_bank.max_tick_height / self.ticks_per_slot).saturating_sub(1);
            self.start_slot = working_slot;
            self.start_tick_height = working_slot * self.ticks_per_slot + 1;
            self.clear_bank();
        }
        if send_result.is_err() {
            info!("WorkingBank::sender disconnected {:?}", send_result);
            // revert the cache, but clear the working bank
            self.clear_bank();
        } else {
            // commit the flush
            let _ = self.tick_cache.drain(..entry_count);
        }

        Ok(())
    }

    pub fn tick(&mut self) {
        let now = Instant::now();
        let (poh_entry, target_time) = {
            let mut poh_l = self.poh.lock().unwrap();
            let poh_entry = poh_l.tick();
            let target_time = if poh_entry.is_some() {
                Some(poh_l.target_poh_time(self.target_ns_per_tick))
            } else {
                None
            };

            (poh_entry, target_time)
        };
        self.tick_lock_contention_us += timing::duration_as_us(&now.elapsed());
        let now = Instant::now();
        if let Some(poh_entry) = poh_entry {
            self.tick_height += 1;
            trace!("tick_height {}", self.tick_height);

            if self.leader_first_tick_height.is_none() {
                self.tick_overhead_us += timing::duration_as_us(&now.elapsed());
                return;
            }

            let entry = Entry {
                num_hashes: poh_entry.num_hashes,
                hash: poh_entry.hash,
                transactions: vec![],
            };
            self.tick_overhead_us += timing::duration_as_us(&now.elapsed());

            let now = Instant::now();
            self.tick_cache.push((entry, self.tick_height));
            let _ = self.flush_cache(true);
            self.flush_cache_tick_us += timing::duration_as_us(&now.elapsed());
            let target_time = target_time.unwrap();
            // sleep is not accurate enough to get a predictable time.
            // Kernel can not schedule the thread for a while.
            let started_waiting = Instant::now();
            while Instant::now() < target_time {
                // TODO: a caller could possibly desire to reset or record while we're spinning here
                std::hint::spin_loop();
            }
            self.total_sleep_us += started_waiting.elapsed().as_nanos() as u64 / 1000;
        }
    }

    fn report_metrics(&mut self, bank_slot: Slot) {
        if self.last_metric.elapsed().as_millis() > 1000 {
            datapoint_info!(
                "poh_recorder",
                ("slot", bank_slot, i64),
                ("tick_lock_contention", self.tick_lock_contention_us, i64),
                ("record_us", self.record_us, i64),
                ("flush_cache_no_tick_us", self.flush_cache_no_tick_us, i64),
                ("flush_cache_tick_us", self.flush_cache_tick_us, i64),
                ("prepare_send_us", self.prepare_send_us, i64),
                ("send_us", self.send_us, i64),
                ("ticks_from_record", self.ticks_from_record, i64),
                ("total_sleep_us", self.total_sleep_us, i64),
                ("tick_overhead", self.tick_overhead_us, i64),
                (
                    "record_lock_contention",
                    self.record_lock_contention_us,
                    i64
                ),
            );

            self.tick_lock_contention_us = 0;
            self.record_us = 0;
            self.tick_overhead_us = 0;
            self.total_sleep_us = 0;
            self.record_lock_contention_us = 0;
            self.flush_cache_no_tick_us = 0;
            self.flush_cache_tick_us = 0;
            self.prepare_send_us = 0;
            self.send_us = 0;
            self.ticks_from_record = 0;
            self.last_metric = Instant::now();
        }
    }

    pub fn record(
        &mut self,
        bank_slot: Slot,
        mixin: Hash,
        transactions: Vec<Transaction>,
    ) -> Result<()> {
        // Entries without transactions are used to track real-time passing in the ledger and
        // cannot be generated by `record()`
        assert!(!transactions.is_empty(), "No transactions provided");
        self.report_metrics(bank_slot);
        loop {
            let now = Instant::now();
            self.flush_cache(false)?;
            self.flush_cache_no_tick_us += timing::duration_as_us(&now.elapsed());

            let working_bank = self
                .working_bank
                .as_ref()
                .ok_or(PohRecorderError::MaxHeightReached)?;
            if bank_slot != working_bank.bank.slot() {
                return Err(PohRecorderError::MaxHeightReached);
            }

            {
                let now = Instant::now();
                let mut poh_lock = self.poh.lock().unwrap();

                self.record_lock_contention_us += timing::duration_as_us(&now.elapsed());
                let now = Instant::now();
                let res = poh_lock.record(mixin);
                drop(poh_lock);
                self.record_us += timing::duration_as_us(&now.elapsed());
                let now = Instant::now();
                if let Some(poh_entry) = res {
                    let entry = Entry {
                        num_hashes: poh_entry.num_hashes,
                        hash: poh_entry.hash,
                        transactions,
                    };
                    let bank_clone = working_bank.bank.clone();
                    self.prepare_send_us += timing::duration_as_us(&now.elapsed());
                    let now = Instant::now();
                    self.sender.send((bank_clone, (entry, self.tick_height)))?;
                    self.send_us += timing::duration_as_us(&now.elapsed());
                    return Ok(());
                }
            }
            // record() might fail if the next PoH hash needs to be a tick.  But that's ok, tick()
            // and re-record()
            self.ticks_from_record += 1;
            self.tick();
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_clear_signal(
        tick_height: u64,
        last_entry_hash: Hash,
        start_slot: Slot,
        next_leader_slot: Option<(Slot, Slot)>,
        ticks_per_slot: u64,
        id: &Pubkey,
        blockstore: &Arc<Blockstore>,
        clear_bank_signal: Option<SyncSender<bool>>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        poh_config: &Arc<PohConfig>,
        is_exited: Arc<AtomicBool>,
    ) -> (Self, Receiver<WorkingBankEntry>, CrossbeamReceiver<Record>) {
        let tick_number = 0;
        let poh = Arc::new(Mutex::new(Poh::new_with_slot_info(
            last_entry_hash,
            poh_config.hashes_per_tick,
            ticks_per_slot,
            tick_number,
        )));

        let target_ns_per_tick = PohService::target_ns_per_tick(
            ticks_per_slot,
            poh_config.target_tick_duration.as_nanos() as u64,
        );
        let (sender, receiver) = channel();
        let (record_sender, record_receiver) = unbounded();
        let (leader_first_tick_height, leader_last_tick_height, grace_ticks) =
            Self::compute_leader_slot_tick_heights(next_leader_slot, ticks_per_slot);
        (
            Self {
                poh,
                tick_height,
                tick_cache: vec![],
                working_bank: None,
                sender,
                clear_bank_signal,
                start_slot,
                start_tick_height: tick_height + 1,
                leader_first_tick_height,
                leader_last_tick_height,
                grace_ticks,
                id: *id,
                blockstore: blockstore.clone(),
                leader_schedule_cache: leader_schedule_cache.clone(),
                ticks_per_slot,
                target_ns_per_tick,
                poh_config: poh_config.clone(),
                record_lock_contention_us: 0,
                flush_cache_tick_us: 0,
                flush_cache_no_tick_us: 0,
                prepare_send_us: 0,
                send_us: 0,
                tick_lock_contention_us: 0,
                record_us: 0,
                tick_overhead_us: 0,
                total_sleep_us: 0,
                ticks_from_record: 0,
                last_metric: Instant::now(),
                record_sender,
                is_exited,
            },
            receiver,
            record_receiver,
        )
    }

    /// A recorder to synchronize PoH with the following data structures
    /// * bank - the LastId's queue is updated on `tick` and `record` events
    /// * sender - the Entry channel that outputs to the ledger
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tick_height: u64,
        last_entry_hash: Hash,
        start_slot: Slot,
        next_leader_slot: Option<(Slot, Slot)>,
        ticks_per_slot: u64,
        id: &Pubkey,
        blockstore: &Arc<Blockstore>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        poh_config: &Arc<PohConfig>,
        is_exited: Arc<AtomicBool>,
    ) -> (Self, Receiver<WorkingBankEntry>, CrossbeamReceiver<Record>) {
        Self::new_with_clear_signal(
            tick_height,
            last_entry_hash,
            start_slot,
            next_leader_slot,
            ticks_per_slot,
            id,
            blockstore,
            None,
            leader_schedule_cache,
            poh_config,
            is_exited,
        )
    }

    // Filters the return result of PohRecorder::bank_start(), returns the bank
    // if it's still processing transactions
    pub fn get_bank_still_processing_txs(bank_start: &Option<BankStart>) -> Option<&Arc<Bank>> {
        bank_start.as_ref().and_then(|(bank, bank_creation_time)| {
            if Bank::should_bank_still_be_processing_txs(bank_creation_time, bank.ns_per_slot) {
                Some(bank)
            } else {
                None
            }
        })
    }

    // Used in tests
    pub fn schedule_dummy_max_height_reached_failure(&mut self) {
        self.reset(Hash::default(), 1, None);
    }
}

pub fn create_test_recorder(
    bank: &Arc<Bank>,
    blockstore: &Arc<Blockstore>,
    poh_config: Option<PohConfig>,
) -> (
    Arc<AtomicBool>,
    Arc<Mutex<PohRecorder>>,
    PohService,
    Receiver<WorkingBankEntry>,
) {
    let exit = Arc::new(AtomicBool::new(false));
    let poh_config = Arc::new(poh_config.unwrap_or_default());
    let (mut poh_recorder, entry_receiver, record_receiver) = PohRecorder::new(
        bank.tick_height(),
        bank.last_blockhash(),
        bank.slot(),
        Some((4, 4)),
        bank.ticks_per_slot(),
        &Pubkey::default(),
        blockstore,
        &Arc::new(LeaderScheduleCache::new_from_bank(bank)),
        &poh_config,
        exit.clone(),
    );
    poh_recorder.set_bank(bank);

    let poh_recorder = Arc::new(Mutex::new(poh_recorder));
    let poh_service = PohService::new(
        poh_recorder.clone(),
        &poh_config,
        &exit,
        bank.ticks_per_slot(),
        crate::poh_service::DEFAULT_PINNED_CPU_CORE,
        crate::poh_service::DEFAULT_HASHES_PER_BATCH,
        record_receiver,
    );

    (exit, poh_recorder, poh_service, entry_receiver)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bincode::serialize,
        solana_ledger::{
            blockstore::Blockstore,
            blockstore_meta::SlotMeta,
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
            get_tmp_ledger_path,
        },
        solana_perf::test_tx::test_tx,
        solana_sdk::{clock::DEFAULT_TICKS_PER_SLOT, hash::hash},
        std::sync::mpsc::sync_channel,
    };

    #[test]
    fn test_poh_recorder_no_zero_tick() {
        let prev_hash = Hash::default();
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");

            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                Some((4, 4)),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_cache.len(), 1);
            assert_eq!(poh_recorder.tick_cache[0].1, 1);
            assert_eq!(poh_recorder.tick_height, 1);
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_poh_recorder_tick_height_is_last_tick() {
        let prev_hash = Hash::default();
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");

            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                Some((4, 4)),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            poh_recorder.tick();
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_cache.len(), 2);
            assert_eq!(poh_recorder.tick_cache[1].1, 2);
            assert_eq!(poh_recorder.tick_height, 2);
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_poh_recorder_reset_clears_cache() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                Hash::default(),
                0,
                Some((4, 4)),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_cache.len(), 1);
            poh_recorder.reset(Hash::default(), 0, Some((4, 4)));
            assert_eq!(poh_recorder.tick_cache.len(), 0);
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_poh_recorder_clear() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let prev_hash = bank.last_blockhash();
            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            let start = Arc::new(Instant::now());
            let working_bank = WorkingBank {
                bank,
                start,
                min_tick_height: 2,
                max_tick_height: 3,
            };
            poh_recorder.set_working_bank(working_bank);
            assert!(poh_recorder.working_bank.is_some());
            poh_recorder.clear_bank();
            assert!(poh_recorder.working_bank.is_none());
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_poh_recorder_tick_sent_after_min() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let prev_hash = bank.last_blockhash();
            let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            let start = Arc::new(Instant::now());
            let working_bank = WorkingBank {
                bank: bank.clone(),
                start,
                min_tick_height: 2,
                max_tick_height: 3,
            };
            poh_recorder.set_working_bank(working_bank);
            poh_recorder.tick();
            poh_recorder.tick();
            //tick height equal to min_tick_height
            //no tick has been sent
            assert_eq!(poh_recorder.tick_cache.last().unwrap().1, 2);
            assert!(entry_receiver.try_recv().is_err());

            // all ticks are sent after height > min
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_height, 3);
            assert_eq!(poh_recorder.tick_cache.len(), 0);
            let mut num_entries = 0;
            while let Ok((wbank, (_entry, _tick_height))) = entry_receiver.try_recv() {
                assert_eq!(wbank.slot(), bank.slot());
                num_entries += 1;
            }
            assert_eq!(num_entries, 3);
            assert!(poh_recorder.working_bank.is_none());
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_poh_recorder_tick_sent_upto_and_including_max() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let prev_hash = bank.last_blockhash();
            let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            poh_recorder.tick();
            poh_recorder.tick();
            poh_recorder.tick();
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_cache.last().unwrap().1, 4);
            assert_eq!(poh_recorder.tick_height, 4);

            let start = Arc::new(Instant::now());
            let working_bank = WorkingBank {
                bank,
                start,
                min_tick_height: 2,
                max_tick_height: 3,
            };
            poh_recorder.set_working_bank(working_bank);
            poh_recorder.tick();

            assert_eq!(poh_recorder.tick_height, 5);
            assert!(poh_recorder.working_bank.is_none());
            let mut num_entries = 0;
            while entry_receiver.try_recv().is_ok() {
                num_entries += 1;
            }
            assert_eq!(num_entries, 3);
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_poh_recorder_record_to_early() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let prev_hash = bank.last_blockhash();
            let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            let start = Arc::new(Instant::now());
            let working_bank = WorkingBank {
                bank: bank.clone(),
                start,
                min_tick_height: 2,
                max_tick_height: 3,
            };
            poh_recorder.set_working_bank(working_bank);
            poh_recorder.tick();
            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert!(poh_recorder.record(bank.slot(), h1, vec![tx]).is_err());
            assert!(entry_receiver.try_recv().is_err());
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_poh_recorder_record_bad_slot() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let prev_hash = bank.last_blockhash();
            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            let start = Arc::new(Instant::now());
            let working_bank = WorkingBank {
                bank: bank.clone(),
                start,
                min_tick_height: 1,
                max_tick_height: 2,
            };
            poh_recorder.set_working_bank(working_bank);
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_cache.len(), 1);
            assert_eq!(poh_recorder.tick_height, 1);
            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert_matches!(
                poh_recorder.record(bank.slot() + 1, h1, vec![tx]),
                Err(PohRecorderError::MaxHeightReached)
            );
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_poh_recorder_record_at_min_passes() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let prev_hash = bank.last_blockhash();
            let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            let start = Arc::new(Instant::now());
            let working_bank = WorkingBank {
                bank: bank.clone(),
                start,
                min_tick_height: 1,
                max_tick_height: 2,
            };
            poh_recorder.set_working_bank(working_bank);
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_cache.len(), 1);
            assert_eq!(poh_recorder.tick_height, 1);
            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert!(poh_recorder.record(bank.slot(), h1, vec![tx]).is_ok());
            assert_eq!(poh_recorder.tick_cache.len(), 0);

            //tick in the cache + entry
            let (_bank, (e, _tick_height)) = entry_receiver.recv().expect("recv 1");
            assert!(e.is_tick());
            let (_bank, (e, _tick_height)) = entry_receiver.recv().expect("recv 2");
            assert!(!e.is_tick());
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_poh_recorder_record_at_max_fails() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let prev_hash = bank.last_blockhash();
            let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            let start = Arc::new(Instant::now());
            let working_bank = WorkingBank {
                bank: bank.clone(),
                start,
                min_tick_height: 1,
                max_tick_height: 2,
            };
            poh_recorder.set_working_bank(working_bank);
            poh_recorder.tick();
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_height, 2);
            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert!(poh_recorder.record(bank.slot(), h1, vec![tx]).is_err());

            let (_bank, (entry, _tick_height)) = entry_receiver.recv().unwrap();
            assert!(entry.is_tick());
            let (_bank, (entry, _tick_height)) = entry_receiver.recv().unwrap();
            assert!(entry.is_tick());
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_poh_cache_on_disconnect() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let prev_hash = bank.last_blockhash();
            let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            let start = Arc::new(Instant::now());
            let working_bank = WorkingBank {
                bank,
                start,
                min_tick_height: 2,
                max_tick_height: 3,
            };
            poh_recorder.set_working_bank(working_bank);
            poh_recorder.tick();
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_height, 2);
            drop(entry_receiver);
            poh_recorder.tick();
            assert!(poh_recorder.working_bank.is_none());
            assert_eq!(poh_recorder.tick_cache.len(), 3);
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_current() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                Hash::default(),
                0,
                Some((4, 4)),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            poh_recorder.tick();
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_cache.len(), 2);
            let hash = poh_recorder.poh.lock().unwrap().hash;
            poh_recorder.reset(hash, 0, Some((4, 4)));
            assert_eq!(poh_recorder.tick_cache.len(), 0);
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_with_cached() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                Hash::default(),
                0,
                Some((4, 4)),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            poh_recorder.tick();
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_cache.len(), 2);
            poh_recorder.reset(poh_recorder.tick_cache[0].0.hash, 0, Some((4, 4)));
            assert_eq!(poh_recorder.tick_cache.len(), 0);
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_to_new_value() {
        solana_logger::setup();

        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                Hash::default(),
                0,
                Some((4, 4)),
                DEFAULT_TICKS_PER_SLOT,
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::default()),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            poh_recorder.tick();
            poh_recorder.tick();
            poh_recorder.tick();
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_cache.len(), 4);
            assert_eq!(poh_recorder.tick_height, 4);
            poh_recorder.reset(hash(b"hello"), 0, Some((4, 4))); // parent slot 0 implies tick_height of 3
            assert_eq!(poh_recorder.tick_cache.len(), 0);
            poh_recorder.tick();
            assert_eq!(poh_recorder.tick_height, DEFAULT_TICKS_PER_SLOT + 1);
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_reset_clear_bank() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                Hash::default(),
                0,
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            let start = Arc::new(Instant::now());
            let working_bank = WorkingBank {
                bank,
                start,
                min_tick_height: 2,
                max_tick_height: 3,
            };
            poh_recorder.set_working_bank(working_bank);
            poh_recorder.reset(hash(b"hello"), 0, Some((4, 4)));
            assert!(poh_recorder.working_bank.is_none());
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    pub fn test_clear_signal() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let (sender, receiver) = sync_channel(1);
            let (mut poh_recorder, _entry_receiver, _record_receiver) =
                PohRecorder::new_with_clear_signal(
                    0,
                    Hash::default(),
                    0,
                    None,
                    bank.ticks_per_slot(),
                    &Pubkey::default(),
                    &Arc::new(blockstore),
                    Some(sender),
                    &Arc::new(LeaderScheduleCache::default()),
                    &Arc::new(PohConfig::default()),
                    Arc::new(AtomicBool::default()),
                );
            poh_recorder.set_bank(&bank);
            poh_recorder.clear_bank();
            assert!(receiver.try_recv().is_ok());
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_poh_recorder_reset_start_slot() {
        solana_logger::setup();
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let ticks_per_slot = 5;
            let GenesisConfigInfo {
                mut genesis_config, ..
            } = create_genesis_config(2);
            genesis_config.ticks_per_slot = ticks_per_slot;
            let bank = Arc::new(Bank::new(&genesis_config));

            let prev_hash = bank.last_blockhash();
            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                Some((4, 4)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            let end_slot = 3;
            let max_tick_height = (end_slot + 1) * ticks_per_slot;
            let start = Arc::new(Instant::now());
            let working_bank = WorkingBank {
                bank: bank.clone(),
                start,
                min_tick_height: 1,
                max_tick_height,
            };

            poh_recorder.set_working_bank(working_bank);
            for _ in 0..max_tick_height {
                poh_recorder.tick();
            }

            let tx = test_tx();
            let h1 = hash(b"hello world!");
            assert!(poh_recorder.record(bank.slot(), h1, vec![tx]).is_err());
            assert!(poh_recorder.working_bank.is_none());
            // Make sure the starting slot is updated
            assert_eq!(poh_recorder.start_slot, end_slot);
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_reached_leader_tick() {
        solana_logger::setup();

        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let prev_hash = bank.last_blockhash();
            let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));
            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                None,
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &leader_schedule_cache,
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            let bootstrap_validator_id = leader_schedule_cache.slot_leader_at(0, None).unwrap();

            assert!(poh_recorder.reached_leader_tick(0));

            let grace_ticks = bank.ticks_per_slot() * MAX_GRACE_SLOTS;
            let new_tick_height = NUM_CONSECUTIVE_LEADER_SLOTS * bank.ticks_per_slot();
            for _ in 0..new_tick_height {
                poh_recorder.tick();
            }

            poh_recorder.grace_ticks = grace_ticks;

            // False, because the Poh was reset on slot 0, which
            // is a block produced by the previous leader, so a grace
            // period must be given
            assert!(!poh_recorder.reached_leader_tick(new_tick_height + grace_ticks));

            // Tick `NUM_CONSECUTIVE_LEADER_SLOTS` more times
            let new_tick_height = 2 * NUM_CONSECUTIVE_LEADER_SLOTS * bank.ticks_per_slot();
            for _ in 0..new_tick_height {
                poh_recorder.tick();
            }
            // True, because
            // 1) the Poh was reset on slot 0
            // 2) Our slot starts at 2 * NUM_CONSECUTIVE_LEADER_SLOTS, which means
            // none of the previous leader's `NUM_CONSECUTIVE_LEADER_SLOTS` were slots
            // this Poh built on (previous leader was on different fork). Thus, skip the
            // grace period.
            assert!(poh_recorder.reached_leader_tick(new_tick_height + grace_ticks));

            // From the bootstrap validator's perspective, it should have reached
            // the tick because the previous slot was also it's own slot (all slots
            // belong to the bootstrap leader b/c it's the only staked node!), and
            // validators don't give grace periods if previous slot was also their own.
            poh_recorder.id = bootstrap_validator_id;
            assert!(poh_recorder.reached_leader_tick(new_tick_height + grace_ticks));
        }
    }

    #[test]
    fn test_reached_leader_slot() {
        solana_logger::setup();

        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let prev_hash = bank.last_blockhash();
            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                None,
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            // Test that with no next leader slot, we don't reach the leader slot
            assert!(!poh_recorder.reached_leader_slot().0);

            // Test that with no next leader slot in reset(), we don't reach the leader slot
            poh_recorder.reset(bank.last_blockhash(), 0, None);
            assert!(!poh_recorder.reached_leader_slot().0);

            // Provide a leader slot one slot down
            poh_recorder.reset(bank.last_blockhash(), 0, Some((2, 2)));

            let init_ticks = poh_recorder.tick_height();

            // Send one slot worth of ticks
            for _ in 0..bank.ticks_per_slot() {
                poh_recorder.tick();
            }

            // Tick should be recorded
            assert_eq!(
                poh_recorder.tick_height(),
                init_ticks + bank.ticks_per_slot()
            );

            let parent_meta = SlotMeta {
                received: 1,
                ..SlotMeta::default()
            };
            poh_recorder
                .blockstore
                .put_meta_bytes(0, &serialize(&parent_meta).unwrap())
                .unwrap();

            // Test that we don't reach the leader slot because of grace ticks
            assert!(!poh_recorder.reached_leader_slot().0);

            // reset poh now. we should immediately be leader
            poh_recorder.reset(bank.last_blockhash(), 1, Some((2, 2)));
            let (reached_leader_slot, grace_ticks, leader_slot, ..) =
                poh_recorder.reached_leader_slot();
            assert!(reached_leader_slot);
            assert_eq!(grace_ticks, 0);
            assert_eq!(leader_slot, 2);

            // Now test that with grace ticks we can reach leader slot
            // Set the leader slot one slot down
            poh_recorder.reset(bank.last_blockhash(), 1, Some((3, 3)));

            // Send one slot worth of ticks ("skips" slot 2)
            for _ in 0..bank.ticks_per_slot() {
                poh_recorder.tick();
            }

            // We are not the leader yet, as expected
            assert!(!poh_recorder.reached_leader_slot().0);

            // Send the grace ticks
            for _ in 0..bank.ticks_per_slot() / GRACE_TICKS_FACTOR {
                poh_recorder.tick();
            }

            // We should be the leader now
            let (reached_leader_slot, grace_ticks, leader_slot, ..) =
                poh_recorder.reached_leader_slot();
            assert!(reached_leader_slot);
            assert_eq!(grace_ticks, bank.ticks_per_slot() / GRACE_TICKS_FACTOR);
            assert_eq!(leader_slot, 3);

            // Let's test that correct grace ticks are reported
            // Set the leader slot one slot down
            poh_recorder.reset(bank.last_blockhash(), 2, Some((4, 4)));

            // send ticks for a slot
            for _ in 0..bank.ticks_per_slot() {
                poh_recorder.tick();
            }

            // We are not the leader yet, as expected
            assert!(!poh_recorder.reached_leader_slot().0);
            poh_recorder.reset(bank.last_blockhash(), 3, Some((4, 4)));

            // without sending more ticks, we should be leader now
            let (reached_leader_slot, grace_ticks, leader_slot, ..) =
                poh_recorder.reached_leader_slot();
            assert!(reached_leader_slot);
            assert_eq!(grace_ticks, 0);
            assert_eq!(leader_slot, 4);

            // Let's test that if a node overshoots the ticks for its target
            // leader slot, reached_leader_slot() will return true, because it's overdue
            // Set the leader slot one slot down
            poh_recorder.reset(bank.last_blockhash(), 4, Some((5, 5)));

            // Overshoot ticks for the slot
            let overshoot_factor = 4;
            for _ in 0..overshoot_factor * bank.ticks_per_slot() {
                poh_recorder.tick();
            }

            // We are overdue to lead
            let (reached_leader_slot, grace_ticks, leader_slot, ..) =
                poh_recorder.reached_leader_slot();
            assert!(reached_leader_slot);
            assert_eq!(grace_ticks, overshoot_factor * bank.ticks_per_slot());
            assert_eq!(leader_slot, 9);
        }
        Blockstore::destroy(&ledger_path).unwrap();
    }

    #[test]
    fn test_would_be_leader_soon() {
        let ledger_path = get_tmp_ledger_path!();
        {
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let prev_hash = bank.last_blockhash();
            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                prev_hash,
                0,
                None,
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );

            // Test that with no leader slot, we don't reach the leader tick
            assert!(!poh_recorder.would_be_leader(2 * bank.ticks_per_slot()));

            poh_recorder.reset(bank.last_blockhash(), 0, None);

            assert!(!poh_recorder.would_be_leader(2 * bank.ticks_per_slot()));

            // We reset with leader slot after 3 slots
            let bank_slot = bank.slot() + 3;
            poh_recorder.reset(bank.last_blockhash(), 0, Some((bank_slot, bank_slot)));

            // Test that the node won't be leader in next 2 slots
            assert!(!poh_recorder.would_be_leader(2 * bank.ticks_per_slot()));

            // Test that the node will be leader in next 3 slots
            assert!(poh_recorder.would_be_leader(3 * bank.ticks_per_slot()));

            assert!(!poh_recorder.would_be_leader(2 * bank.ticks_per_slot()));

            // Move the bank up a slot (so that max_tick_height > slot 0's tick_height)
            let bank = Arc::new(Bank::new_from_parent(&bank, &Pubkey::default(), 1));
            // If we set the working bank, the node should be leader within next 2 slots
            poh_recorder.set_bank(&bank);
            assert!(poh_recorder.would_be_leader(2 * bank.ticks_per_slot()));
        }
    }

    #[test]
    fn test_flush_virtual_ticks() {
        let ledger_path = get_tmp_ledger_path!();
        {
            // test that virtual ticks are flushed into a newly set bank asap
            let blockstore = Blockstore::open(&ledger_path)
                .expect("Expected to be able to open database ledger");
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
            let bank = Arc::new(Bank::new(&genesis_config));
            let genesis_hash = bank.last_blockhash();

            let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
                0,
                bank.last_blockhash(),
                0,
                Some((2, 2)),
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &Arc::new(PohConfig::default()),
                Arc::new(AtomicBool::default()),
            );
            //create a new bank
            let bank = Arc::new(Bank::new_from_parent(&bank, &Pubkey::default(), 2));
            //put 2 slots worth of virtual ticks into poh
            for _ in 0..(bank.ticks_per_slot() * 2) {
                poh_recorder.tick();
            }
            poh_recorder.set_bank(&bank);
            assert_eq!(Some(false), bank.check_hash_age(&genesis_hash, 1));
        }
    }

    #[test]
    fn test_compute_leader_slot_tick_heights() {
        assert_eq!(
            PohRecorder::compute_leader_slot_tick_heights(None, 0),
            (None, 0, 0)
        );

        assert_eq!(
            PohRecorder::compute_leader_slot_tick_heights(Some((4, 4)), 8),
            (Some(37), 40, 4)
        );

        assert_eq!(
            PohRecorder::compute_leader_slot_tick_heights(Some((4, 7)), 8),
            (Some(49), 64, 2 * 8)
        );

        assert_eq!(
            PohRecorder::compute_leader_slot_tick_heights(Some((6, 7)), 8),
            (Some(57), 64, 8)
        );

        assert_eq!(
            PohRecorder::compute_leader_slot_tick_heights(Some((6, 7)), 4),
            (Some(29), 32, 4)
        );
    }
}
