//! this service receives transactions that have been successfully added
//! to a bank from banking_stage, it therefore calculates and applies the
//! costs of these transactions to cost_tracker.
//! This process has some overhead, being in its own service thread is to
//! minimize impact on main TPU threads.

use crate::cost_tracker::CostTracker;
use solana_measure::measure::Measure;
use solana_runtime::bank::TransactionExecutionResult;
use solana_sdk::{clock::Slot, timing::timestamp, transaction::Transaction};
use std::{
    cmp,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::Receiver,
        Arc, RwLock,
    },
    thread::{self, Builder, JoinHandle},
    time::Duration,
};

#[derive(Default)]
pub struct BlockGenerationCostTrackingServiceStats {
    last_print: u64,
    reset_cost_tracker_count: u64,
    cost_tracker_update_count: u64,
    cost_tracker_update_elapsed: u64,
}

impl BlockGenerationCostTrackingServiceStats {
    fn update(&mut self, cost_tracker_update_count: u64, cost_tracker_update_elapsed: u64) {
        self.cost_tracker_update_count += cost_tracker_update_count;
        self.cost_tracker_update_elapsed += cost_tracker_update_elapsed;

        let now = timestamp();
        let elapsed_ms = now - self.last_print;
        if elapsed_ms > 1000 {
            datapoint_info!(
                "cost-tracking-service-stats",
                (
                    "reset_cost_tracker_count",
                    self.reset_cost_tracker_count as i64,
                    i64
                ),
                (
                    "cost_tracker_update_count",
                    self.cost_tracker_update_count as i64,
                    i64
                ),
                (
                    "cost_tracker_update_elapsed",
                    self.cost_tracker_update_elapsed as i64,
                    i64
                ),
            );

            *self = BlockGenerationCostTrackingServiceStats::default();
            self.last_print = now;
        }
    }
}

pub struct CommittedTransactionBatch {
    pub slot: Slot,
    pub transactions: Vec<Transaction>,
    pub execution_results: Vec<TransactionExecutionResult>,
}

pub type BlockGenerationCostTrackingReceiver = Receiver<CommittedTransactionBatch>;

pub struct BlockGenerationCostTrackingService {
    thread_hdl: JoinHandle<()>,
}

impl BlockGenerationCostTrackingService {
    pub fn new(
        exit: Arc<AtomicBool>,
        cost_tracker: Arc<RwLock<CostTracker>>,
        cost_tracking_receiver: BlockGenerationCostTrackingReceiver,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("solana-cost-tracking-service".to_string())
            .spawn(move || {
                Self::service_loop(exit, cost_tracker, cost_tracking_receiver);
            })
            .unwrap();

        Self { thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }

    fn service_loop(
        exit: Arc<AtomicBool>,
        cost_tracker: Arc<RwLock<CostTracker>>,
        cost_tracking_receiver: BlockGenerationCostTrackingReceiver,
    ) {
        let mut cost_tracking_service_stats = BlockGenerationCostTrackingServiceStats::default();
        let wait_timer = Duration::from_millis(10);

        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            let mut cost_tracker_update_time = Measure::start("cost_tracker_update_time");
            let mut cost_tracker_update_count = 0_u64;
            for batch in cost_tracking_receiver.try_iter() {
                cost_tracker_update_count += batch.transactions.len() as u64;
                Self::process_batch(&cost_tracker, &batch, &mut cost_tracking_service_stats);
            }
            cost_tracker_update_time.stop();
            debug!("cost_update_loop cleared update channel");

            cost_tracking_service_stats
                .update(cost_tracker_update_count, cost_tracker_update_time.as_us());

            thread::sleep(wait_timer);
        }
    }

    fn process_batch(
        cost_tracker: &RwLock<CostTracker>,
        batch: &CommittedTransactionBatch,
        cost_tracking_service_stats: &mut BlockGenerationCostTrackingServiceStats,
    ) {
        debug!(
            "cost_tracking_service processes a batch size {} slot {:?}",
            batch.transactions.len(),
            batch.slot
        );

        let current_slot = cost_tracker.read().unwrap().get_current_slot();
        match batch.slot.cmp(&current_slot) {
            cmp::Ordering::Less => {
                debug!(
                    "cost_tracking_service ignores update for old slot {:?}, current slot {:?}",
                    batch.slot, current_slot
                );
                return;
            }
            cmp::Ordering::Greater => {
                debug!(
                    "cost_tracking_service received updates implicitly reset slot from {:?} to {:?}",
                    current_slot, batch.slot
                );
                cost_tracker.write().unwrap().reset_if_new_bank(batch.slot);
                cost_tracking_service_stats.reset_cost_tracker_count += 1;
                // continue to update tracker for new slot
            }
            _ => (),
        }

        // only track the cost of transactions that were successfully executed and committed to
        // bank
        for ((result, _), tx) in batch
            .execution_results
            .iter()
            .zip(batch.transactions.iter())
        {
            if result.is_err() {
                continue;
            }
            cost_tracker.write().unwrap().add_transaction_cost(tx);
            debug!(
                "cost_update_loop updated for transaction {:?}, block cost {:?}",
                tx,
                cost_tracker.read().unwrap().get_stats()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost_model::CostModel;
    use solana_sdk::{
        hash::Hash, pubkey::Pubkey, signature::Keypair, system_transaction,
        transaction::TransactionError,
    };

    #[test]
    fn test_cost_tracking_service_process_batch() {
        let mint_keypair = Keypair::new();
        let start_hash = Hash::new_unique();
        let mut cost_tracking_service_stats = BlockGenerationCostTrackingServiceStats::default();

        // make three simple transfer transactions, the second one is not ok()
        let transactions: Vec<_> = vec![
            system_transaction::transfer(&mint_keypair, &Pubkey::new_unique(), 1, start_hash),
            system_transaction::transfer(&mint_keypair, &Pubkey::new_unique(), 2, start_hash),
            system_transaction::transfer(&mint_keypair, &Pubkey::new_unique(), 3, start_hash),
        ];
        let execution_results: Vec<TransactionExecutionResult> = vec![
            (Ok(()), None),
            (Err(TransactionError::AccountNotFound), None),
            (Ok(()), None),
        ];
        let batch = CommittedTransactionBatch {
            slot: 0_u64,
            transactions,
            execution_results,
        };

        let cost_tracker = Arc::new(RwLock::new(CostTracker::new(Arc::new(RwLock::new(
            CostModel::default(),
        )))));
        BlockGenerationCostTrackingService::process_batch(
            &cost_tracker,
            &batch,
            &mut cost_tracking_service_stats,
        );
        let cost_stats = cost_tracker.read().unwrap().get_stats();

        assert_eq!(0, cost_stats.bank_slot);
        // each transfer tx has account access cost of 58 and 0 execution cost, 2 OK TXs
        assert_eq!(116, cost_stats.total_cost);
        // shared mint account, plus 2 transfer accounts
        assert_eq!(3, cost_stats.number_of_accounts);
        // the costest account is the mint account, which has both TXs
        assert_eq!(116, cost_stats.costliest_account_cost);
    }

    #[test]
    fn test_cost_tracking_old_slot_update() {
        let mint_keypair = Keypair::new();
        let start_hash = Hash::new_unique();
        let mut cost_tracking_service_stats = BlockGenerationCostTrackingServiceStats::default();

        // make three simple transfer transactions, the second one is not ok()
        let transactions: Vec<_> = vec![
            system_transaction::transfer(&mint_keypair, &Pubkey::new_unique(), 1, start_hash),
            system_transaction::transfer(&mint_keypair, &Pubkey::new_unique(), 2, start_hash),
            system_transaction::transfer(&mint_keypair, &Pubkey::new_unique(), 3, start_hash),
        ];
        let execution_results: Vec<TransactionExecutionResult> = vec![
            (Ok(()), None),
            (Err(TransactionError::AccountNotFound), None),
            (Ok(()), None),
        ];
        // make an update with slot == 9
        let batch = CommittedTransactionBatch {
            slot: 9_u64,
            transactions,
            execution_results,
        };

        let cost_tracker = Arc::new(RwLock::new(CostTracker::new(Arc::new(RwLock::new(
            CostModel::default(),
        )))));
        // set cost track curernt slot to 10
        cost_tracker.write().unwrap().reset_if_new_bank(10_u64);

        BlockGenerationCostTrackingService::process_batch(
            &cost_tracker,
            &batch,
            &mut cost_tracking_service_stats,
        );
        let cost_stats = cost_tracker.read().unwrap().get_stats();

        // update should be ignored
        assert_eq!(10, cost_stats.bank_slot);
        assert_eq!(0, cost_stats.total_cost);
        assert_eq!(0, cost_stats.number_of_accounts);
        assert_eq!(0, cost_stats.costliest_account_cost);
    }

    #[test]
    fn test_cost_tracking_new_slot_update() {
        let mint_keypair = Keypair::new();
        let start_hash = Hash::new_unique();
        let mut cost_tracking_service_stats = BlockGenerationCostTrackingServiceStats::default();

        // make three simple transfer transactions, the second one is not ok()
        let transactions: Vec<_> = vec![
            system_transaction::transfer(&mint_keypair, &Pubkey::new_unique(), 1, start_hash),
            system_transaction::transfer(&mint_keypair, &Pubkey::new_unique(), 2, start_hash),
            system_transaction::transfer(&mint_keypair, &Pubkey::new_unique(), 3, start_hash),
        ];
        let execution_results: Vec<TransactionExecutionResult> = vec![
            (Ok(()), None),
            (Err(TransactionError::AccountNotFound), None),
            (Ok(()), None),
        ];
        // make an update with slot == 9
        let batch = CommittedTransactionBatch {
            slot: 9_u64,
            transactions,
            execution_results,
        };

        let cost_tracker = Arc::new(RwLock::new(CostTracker::new(Arc::new(RwLock::new(
            CostModel::default(),
        )))));

        BlockGenerationCostTrackingService::process_batch(
            &cost_tracker,
            &batch,
            &mut cost_tracking_service_stats,
        );
        let cost_stats = cost_tracker.read().unwrap().get_stats();

        // update should be ignored
        assert_eq!(9, cost_stats.bank_slot);
        assert_eq!(116, cost_stats.total_cost);
        assert_eq!(3, cost_stats.number_of_accounts);
        assert_eq!(116, cost_stats.costliest_account_cost);
    }
}
