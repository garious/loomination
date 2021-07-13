//! `cost_tracker` keeps tracking transaction cost per chained accounts as well as for entire block
//! It aggregates `cost_model`, which provides service of calculating transaction cost.
//! The main functions are:
//! - would_transaction_fit(&tx), immutable function to test if `tx` would fit into current block
//! - add_transaction_cost(&tx), mutable function to accumulate `tx` cost to tracker.
//!
use crate::cost_model::{CostModel, TransactionCost};
use solana_sdk::{clock::Slot, pubkey::Pubkey, transaction::Transaction};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

const WRITABLE_ACCOUNTS_PER_BLOCK: usize = 512;

#[derive(Debug)]
pub struct CostTracker {
    cost_model: Arc<RwLock<CostModel>>,
    account_cost_limit: u64,
    block_cost_limit: u64,
    current_bank_slot: Slot,
    cost_by_writable_accounts: HashMap<Pubkey, u64>,
    block_cost: u64,
}

impl CostTracker {
    pub fn new(cost_model: Arc<RwLock<CostModel>>) -> Self {
        let (account_cost_limit, block_cost_limit) = {
            let cost_model = cost_model.read().unwrap();
            (
                cost_model.get_account_cost_limit(),
                cost_model.get_block_cost_limit(),
            )
        };
        assert!(account_cost_limit <= block_cost_limit);
        Self {
            cost_model,
            account_cost_limit,
            block_cost_limit,
            current_bank_slot: 0,
            cost_by_writable_accounts: HashMap::with_capacity(WRITABLE_ACCOUNTS_PER_BLOCK),
            block_cost: 0,
        }
    }

    pub fn would_transaction_fit(&self, transaction: &Transaction) -> Result<(), &'static str> {
        let mut cost_model = self.cost_model.write().unwrap();
        let tx_cost = cost_model.calculate_cost(transaction);
        self.would_fit(
            &tx_cost.writable_accounts,
            &(tx_cost.account_access_cost + tx_cost.execution_cost),
        )
    }

    pub fn add_transaction_cost(&mut self, transaction: &Transaction) {
        let mut cost_model = self.cost_model.write().unwrap();
        let tx_cost = cost_model.calculate_cost(transaction);
        let cost = tx_cost.account_access_cost + tx_cost.execution_cost;
        for account_key in tx_cost.writable_accounts.iter() {
            *self
                .cost_by_writable_accounts
                .entry(*account_key)
                .or_insert(0) += cost;
        }
        self.block_cost += cost;
    }

    pub fn reset_if_new_bank(&mut self, slot: Slot) {
        if slot != self.current_bank_slot {
            self.current_bank_slot = slot;
            self.cost_by_writable_accounts.clear();
            self.block_cost = 0;
        }
    }

    pub fn get_current_slot(&self) -> Slot {
        self.current_bank_slot
    }

    pub fn try_add(&mut self, transaction_cost: &TransactionCost) -> Result<u64, &'static str> {
        let cost = transaction_cost.account_access_cost + transaction_cost.execution_cost;
        self.would_fit(&transaction_cost.writable_accounts, &cost)?;

        self.add_transaction(&transaction_cost.writable_accounts, &cost);
        Ok(self.block_cost)
    }

    fn would_fit(&self, keys: &[Pubkey], cost: &u64) -> Result<(), &'static str> {
        // check against the total package cost
        if self.block_cost + cost > self.block_cost_limit {
            return Err("would exceed block cost limit");
        }

        // check if the transaction itself is more costly than the account_cost_limit
        if *cost > self.account_cost_limit {
            return Err("Transaction is too expansive, exceeds account cost limit");
        }

        // check each account against account_cost_limit,
        for account_key in keys.iter() {
            match self.cost_by_writable_accounts.get(account_key) {
                Some(chained_cost) => {
                    if chained_cost + cost > self.account_cost_limit {
                        return Err("would exceed account cost limit");
                    } else {
                        continue;
                    }
                }
                None => continue,
            }
        }

        Ok(())
    }

    fn add_transaction(&mut self, keys: &[Pubkey], cost: &u64) {
        for account_key in keys.iter() {
            *self
                .cost_by_writable_accounts
                .entry(*account_key)
                .or_insert(0) += cost;
        }
        self.block_cost += cost;
    }
}

// CostStats can be collected by util, such as ledger_tool
#[derive(Default, Debug)]
pub struct CostStats {
    pub bank_slot: Slot,
    pub total_cost: u64,
    pub number_of_accounts: usize,
    pub costliest_account: Pubkey,
    pub costliest_account_cost: u64,
}

impl CostTracker {
    pub fn get_stats(&self) -> CostStats {
        let mut stats = CostStats {
            bank_slot: self.current_bank_slot,
            total_cost: self.block_cost,
            number_of_accounts: self.cost_by_writable_accounts.len(),
            costliest_account: Pubkey::default(),
            costliest_account_cost: 0,
        };

        for (key, cost) in self.cost_by_writable_accounts.iter() {
            if cost > &stats.costliest_account_cost {
                stats.costliest_account = *key;
                stats.costliest_account_cost = *cost;
            }
        }

        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_runtime::{
        bank::Bank,
        genesis_utils::{create_genesis_config, GenesisConfigInfo},
    };
    use solana_sdk::{
        hash::Hash,
        signature::{Keypair, Signer},
        system_transaction,
        transaction::Transaction,
    };
    use std::{cmp, sync::Arc};

    fn test_setup() -> (Keypair, Hash) {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10);
        let bank = Arc::new(Bank::new_no_wallclock_throttle(&genesis_config));
        let start_hash = bank.last_blockhash();
        (mint_keypair, start_hash)
    }

    fn build_simple_transaction(
        mint_keypair: &Keypair,
        start_hash: &Hash,
    ) -> (Transaction, Vec<Pubkey>, u64) {
        let keypair = Keypair::new();
        let simple_transaction =
            system_transaction::transfer(mint_keypair, &keypair.pubkey(), 2, *start_hash);

        (simple_transaction, vec![mint_keypair.pubkey()], 5)
    }

    #[test]
    fn test_cost_tracker_initialization() {
        let testee = CostTracker::new(Arc::new(RwLock::new(CostModel::new(10, 11))));
        assert_eq!(10, testee.account_cost_limit);
        assert_eq!(11, testee.block_cost_limit);
        assert_eq!(0, testee.cost_by_writable_accounts.len());
        assert_eq!(0, testee.block_cost);
    }

    #[test]
    fn test_cost_tracker_ok_add_one() {
        let (mint_keypair, start_hash) = test_setup();
        let (_tx, keys, cost) = build_simple_transaction(&mint_keypair, &start_hash);

        // build testee to have capacity for one simple transaction
        let mut testee = CostTracker::new(Arc::new(RwLock::new(CostModel::new(cost, cost))));
        assert!(testee.would_fit(&keys, &cost).is_ok());
        testee.add_transaction(&keys, &cost);
        assert_eq!(cost, testee.block_cost);
    }

    #[test]
    fn test_cost_tracker_ok_add_two_same_accounts() {
        let (mint_keypair, start_hash) = test_setup();
        // build two transactions with same signed account
        let (_tx1, keys1, cost1) = build_simple_transaction(&mint_keypair, &start_hash);
        let (_tx2, keys2, cost2) = build_simple_transaction(&mint_keypair, &start_hash);

        // build testee to have capacity for two simple transactions, with same accounts
        let mut testee = CostTracker::new(Arc::new(RwLock::new(CostModel::new(
            cost1 + cost2,
            cost1 + cost2,
        ))));
        {
            assert!(testee.would_fit(&keys1, &cost1).is_ok());
            testee.add_transaction(&keys1, &cost1);
        }
        {
            assert!(testee.would_fit(&keys2, &cost2).is_ok());
            testee.add_transaction(&keys2, &cost2);
        }
        assert_eq!(cost1 + cost2, testee.block_cost);
        assert_eq!(1, testee.cost_by_writable_accounts.len());
    }

    #[test]
    fn test_cost_tracker_ok_add_two_diff_accounts() {
        let (mint_keypair, start_hash) = test_setup();
        // build two transactions with diff accounts
        let (_tx1, keys1, cost1) = build_simple_transaction(&mint_keypair, &start_hash);
        let second_account = Keypair::new();
        let (_tx2, keys2, cost2) = build_simple_transaction(&second_account, &start_hash);

        // build testee to have capacity for two simple transactions, with same accounts
        let mut testee = CostTracker::new(Arc::new(RwLock::new(CostModel::new(
            cmp::max(cost1, cost2),
            cost1 + cost2,
        ))));
        {
            assert!(testee.would_fit(&keys1, &cost1).is_ok());
            testee.add_transaction(&keys1, &cost1);
        }
        {
            assert!(testee.would_fit(&keys2, &cost2).is_ok());
            testee.add_transaction(&keys2, &cost2);
        }
        assert_eq!(cost1 + cost2, testee.block_cost);
        assert_eq!(2, testee.cost_by_writable_accounts.len());
    }

    #[test]
    fn test_cost_tracker_chain_reach_limit() {
        let (mint_keypair, start_hash) = test_setup();
        // build two transactions with same signed account
        let (_tx1, keys1, cost1) = build_simple_transaction(&mint_keypair, &start_hash);
        let (_tx2, keys2, cost2) = build_simple_transaction(&mint_keypair, &start_hash);

        // build testee to have capacity for two simple transactions, but not for same accounts
        let mut testee = CostTracker::new(Arc::new(RwLock::new(CostModel::new(
            cmp::min(cost1, cost2),
            cost1 + cost2,
        ))));
        // should have room for first transaction
        {
            assert!(testee.would_fit(&keys1, &cost1).is_ok());
            testee.add_transaction(&keys1, &cost1);
        }
        // but no more sapce on the same chain (same signer account)
        {
            assert!(testee.would_fit(&keys2, &cost2).is_err());
        }
    }

    #[test]
    fn test_cost_tracker_reach_limit() {
        let (mint_keypair, start_hash) = test_setup();
        // build two transactions with diff accounts
        let (_tx1, keys1, cost1) = build_simple_transaction(&mint_keypair, &start_hash);
        let second_account = Keypair::new();
        let (_tx2, keys2, cost2) = build_simple_transaction(&second_account, &start_hash);

        // build testee to have capacity for each chain, but not enough room for both transactions
        let mut testee = CostTracker::new(Arc::new(RwLock::new(CostModel::new(
            cmp::max(cost1, cost2),
            cost1 + cost2 - 1,
        ))));
        // should have room for first transaction
        {
            assert!(testee.would_fit(&keys1, &cost1).is_ok());
            testee.add_transaction(&keys1, &cost1);
        }
        // but no more room for package as whole
        {
            assert!(testee.would_fit(&keys2, &cost2).is_err());
        }
    }

    #[test]
    fn test_cost_tracker_reset() {
        let (mint_keypair, start_hash) = test_setup();
        // build two transactions with same signed account
        let (_tx1, keys1, cost1) = build_simple_transaction(&mint_keypair, &start_hash);
        let (_tx2, keys2, cost2) = build_simple_transaction(&mint_keypair, &start_hash);

        // build testee to have capacity for two simple transactions, but not for same accounts
        let mut testee = CostTracker::new(Arc::new(RwLock::new(CostModel::new(
            cmp::min(cost1, cost2),
            cost1 + cost2,
        ))));
        // should have room for first transaction
        {
            assert!(testee.would_fit(&keys1, &cost1).is_ok());
            testee.add_transaction(&keys1, &cost1);
            assert_eq!(1, testee.cost_by_writable_accounts.len());
            assert_eq!(cost1, testee.block_cost);
        }
        // but no more sapce on the same chain (same signer account)
        {
            assert!(testee.would_fit(&keys2, &cost2).is_err());
        }
        // reset the tracker
        {
            testee.reset_if_new_bank(100);
            assert_eq!(0, testee.cost_by_writable_accounts.len());
            assert_eq!(0, testee.block_cost);
        }
        //now the second transaction can be added
        {
            assert!(testee.would_fit(&keys2, &cost2).is_ok());
        }
    }

    #[test]
    fn test_cost_tracker_try_add_is_atomic() {
        let acct1 = Pubkey::new_unique();
        let acct2 = Pubkey::new_unique();
        let acct3 = Pubkey::new_unique();
        let cost = 100;
        let account_max = cost * 2;
        let block_max = account_max * 3; // for three accts

        let mut testee = CostTracker::new(Arc::new(RwLock::new(CostModel::new(
            account_max,
            block_max,
        ))));

        // case 1: a tx writes to 3 accounts, should success, we will have:
        // | acct1 | $cost |
        // | acct2 | $cost |
        // | acct2 | $cost |
        // and block_cost = $cost
        {
            let tx_cost = TransactionCost {
                writable_accounts: vec![acct1, acct2, acct3],
                account_access_cost: 0,
                execution_cost: cost,
            };
            assert!(testee.try_add(&tx_cost).is_ok());
            let stat = testee.get_stats();
            assert_eq!(cost, stat.total_cost);
            assert_eq!(3, stat.number_of_accounts);
            assert_eq!(cost, stat.costliest_account_cost);
        }

        // case 2: add tx writes to acct2 with $cost, should succeed, result to
        // | acct1 | $cost |
        // | acct2 | $cost * 2 |
        // | acct2 | $cost |
        // and block_cost = $cost * 2
        {
            let tx_cost = TransactionCost {
                writable_accounts: vec![acct2],
                account_access_cost: 0,
                execution_cost: cost,
            };
            assert!(testee.try_add(&tx_cost).is_ok());
            let stat = testee.get_stats();
            assert_eq!(cost * 2, stat.total_cost);
            assert_eq!(3, stat.number_of_accounts);
            assert_eq!(cost * 2, stat.costliest_account_cost);
            assert_eq!(acct2, stat.costliest_account);
        }

        // case 3: add tx writes to [acct1, acct2], acct2 exceeds limit, should failed atomically,
        // we shoudl still have:
        // | acct1 | $cost |
        // | acct2 | $cost |
        // | acct2 | $cost |
        // and block_cost = $cost
        {
            let tx_cost = TransactionCost {
                writable_accounts: vec![acct1, acct2],
                account_access_cost: 0,
                execution_cost: cost,
            };
            assert!(testee.try_add(&tx_cost).is_err());
            let stat = testee.get_stats();
            assert_eq!(cost * 2, stat.total_cost);
            assert_eq!(3, stat.number_of_accounts);
            assert_eq!(cost * 2, stat.costliest_account_cost);
            assert_eq!(acct2, stat.costliest_account);
        }
    }
}
