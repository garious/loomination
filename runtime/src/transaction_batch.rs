use crate::bank::Bank;
use solana_sdk::hashed_transaction::HashedTransaction;
use solana_sdk::transaction::{Result, Transaction};
use std::borrow::Cow;

// Represents the results of trying to lock a set of accounts
pub struct TransactionBatch<'a, 'b> {
    lock_results: Vec<Result<()>>,
    bank: &'a Bank,
    hashed_txs: Cow<'b, [HashedTransaction<'b>]>,
    pub(crate) needs_unlock: bool,
}

impl<'a, 'b> TransactionBatch<'a, 'b> {
    pub fn new(
        lock_results: Vec<Result<()>>,
        bank: &'a Bank,
        hashed_txs: Cow<'b, [HashedTransaction<'b>]>,
    ) -> Self {
        assert_eq!(lock_results.len(), hashed_txs.len());
        Self {
            lock_results,
            bank,
            hashed_txs,
            needs_unlock: true,
        }
    }

    pub fn lock_results(&self) -> &Vec<Result<()>> {
        &self.lock_results
    }

    pub fn hashed_transactions(&self) -> &[HashedTransaction] {
        &self.hashed_txs
    }

    pub fn transactions_iter(&self) -> impl Iterator<Item = &Transaction> {
        self.hashed_txs.iter().map(|h| h.transaction())
    }

    pub fn bank(&self) -> &Bank {
        self.bank
    }
}

// Unlock all locked accounts in destructor.
impl<'a, 'b> Drop for TransactionBatch<'a, 'b> {
    fn drop(&mut self) {
        self.bank.unlock_accounts(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::genesis_utils::{create_genesis_config_with_leader, GenesisConfigInfo};
    use solana_sdk::{signature::Keypair, system_transaction};

    #[test]
    fn test_transaction_batch() {
        let (bank, txs) = setup();

        // Test getting locked accounts
        let batch = bank.prepare_batch(txs.iter());

        // Grab locks
        assert!(batch.lock_results().iter().all(|x| x.is_ok()));

        // Trying to grab locks again should fail
        let batch2 = bank.prepare_batch(txs.iter());
        assert!(batch2.lock_results().iter().all(|x| x.is_err()));

        // Drop the first set of locks
        drop(batch);

        // Now grabbing locks should work again
        let batch2 = bank.prepare_batch(txs.iter());
        assert!(batch2.lock_results().iter().all(|x| x.is_ok()));
    }

    #[test]
    fn test_simulation_batch() {
        let (bank, txs) = setup();

        // Prepare batch without locks
        let batch = bank.prepare_simulation_batch(&txs[0]);
        assert!(batch.lock_results().iter().all(|x| x.is_ok()));

        // Grab locks
        let batch2 = bank.prepare_batch(txs.iter());
        assert!(batch2.lock_results().iter().all(|x| x.is_ok()));

        // Prepare another batch without locks
        let batch3 = bank.prepare_simulation_batch(&txs[0]);
        assert!(batch3.lock_results().iter().all(|x| x.is_ok()));
    }

    fn setup() -> (Bank, Vec<Transaction>) {
        let dummy_leader_pubkey = solana_sdk::pubkey::new_rand();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(500, &dummy_leader_pubkey, 100);
        let bank = Bank::new(&genesis_config);

        let pubkey = solana_sdk::pubkey::new_rand();
        let keypair2 = Keypair::new();
        let pubkey2 = solana_sdk::pubkey::new_rand();

        let txs = vec![
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_config.hash()),
            system_transaction::transfer(&keypair2, &pubkey2, 1, genesis_config.hash()),
        ];

        (bank, txs)
    }
}
