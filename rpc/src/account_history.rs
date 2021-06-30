use {
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_measure::measure::Measure,
    solana_metrics::datapoint_info,
    solana_runtime::bank::Bank,
    solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey},
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock, RwLockWriteGuard,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

pub type AccountHistory = BTreeMap<Slot, HashMap<Pubkey, Account>>;
pub type AccountKeys = HashSet<Pubkey>;

pub struct AccountHistoryService {
    thread_hdl: JoinHandle<()>,
}

impl AccountHistoryService {
    pub fn new(
        num_slots: usize,
        account_keys: Arc<RwLock<AccountKeys>>,
        account_history: Arc<RwLock<AccountHistory>>,
        account_history_receiver: Receiver<Arc<Bank>>,
        exit: &Arc<AtomicBool>,
    ) -> Self {
        let exit = exit.clone();
        let thread_hdl = Builder::new()
            .name("solana-account-history".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                if let Err(RecvTimeoutError::Disconnected) = Self::receive_bank(
                    &num_slots,
                    &account_keys,
                    &account_history,
                    &account_history_receiver,
                ) {
                    break;
                }
            })
            .unwrap();
        Self { thread_hdl }
    }

    fn receive_bank(
        num_slots: &usize,
        account_keys: &Arc<RwLock<AccountKeys>>,
        account_history: &Arc<RwLock<AccountHistory>>,
        account_history_receiver: &Receiver<Arc<Bank>>,
    ) -> Result<(), RecvTimeoutError> {
        let frozen_bank = account_history_receiver.recv_timeout(Duration::from_secs(1))?;

        let r_account_keys = account_keys.read().unwrap();
        let mut measure_collect = Measure::start("collect-account-history");
        let slot_accounts = Self::collect_accounts(&frozen_bank, &r_account_keys);
        measure_collect.stop();
        drop(r_account_keys);

        let mut measure_write = Measure::start("write-account-history");
        let mut w_account_history = account_history.write().unwrap();
        w_account_history.insert(frozen_bank.slot(), slot_accounts);
        measure_write.stop();

        let mut measure_prune = Measure::start("prune-account-history");
        Self::remove_old_slots(w_account_history, num_slots);
        measure_prune.stop();

        datapoint_info!(
            "rpc_account_history",
            ("collect", measure_collect.as_us(), i64),
            ("write", measure_write.as_us(), i64),
            ("prune", measure_prune.as_us(), i64),
        );

        Ok(())
    }

    fn collect_accounts(
        frozen_bank: &Bank,
        accounts: &HashSet<Pubkey>,
    ) -> HashMap<Pubkey, Account> {
        let mut slot_accounts = HashMap::new();
        for address in accounts.iter() {
            if let Some((shared_account, slot)) = frozen_bank.get_account_modified_slot(address) {
                if slot == frozen_bank.slot() {
                    slot_accounts.insert(*address, shared_account.into());
                }
            }
        }
        slot_accounts
    }

    fn remove_old_slots(
        mut w_account_history: RwLockWriteGuard<AccountHistory>,
        num_slots: &usize,
    ) {
        while w_account_history.len() > *num_slots {
            let oldest_slot = w_account_history.keys().cloned().next().unwrap_or_default();
            w_account_history.remove(&oldest_slot);
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_old_slots() {
        let num_slots = 3;
        let account_history = RwLock::new(BTreeMap::new());
        assert_eq!(account_history.read().unwrap().len(), 0);
        AccountHistoryService::remove_old_slots(account_history.write().unwrap(), num_slots);
        assert_eq!(account_history.read().unwrap().len(), 0);

        let accounts: HashMap<Pubkey, Account> = vec![
            (Pubkey::new_unique(), Account::default()),
            (Pubkey::new_unique(), Account::default()),
        ]
        .into_iter()
        .collect();
        account_history.write().unwrap().insert(0, accounts.clone());
        assert_eq!(account_history.read().unwrap().len(), 1);
        AccountHistoryService::remove_old_slots(account_history.write().unwrap(), num_slots);
        assert_eq!(account_history.read().unwrap().len(), 1);

        for slot in 1..num_slots {
            account_history
                .write()
                .unwrap()
                .insert(slot as Slot, accounts.clone());
        }
        assert_eq!(account_history.read().unwrap().len(), num_slots);
        AccountHistoryService::remove_old_slots(account_history.write().unwrap(), num_slots);
        assert_eq!(account_history.read().unwrap().len(), num_slots);

        for slot in num_slots..num_slots + 2 {
            account_history
                .write()
                .unwrap()
                .insert(slot as Slot, accounts.clone());
        }
        assert_eq!(account_history.read().unwrap().len(), num_slots + 2);
        AccountHistoryService::remove_old_slots(account_history.write().unwrap(), num_slots);
        assert_eq!(account_history.read().unwrap().len(), num_slots);
        assert_eq!(*account_history.read().unwrap().iter().next().unwrap().0, 2);
    }
}
