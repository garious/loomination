use crate::cluster_info::ClusterInfo;
use solana_ledger::bank_forks::BankForks;
use solana_sdk::{
    instruction_processor_utils::limited_deserialize, slot_hashes::SlotHashes,
    slot_history::SlotHistory, timing::timestamp,
};
use solana_vote_program::vote_state::VoteState;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, RwLock,
};

//8 hours
pub const TIMEOUT_MS: usize = 1_000*60*60*8;

struct WatchdogService {
    t_dog: JoinHandle<Result<()>>,
}

struct Watchdog {
    cluster_info: Arc<RwLock<ClusterInfo>>,
    bank_forks: Arc<RwLock<BankForks>>,
    slot_heat_map: HashMap<Slot, (u64, HashSet<Pubkey>)>,
    hash_heat_map: HashMap<Hash, (u64, HashSet<Pubkey>)>,
    observed_hashes: HashMap<Hash, (u64, HashSet<Slot>)>,
    observed_slots: HashMap<Slot, (u64, HashSet<Hash>)>,
    since_votes: u64,
}

const MIN_CLUSTER_AGREEMENT: f64 = 2f64/3f64;

impl Watchdog {
    fn verify(&mut self) -> bool {
        self.read_bank_forks();
        self.read_cluster_votes();

       let vote_accounts: HashMap<Pubkey, (u64, Account)> = self
            .bank_forks
            .read()
            .unwrap()
            .working_bank()
            .epoch_vote_accounts();
        self.gc();
        self.filter_known();
        let leftovers = self.compute_unknown_slots();
    }

    fn gc(&mut self) {
        let now = timestamp();
        self.slot_heat_map.retain(|v| v.0 > now - TIMEOUT_MS);
        self.hash_heat_map.retain(|v| v.0 > now - TIMEOUT_MS);
        self.observed_hashes.retain(|v| v.0 > now - TIMEOUT_MS);
        self.observed_slots.retain(|v| v.0 > now - TIMEOUT_MS);
    }

    fn read_bank_forks(&mut self) {
        let now = timestamp();
        let frozen = self.bank_forks
                .read()
                .unwrap()
                .frozen_banks();
        for b in frozen.iter() {
            if self.observed_hashes.contains(b.hash()) {
                continue;
            }
            let s = b.slot();
            let h = b.hash();
            self.observed_slots.entry(s).or_default().insert(h); 
            self.observed_hashes.entry(h).or_default().insert(s); 
            self.observed_slots.entry(s).or_default().0 = now;
            self.observed_hashes.entry(h).or_default().0 = now;
            if self.observed_hashes.contains(b.parent().hash()) {
                continue;
            }
            let slot_hashes = b
                .get_sysvar_account(&sysvar::slot_hashes::id())
                .map(|account| SlotHashes::from_account(&account).unwrap())
                .unwrap_or_default();
            for (s,h) in slot_hashes {
                self.observed_slots.entry(s).or_default().1.insert(h); 
                self.observed_hashes.entry(h).or_default().1.insert(s); 
                self.observed_slots.entry(s).or_default().0 = now;
                self.observed_hashes.entry(h).or_default().0 = now;
            }
        }
    }

    fn read_cluster_votes(&mut self) {
        let (votes, ts) = self
            .cluster_info
            .read()
            .unwrap()
            .get_votes(&self.since_votes);
        self.since_votes = ts;
        let new_votes = Self::collect_votes(votes);
        self.update_slot_heat_map(&new_votes);
        self.update_hash_heat_map(&new_votes);
    }
 
    fn update_hash_heat_map(&mut self,
        votes: &HashMap<Pubkey, Vec<Vote>>,
    ) -> HashMap<Hash, u64> {
        let now = timestamp();
        for (key, val) in votes {
            let hashes: HashSet<Hash> = val
                .votes
                .iter()
                .flat_map(|v| {
                    let mut hss = vec![v.hash];
                    if hash_slots[v.hash] == v.slots[0] {
                        hss.extend(v.slots.iter().flat_map(|s| self.observed_slots.get(s).flat_map(|h| h.1.iter())))
                    }
                    hss
                })
                .collect();
            for hash in hashes {
                self.hash_heat_map.entry(hash).or_default().1.insert(key);
                self.hash_heat_map.entry(hash).or_default().0 = now;
            }
        }
    }

    fn update_vote_heat_map(
        &mut self,
        votes: &HashMap<Pubkey, Vec<Vote>>,
    ) -> HashMap<Slot, u64> {
        let now = timestamp();
        for (key, val) in votes {
            val.votes.iter().flat_map(|v| v.slots).for_each(|slot| {
                self.slot_heat_map.entry(slot).or_default().insert(key);
                self.slot_heat_map.entry(slot).or_default().0 = now;
            });
        }
    }

    fn collect_votes(votes: Vec<Transaction>) -> HashMap<Pubkey, Vec<Vote>> {
        let mut votes = HashMap::new();
        votes.into_iter().for_each(|tx| {
            let decoded = Self::decode_votes(tx);
            decoded
                .into_iter()
                .for_each(|(key, vote)| votes.entry(key).or_insert(vec![]).push(vote));
        });
        votes
    }
    fn decode_votes(tx: Transaction) -> Vec<(Pubkey, Vote)> {
        tx.message
            .instructions
            .enumerate()
            .filter(|(i, ix)| {
                tx.message.account_keys.get(ixx.program_id_index) == solana_vote_program::id()
            })
            .filter_map(|(i, _)| {
                let VoteInstruction::Vote(vote) = limited_deserialize(tx.data(ix)).ok()?;
                Some((tx.key(i, 0)?, vote))
            })
            .collect()
    }
}

impl WatchdogService {
    fn new(
        cluster_info: Arc<RwLock<ClusterInfo>>,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let t_dog = Builder::new()
            .name("solana-watchdog".to_string())
            .spawn(move || {
                let mut dog = Watchdog {
                    cluster_info,
                    bank_forks,
                };
                loop {
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }
                    if !dog.verify() {
                        panic!("CLUSTER CONSISTENCY WATCHDOG FAILURE");
                    }
                    thread::sleep(Duration::from_millis(1000));
                }
            });
        Self { t_dog }
    }
    pub fn join(self) -> thread::Result<()> {
        self.t_dog.join()
    }
}
