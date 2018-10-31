//! The `mint` module is a library for generating the chain's genesis block.

use entry::Entry;
use hash::{hash, Hash};
use ledger::next_entries;
use ring::rand::SystemRandom;
use signature::{Keypair, KeypairUtil};
use solana_sdk::pubkey::Pubkey;
use system_transaction::SystemTransaction;
use transaction::Transaction;
use untrusted::Input;

#[derive(Serialize, Deserialize, Debug)]
pub struct Mint {
    pub pkcs8: Vec<u8>,
    pubkey: Pubkey,
    pub tokens: i64,
    pub first_leader_id: Pubkey,
    pub first_leader_tokens: i64,
}

impl Mint {
    pub fn new_with_pkcs8(
        tokens: i64,
        pkcs8: Vec<u8>,
        first_leader_id: Pubkey,
        first_leader_tokens: i64,
    ) -> Self {
        let keypair =
            Keypair::from_pkcs8(Input::from(&pkcs8)).expect("from_pkcs8 in mint pub fn new");
        let pubkey = keypair.pubkey();
        Mint {
            pkcs8,
            pubkey,
            tokens,
            first_leader_id,
            first_leader_tokens,
        }
    }

    pub fn new(tokens: i64, first_leader: Pubkey, first_leader_tokens: i64) -> Self {
        let rnd = SystemRandom::new();
        let pkcs8 = Keypair::generate_pkcs8(&rnd)
            .expect("generate_pkcs8 in mint pub fn new")
            .to_vec();
        Self::new_with_pkcs8(tokens, pkcs8, first_leader, first_leader_tokens)
    }

    pub fn seed(&self) -> Hash {
        hash(&self.pkcs8)
    }

    pub fn last_id(&self) -> Hash {
        self.create_entries().last().unwrap().id
    }

    pub fn keypair(&self) -> Keypair {
        Keypair::from_pkcs8(Input::from(&self.pkcs8)).expect("from_pkcs8 in mint pub fn keypair")
    }

    pub fn pubkey(&self) -> Pubkey {
        self.pubkey
    }

    pub fn create_transaction(&self) -> Vec<Transaction> {
        let keypair = self.keypair();
        // Create moves from mint to itself (deposit), and then a move from the mint
        // to the first leader
        let moves = vec![
            (self.pubkey(), self.tokens),
            (self.first_leader_id, self.first_leader_tokens),
        ];
        vec![Transaction::system_move_many(
            &keypair,
            &moves,
            self.seed(),
            0,
        )]
    }

    pub fn create_entries(&self) -> Vec<Entry> {
        let e0 = Entry::new(&self.seed(), 0, vec![]);

        // Create the transactions that give the mint the initial tokens, and gives the first
        // leader the initial tokens
        let e1 = Entry::new(&self.seed(), 0, self.create_transaction());
        vec![e0, e1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::deserialize;
    use ledger::Block;
    use system_program::SystemProgram;

    #[test]
    fn test_create_transactions() {
        let dummy_leader_id = Keypair::new().pubkey();
        let dummy_leader_tokens = 1;
        let mut transactions = Mint::new(100, dummy_leader_id, dummy_leader_tokens)
            .create_transaction()
            .into_iter();
        let tx = transactions.next().unwrap();
        assert_eq!(tx.instructions.len(), 2);
        assert!(SystemProgram::check_id(tx.program_id(0)));
        assert!(SystemProgram::check_id(tx.program_id(1)));
        let instruction: SystemProgram = deserialize(tx.userdata(0)).unwrap();
        if let SystemProgram::Move { tokens } = instruction {
            assert_eq!(tokens, 100);
        }
        let instruction: SystemProgram = deserialize(tx.userdata(1)).unwrap();
        if let SystemProgram::Move { tokens } = instruction {
            assert_eq!(tokens, 1);
        }
        assert_eq!(transactions.next(), None);
    }

    #[test]
    fn test_verify_entries() {
        let dummy_leader_id = Keypair::new().pubkey();
        let dummy_leader_tokens = 1;
        let entries = Mint::new(100, dummy_leader_id, dummy_leader_tokens).create_entries();
        assert!(entries[..].verify(&entries[0].id));
    }
}
