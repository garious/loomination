//! The `system_transaction` module provides functionality for creating system transactions.

use bincode::serialize;
use hash::Hash;
use signature::{Keypair, KeypairUtil};
use solana_program_interface::pubkey::Pubkey;
use system_interpreter::SystemInterpreter;
use transaction::{Instruction, Transaction};

pub trait SystemTransaction {
    fn system_create(
        from_keypair: &Keypair,
        to: Pubkey,
        last_id: Hash,
        tokens: i64,
        space: u64,
        interpreter_id: Pubkey,
        fee: i64,
    ) -> Self;

    fn system_assign(
        from_keypair: &Keypair,
        last_id: Hash,
        interpreter_id: Pubkey,
        fee: i64,
    ) -> Self;

    fn system_new(from_keypair: &Keypair, to: Pubkey, tokens: i64, last_id: Hash) -> Self;

    fn system_move(
        from_keypair: &Keypair,
        to: Pubkey,
        tokens: i64,
        last_id: Hash,
        fee: i64,
    ) -> Self;

    fn system_load(
        from_keypair: &Keypair,
        last_id: Hash,
        fee: i64,
        interpreter_id: Pubkey,
        name: String,
    ) -> Self;
    fn system_move_many(
        from_keypair: &Keypair,
        moves: &[(Pubkey, i64)],
        last_id: Hash,
        fee: i64,
    ) -> Self;
}

impl SystemTransaction for Transaction {
    /// Create and sign new SystemInterpreter::CreateAccount transaction
    fn system_create(
        from_keypair: &Keypair,
        to: Pubkey,
        last_id: Hash,
        tokens: i64,
        space: u64,
        interpreter_id: Pubkey,
        fee: i64,
    ) -> Self {
        let create = SystemInterpreter::CreateAccount {
            tokens, //TODO, the tokens to allocate might need to be higher then 0 in the future
            space,
            interpreter_id,
        };
        let userdata = serialize(&create).unwrap();
        Transaction::new(
            from_keypair,
            &[to],
            SystemInterpreter::id(),
            userdata,
            last_id,
            fee,
        )
    }
    /// Create and sign new SystemInterpreter::Assign transaction
    fn system_assign(
        from_keypair: &Keypair,
        last_id: Hash,
        interpreter_id: Pubkey,
        fee: i64,
    ) -> Self {
        let assign = SystemInterpreter::Assign { interpreter_id };
        let userdata = serialize(&assign).unwrap();
        Transaction::new(
            from_keypair,
            &[],
            SystemInterpreter::id(),
            userdata,
            last_id,
            fee,
        )
    }
    /// Create and sign new SystemInterpreter::CreateAccount transaction with some defaults
    fn system_new(from_keypair: &Keypair, to: Pubkey, tokens: i64, last_id: Hash) -> Self {
        Transaction::system_create(from_keypair, to, last_id, tokens, 0, Pubkey::default(), 0)
    }
    /// Create and sign new SystemInterpreter::Move transaction
    fn system_move(
        from_keypair: &Keypair,
        to: Pubkey,
        tokens: i64,
        last_id: Hash,
        fee: i64,
    ) -> Self {
        let move_tokens = SystemInterpreter::Move { tokens };
        let userdata = serialize(&move_tokens).unwrap();
        Transaction::new(
            from_keypair,
            &[to],
            SystemInterpreter::id(),
            userdata,
            last_id,
            fee,
        )
    }
    /// Create and sign new SystemInterpreter::Load transaction
    fn system_load(
        from_keypair: &Keypair,
        last_id: Hash,
        fee: i64,
        interpreter_id: Pubkey,
        name: String,
    ) -> Self {
        let load = SystemInterpreter::Load {
            interpreter_id,
            name,
        };
        let userdata = serialize(&load).unwrap();
        Transaction::new(
            from_keypair,
            &[],
            SystemInterpreter::id(),
            userdata,
            last_id,
            fee,
        )
    }
    fn system_move_many(from: &Keypair, moves: &[(Pubkey, i64)], last_id: Hash, fee: i64) -> Self {
        let instructions: Vec<_> = moves
            .iter()
            .enumerate()
            .map(|(i, (_, amount))| {
                let spend = SystemInterpreter::Move { tokens: *amount };
                Instruction {
                    interpreter_ids_index: 0,
                    userdata: serialize(&spend).unwrap(),
                    accounts: vec![0, i as u8],
                }
            }).collect();
        let to_keys: Vec<_> = moves.iter().map(|(to_key, _)| *to_key).collect();

        Transaction::new_with_instructions(
            from,
            &to_keys,
            last_id,
            fee,
            vec![SystemInterpreter::id()],
            instructions,
        )
    }
}

pub fn test_tx() -> Transaction {
    let keypair1 = Keypair::new();
    let pubkey1 = keypair1.pubkey();
    let zero = Hash::default();
    Transaction::system_new(&keypair1, pubkey1, 42, zero)
}

#[cfg(test)]
pub fn memfind<A: Eq>(a: &[A], b: &[A]) -> Option<usize> {
    assert!(a.len() >= b.len());
    let end = a.len() - b.len() + 1;
    for i in 0..end {
        if a[i..i + b.len()] == b[..] {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{deserialize, serialize};
    use packet::PACKET_DATA_SIZE;
    use transaction::{PUB_KEY_OFFSET, SIGNED_DATA_OFFSET, SIG_OFFSET};

    #[test]
    fn test_layout() {
        let tx = test_tx();
        let sign_data = tx.get_sign_data();
        let tx_bytes = serialize(&tx).unwrap();
        assert_eq!(memfind(&tx_bytes, &sign_data), Some(SIGNED_DATA_OFFSET));
        assert_eq!(memfind(&tx_bytes, &tx.signature.as_ref()), Some(SIG_OFFSET));
        assert_eq!(
            memfind(&tx_bytes, &tx.account_keys[0].as_ref()),
            Some(PUB_KEY_OFFSET)
        );
        assert!(tx.verify_signature());
    }

    #[test]
    fn test_userdata_layout() {
        let mut tx0 = test_tx();
        tx0.instructions[0].userdata = vec![1, 2, 3];
        let sign_data0a = tx0.get_sign_data();
        let tx_bytes = serialize(&tx0).unwrap();
        assert!(tx_bytes.len() < PACKET_DATA_SIZE);
        assert_eq!(memfind(&tx_bytes, &sign_data0a), Some(SIGNED_DATA_OFFSET));
        assert_eq!(
            memfind(&tx_bytes, &tx0.signature.as_ref()),
            Some(SIG_OFFSET)
        );
        assert_eq!(
            memfind(&tx_bytes, &tx0.account_keys[0].as_ref()),
            Some(PUB_KEY_OFFSET)
        );
        let tx1 = deserialize(&tx_bytes).unwrap();
        assert_eq!(tx0, tx1);
        assert_eq!(tx1.instructions[0].userdata, vec![1, 2, 3]);

        tx0.instructions[0].userdata = vec![1, 2, 4];
        let sign_data0b = tx0.get_sign_data();
        assert_ne!(sign_data0a, sign_data0b);
    }
}
