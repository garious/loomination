#![cfg(feature = "full")]

use crate::{hash::Hash, transaction::Transaction};
use std::borrow::Cow;

/// Transaction and the hash of its message
#[derive(Debug, Clone)]
pub struct HashedTransaction<'a> {
    transaction: Cow<'a, Transaction>,
    pub message_hash: Hash,
}

impl<'a> HashedTransaction<'a> {
    pub fn new(transaction: Cow<'a, Transaction>, message_hash: Hash) -> Self {
        Self {
            transaction,
            message_hash,
        }
    }

    pub fn transaction(&self) -> &Transaction {
        self.transaction.as_ref()
    }
}

impl<'a> From<Transaction> for HashedTransaction<'_> {
    fn from(transaction: Transaction) -> Self {
        Self {
            message_hash: transaction.message().hash(),
            transaction: Cow::Owned(transaction),
        }
    }
}

impl<'a> From<&'a Transaction> for HashedTransaction<'a> {
    fn from(transaction: &'a Transaction) -> Self {
        Self {
            message_hash: transaction.message().hash(),
            transaction: Cow::Borrowed(transaction),
        }
    }
}

pub trait HashedTransactionSlice<'a> {
    fn as_transactions_iter(&'a self) -> Box<dyn Iterator<Item = &'a Transaction> + '_>;
}

impl<'a> HashedTransactionSlice<'a> for [HashedTransaction<'a>] {
    fn as_transactions_iter(&'a self) -> Box<dyn Iterator<Item = &'a Transaction> + '_> {
        Box::new(self.iter().map(|h| h.transaction.as_ref()))
    }
}
