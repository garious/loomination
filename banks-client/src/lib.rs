//! A client for the ledger state, from the perspective of an arbitrary validator.
//!
//! Use start_tcp_client() to create a client and then import BanksClientExt to
//! access its methods. Additional "*_with_context" methods are also available,
//! but they are undocumented, may change over time, and are generally more
//! cumbersome to use.

use borsh::BorshDeserialize;
use futures::{future::join_all, Future, FutureExt};
pub use solana_banks_interface::{BanksClient as TarpcClient, TransactionStatus};
use solana_banks_interface::{BanksRequest, BanksResponse};
use solana_program::{
    clock::Clock, clock::Slot, fee_calculator::FeeCalculator, hash::Hash, program_pack::Pack,
    pubkey::Pubkey, rent::Rent, sysvar,
};
use solana_sdk::{
    account::{from_account, Account},
    commitment_config::CommitmentLevel,
    signature::Signature,
    transaction::{self, Transaction},
    transport,
};
use std::io::{self, Error, ErrorKind};
use tarpc::{
    client::{self, channel::RequestDispatch, NewClient},
    context::{self, Context},
    rpc::{ClientMessage, Response},
    serde_transport::tcp,
    Transport,
};
use tokio::{net::ToSocketAddrs, time::Duration};
use tokio_serde::formats::Bincode;

// This exists only for backward compatibility
pub trait BanksClientExt {}

#[derive(Clone)]
pub struct BanksClient {
    inner: TarpcClient,
}

impl BanksClient {
    #[allow(clippy::new_ret_no_self)]
    pub fn new<C>(
        config: client::Config,
        transport: C,
    ) -> NewClient<TarpcClient, RequestDispatch<BanksRequest, BanksResponse, C>>
    where
        C: Transport<ClientMessage<BanksRequest>, Response<BanksResponse>>,
    {
        TarpcClient::new(config, transport)
    }

    pub fn send_transaction_with_context(
        &mut self,
        ctx: Context,
        transaction: Transaction,
    ) -> impl Future<Output = io::Result<()>> + '_ {
        self.inner.send_transaction_with_context(ctx, transaction)
    }

    pub fn get_fees_with_commitment_and_context(
        &mut self,
        ctx: Context,
        commitment: CommitmentLevel,
    ) -> impl Future<Output = io::Result<(FeeCalculator, Hash, Slot)>> + '_ {
        self.inner
            .get_fees_with_commitment_and_context(ctx, commitment)
    }

    pub fn get_transaction_status_with_context(
        &mut self,
        ctx: Context,
        signature: Signature,
    ) -> impl Future<Output = io::Result<Option<TransactionStatus>>> + '_ {
        self.inner
            .get_transaction_status_with_context(ctx, signature)
    }

    pub fn get_slot_with_context(
        &mut self,
        ctx: Context,
        commitment: CommitmentLevel,
    ) -> impl Future<Output = io::Result<Slot>> + '_ {
        self.inner.get_slot_with_context(ctx, commitment)
    }

    pub fn process_transaction_with_commitment_and_context(
        &mut self,
        ctx: Context,
        transaction: Transaction,
        commitment: CommitmentLevel,
    ) -> impl Future<Output = io::Result<Option<transaction::Result<()>>>> + '_ {
        self.inner
            .process_transaction_with_commitment_and_context(ctx, transaction, commitment)
    }

    pub fn get_account_with_commitment_and_context(
        &mut self,
        ctx: Context,
        address: Pubkey,
        commitment: CommitmentLevel,
    ) -> impl Future<Output = io::Result<Option<Account>>> + '_ {
        self.inner
            .get_account_with_commitment_and_context(ctx, address, commitment)
    }

    /// Send a transaction and return immediately. The server will resend the
    /// transaction until either it is accepted by the cluster or the transaction's
    /// blockhash expires.
    pub fn send_transaction(
        &mut self,
        transaction: Transaction,
    ) -> impl Future<Output = io::Result<()>> + '_ {
        self.send_transaction_with_context(context::current(), transaction)
    }

    /// Return the cluster clock
    pub fn get_clock(&mut self) -> impl Future<Output = io::Result<Clock>> + '_ {
        self.get_account(sysvar::clock::id()).map(|result| {
            let clock_sysvar = result?
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Clock sysvar not present"))?;
            from_account::<Clock, _>(&clock_sysvar).ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "Failed to deserialize Clock sysvar")
            })
        })
    }

    /// Return the fee parameters associated with a recent, rooted blockhash. The cluster
    /// will use the transaction's blockhash to look up these same fee parameters and
    /// use them to calculate the transaction fee.
    pub fn get_fees(
        &mut self,
    ) -> impl Future<Output = io::Result<(FeeCalculator, Hash, Slot)>> + '_ {
        self.get_fees_with_commitment_and_context(context::current(), CommitmentLevel::default())
    }

    /// Return the cluster rent
    pub fn get_rent(&mut self) -> impl Future<Output = io::Result<Rent>> + '_ {
        self.get_account(sysvar::rent::id()).map(|result| {
            let rent_sysvar = result?
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Rent sysvar not present"))?;
            from_account::<Rent, _>(&rent_sysvar).ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "Failed to deserialize Rent sysvar")
            })
        })
    }

    /// Return a recent, rooted blockhash from the server. The cluster will only accept
    /// transactions with a blockhash that has not yet expired. Use the `get_fees`
    /// method to get both a blockhash and the blockhash's last valid slot.
    pub fn get_recent_blockhash(&mut self) -> impl Future<Output = io::Result<Hash>> + '_ {
        self.get_fees().map(|result| Ok(result?.1))
    }

    /// Send a transaction and return after the transaction has been rejected or
    /// reached the given level of commitment.
    pub fn process_transaction_with_commitment(
        &mut self,
        transaction: Transaction,
        commitment: CommitmentLevel,
    ) -> impl Future<Output = transport::Result<()>> + '_ {
        let mut ctx = context::current();
        ctx.deadline += Duration::from_secs(50);
        self.process_transaction_with_commitment_and_context(ctx, transaction, commitment)
            .map(|result| match result? {
                None => {
                    Err(Error::new(ErrorKind::TimedOut, "invalid blockhash or fee-payer").into())
                }
                Some(transaction_result) => Ok(transaction_result?),
            })
    }

    /// Send a transaction and return until the transaction has been finalized or rejected.
    pub fn process_transaction(
        &mut self,
        transaction: Transaction,
    ) -> impl Future<Output = transport::Result<()>> + '_ {
        self.process_transaction_with_commitment(transaction, CommitmentLevel::default())
    }

    pub async fn process_transactions_with_commitment(
        &mut self,
        transactions: Vec<Transaction>,
        commitment: CommitmentLevel,
    ) -> transport::Result<()> {
        let mut clients: Vec<_> = transactions.iter().map(|_| self.clone()).collect();
        let futures = clients
            .iter_mut()
            .zip(transactions)
            .map(|(client, transaction)| {
                client.process_transaction_with_commitment(transaction, commitment)
            });
        let statuses = join_all(futures).await;
        statuses.into_iter().collect() // Convert Vec<Result<_, _>> to Result<Vec<_>>
    }

    /// Send transactions and return until the transaction has been finalized or rejected.
    pub fn process_transactions(
        &mut self,
        transactions: Vec<Transaction>,
    ) -> impl Future<Output = transport::Result<()>> + '_ {
        self.process_transactions_with_commitment(transactions, CommitmentLevel::default())
    }

    /// Return the most recent rooted slot height. All transactions at or below this height
    /// are said to be finalized. The cluster will not fork to a higher slot height.
    pub fn get_root_slot(&mut self) -> impl Future<Output = io::Result<Slot>> + '_ {
        self.get_slot_with_context(context::current(), CommitmentLevel::default())
    }

    /// Return the account at the given address at the slot corresponding to the given
    /// commitment level. If the account is not found, None is returned.
    pub fn get_account_with_commitment(
        &mut self,
        address: Pubkey,
        commitment: CommitmentLevel,
    ) -> impl Future<Output = io::Result<Option<Account>>> + '_ {
        self.get_account_with_commitment_and_context(context::current(), address, commitment)
    }

    /// Return the account at the given address at the time of the most recent root slot.
    /// If the account is not found, None is returned.
    pub fn get_account(
        &mut self,
        address: Pubkey,
    ) -> impl Future<Output = io::Result<Option<Account>>> + '_ {
        self.get_account_with_commitment(address, CommitmentLevel::default())
    }

    /// Return the unpacked account data at the given address
    /// If the account is not found, an error is returned
    pub fn get_packed_account_data<T: Pack>(
        &mut self,
        address: Pubkey,
    ) -> impl Future<Output = io::Result<T>> + '_ {
        self.get_account(address).map(|result| {
            let account =
                result?.ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Account not found"))?;
            T::unpack_from_slice(&account.data)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to deserialize account"))
        })
    }

    /// Return the unpacked account data at the given address
    /// If the account is not found, an error is returned
    pub fn get_account_data_with_borsh<T: BorshDeserialize>(
        &mut self,
        address: Pubkey,
    ) -> impl Future<Output = io::Result<T>> + '_ {
        self.get_account(address).map(|result| {
            let account =
                result?.ok_or_else(|| io::Error::new(io::ErrorKind::Other, "account not found"))?;
            T::try_from_slice(&account.data)
        })
    }

    /// Return the balance in lamports of an account at the given address at the slot
    /// corresponding to the given commitment level.
    pub fn get_balance_with_commitment(
        &mut self,
        address: Pubkey,
        commitment: CommitmentLevel,
    ) -> impl Future<Output = io::Result<u64>> + '_ {
        self.get_account_with_commitment_and_context(context::current(), address, commitment)
            .map(|result| Ok(result?.map(|x| x.lamports).unwrap_or(0)))
    }

    /// Return the balance in lamports of an account at the given address at the time
    /// of the most recent root slot.
    pub fn get_balance(&mut self, address: Pubkey) -> impl Future<Output = io::Result<u64>> + '_ {
        self.get_balance_with_commitment(address, CommitmentLevel::default())
    }

    /// Return the status of a transaction with a signature matching the transaction's first
    /// signature. Return None if the transaction is not found, which may be because the
    /// blockhash was expired or the fee-paying account had insufficient funds to pay the
    /// transaction fee. Note that servers rarely store the full transaction history. This
    /// method may return None if the transaction status has been discarded.
    pub fn get_transaction_status(
        &mut self,
        signature: Signature,
    ) -> impl Future<Output = io::Result<Option<TransactionStatus>>> + '_ {
        self.get_transaction_status_with_context(context::current(), signature)
    }

    /// Same as get_transaction_status, but for multiple transactions.
    pub async fn get_transaction_statuses(
        &mut self,
        signatures: Vec<Signature>,
    ) -> io::Result<Vec<Option<TransactionStatus>>> {
        // tarpc futures oddly hold a mutable reference back to the client so clone the client upfront
        let mut clients_and_signatures: Vec<_> = signatures
            .into_iter()
            .map(|signature| (self.clone(), signature))
            .collect();

        let futs = clients_and_signatures
            .iter_mut()
            .map(|(client, signature)| client.get_transaction_status(*signature));

        let statuses = join_all(futs).await;

        // Convert Vec<Result<_, _>> to Result<Vec<_>>
        statuses.into_iter().collect()
    }
}

pub async fn start_client<C>(transport: C) -> io::Result<BanksClient>
where
    C: Transport<ClientMessage<BanksRequest>, Response<BanksResponse>> + Send + 'static,
{
    Ok(BanksClient {
        inner: TarpcClient::new(client::Config::default(), transport).spawn()?,
    })
}

pub async fn start_tcp_client<T: ToSocketAddrs>(addr: T) -> io::Result<BanksClient> {
    let transport = tcp::connect(addr, Bincode::default).await?;
    Ok(BanksClient {
        inner: TarpcClient::new(client::Config::default(), transport).spawn()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_banks_server::banks_server::start_local_server;
    use solana_runtime::{
        bank::Bank, bank_forks::BankForks, commitment::BlockCommitmentCache,
        genesis_utils::create_genesis_config,
    };
    use solana_sdk::{message::Message, signature::Signer, system_instruction};
    use std::sync::{Arc, RwLock};
    use tarpc::transport;
    use tokio::{runtime::Runtime, time::sleep};

    #[test]
    fn test_banks_client_new() {
        let (client_transport, _server_transport) = transport::channel::unbounded();
        BanksClient::new(client::Config::default(), client_transport);
    }

    #[test]
    fn test_banks_server_transfer_via_server() -> io::Result<()> {
        // This test shows the preferred way to interact with BanksServer.
        // It creates a runtime explicitly (no globals via tokio macros) and calls
        // `runtime.block_on()` just once, to run all the async code.

        let genesis = create_genesis_config(10);
        let bank = Bank::new(&genesis.genesis_config);
        let slot = bank.slot();
        let block_commitment_cache = Arc::new(RwLock::new(
            BlockCommitmentCache::new_for_tests_with_slots(slot, slot),
        ));
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));

        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let mint_pubkey = genesis.mint_keypair.pubkey();
        let instruction = system_instruction::transfer(&mint_pubkey, &bob_pubkey, 1);
        let message = Message::new(&[instruction], Some(&mint_pubkey));

        Runtime::new()?.block_on(async {
            let client_transport = start_local_server(bank_forks, block_commitment_cache).await;
            let mut banks_client = start_client(client_transport).await?;

            let recent_blockhash = banks_client.get_recent_blockhash().await?;
            let transaction = Transaction::new(&[&genesis.mint_keypair], message, recent_blockhash);
            banks_client.process_transaction(transaction).await.unwrap();
            assert_eq!(banks_client.get_balance(bob_pubkey).await?, 1);
            Ok(())
        })
    }

    #[test]
    fn test_banks_server_transfer_via_client() -> io::Result<()> {
        // The caller may not want to hold the connection open until the transaction
        // is processed (or blockhash expires). In this test, we verify the
        // server-side functionality is available to the client.

        let genesis = create_genesis_config(10);
        let bank = Bank::new(&genesis.genesis_config);
        let slot = bank.slot();
        let block_commitment_cache = Arc::new(RwLock::new(
            BlockCommitmentCache::new_for_tests_with_slots(slot, slot),
        ));
        let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));

        let mint_pubkey = &genesis.mint_keypair.pubkey();
        let bob_pubkey = solana_sdk::pubkey::new_rand();
        let instruction = system_instruction::transfer(mint_pubkey, &bob_pubkey, 1);
        let message = Message::new(&[instruction], Some(mint_pubkey));

        Runtime::new()?.block_on(async {
            let client_transport = start_local_server(bank_forks, block_commitment_cache).await;
            let mut banks_client = start_client(client_transport).await?;
            let (_, recent_blockhash, last_valid_slot) = banks_client.get_fees().await?;
            let transaction = Transaction::new(&[&genesis.mint_keypair], message, recent_blockhash);
            let signature = transaction.signatures[0];
            banks_client.send_transaction(transaction).await?;

            let mut status = banks_client.get_transaction_status(signature).await?;

            while status.is_none() {
                let root_slot = banks_client.get_root_slot().await?;
                if root_slot > last_valid_slot {
                    break;
                }
                sleep(Duration::from_millis(100)).await;
                status = banks_client.get_transaction_status(signature).await?;
            }
            assert!(status.unwrap().err.is_none());
            assert_eq!(banks_client.get_balance(bob_pubkey).await?, 1);
            Ok(())
        })
    }
}
