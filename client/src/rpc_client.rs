#[allow(deprecated)]
use crate::rpc_deprecated_config::{
    RpcConfirmedBlockConfig, RpcConfirmedTransactionConfig,
    RpcGetConfirmedSignaturesForAddress2Config,
};
use {
    crate::{
        client_error::{ClientError, ClientErrorKind, Result as ClientResult},
        http_sender::HttpSender,
        mock_sender::{MockSender, Mocks},
        rpc_config::RpcAccountInfoConfig,
        rpc_config::*,
        rpc_request::{RpcError, RpcRequest, RpcResponseErrorData, TokenAccountsFilter},
        rpc_response::*,
        rpc_sender::RpcSender,
    },
    bincode::serialize,
    indicatif::{ProgressBar, ProgressStyle},
    log::*,
    serde_json::{json, Value},
    solana_account_decoder::{
        parse_token::{TokenAccountType, UiTokenAccount, UiTokenAmount},
        UiAccount, UiAccountData, UiAccountEncoding,
    },
    solana_sdk::{
        account::Account,
        clock::{Epoch, Slot, UnixTimestamp, DEFAULT_MS_PER_SLOT, MAX_HASH_AGE_IN_SECONDS},
        commitment_config::{CommitmentConfig, CommitmentLevel},
        epoch_info::EpochInfo,
        epoch_schedule::EpochSchedule,
        fee_calculator::{FeeCalculator, FeeRateGovernor},
        hash::Hash,
        pubkey::Pubkey,
        signature::Signature,
        transaction::{self, uses_durable_nonce, Transaction},
    },
    solana_transaction_status::{
        EncodedConfirmedBlock, EncodedConfirmedTransaction, TransactionStatus, UiConfirmedBlock,
        UiTransactionEncoding,
    },
    solana_vote_program::vote_state::MAX_LOCKOUT_HISTORY,
    std::{
        cmp::min,
        net::SocketAddr,
        str::FromStr,
        sync::RwLock,
        thread::sleep,
        time::{Duration, Instant},
    },
};

#[derive(Default)]
pub struct RpcClientConfig {
    commitment_config: CommitmentConfig,
    confirm_transaction_initial_timeout: Option<Duration>,
}

impl RpcClientConfig {
    fn with_commitment(commitment_config: CommitmentConfig) -> Self {
        RpcClientConfig {
            commitment_config,
            ..Self::default()
        }
    }
}

pub struct RpcClient {
    sender: Box<dyn RpcSender + Send + Sync + 'static>,
    config: RpcClientConfig,
    node_version: RwLock<Option<semver::Version>>,
}

impl RpcClient {
    fn new_sender<T: RpcSender + Send + Sync + 'static>(
        sender: T,
        config: RpcClientConfig,
    ) -> Self {
        Self {
            sender: Box::new(sender),
            node_version: RwLock::new(None),
            config,
        }
    }

    pub fn new(url: String) -> Self {
        Self::new_with_commitment(url, CommitmentConfig::default())
    }

    pub fn new_with_commitment(url: String, commitment_config: CommitmentConfig) -> Self {
        Self::new_sender(
            HttpSender::new(url),
            RpcClientConfig::with_commitment(commitment_config),
        )
    }

    pub fn new_with_timeout(url: String, timeout: Duration) -> Self {
        Self::new_sender(
            HttpSender::new_with_timeout(url, timeout),
            RpcClientConfig::with_commitment(CommitmentConfig::default()),
        )
    }

    pub fn new_with_timeout_and_commitment(
        url: String,
        timeout: Duration,
        commitment_config: CommitmentConfig,
    ) -> Self {
        Self::new_sender(
            HttpSender::new_with_timeout(url, timeout),
            RpcClientConfig::with_commitment(commitment_config),
        )
    }

    pub fn new_with_timeouts_and_commitment(
        url: String,
        timeout: Duration,
        commitment_config: CommitmentConfig,
        confirm_transaction_initial_timeout: Duration,
    ) -> Self {
        Self::new_sender(
            HttpSender::new_with_timeout(url, timeout),
            RpcClientConfig {
                commitment_config,
                confirm_transaction_initial_timeout: Some(confirm_transaction_initial_timeout),
            },
        )
    }

    pub fn new_mock(url: String) -> Self {
        Self::new_sender(
            MockSender::new(url),
            RpcClientConfig::with_commitment(CommitmentConfig::default()),
        )
    }

    pub fn new_mock_with_mocks(url: String, mocks: Mocks) -> Self {
        Self::new_sender(
            MockSender::new_with_mocks(url, mocks),
            RpcClientConfig::with_commitment(CommitmentConfig::default()),
        )
    }

    pub fn new_socket(addr: SocketAddr) -> Self {
        Self::new(get_rpc_request_str(addr, false))
    }

    pub fn new_socket_with_commitment(
        addr: SocketAddr,
        commitment_config: CommitmentConfig,
    ) -> Self {
        Self::new_with_commitment(get_rpc_request_str(addr, false), commitment_config)
    }

    pub fn new_socket_with_timeout(addr: SocketAddr, timeout: Duration) -> Self {
        let url = get_rpc_request_str(addr, false);
        Self::new_with_timeout(url, timeout)
    }

    fn get_node_version(&self) -> Result<semver::Version, RpcError> {
        let r_node_version = self.node_version.read().unwrap();
        if let Some(version) = &*r_node_version {
            Ok(version.clone())
        } else {
            drop(r_node_version);
            let mut w_node_version = self.node_version.write().unwrap();
            let node_version = self.get_version().map_err(|e| {
                RpcError::RpcRequestError(format!("cluster version query failed: {}", e))
            })?;
            let node_version = semver::Version::parse(&node_version.solana_core).map_err(|e| {
                RpcError::RpcRequestError(format!("failed to parse cluster version: {}", e))
            })?;
            *w_node_version = Some(node_version.clone());
            Ok(node_version)
        }
    }

    pub fn commitment(&self) -> CommitmentConfig {
        self.config.commitment_config
    }

    fn use_deprecated_commitment(&self) -> Result<bool, RpcError> {
        Ok(self.get_node_version()? < semver::Version::new(1, 5, 5))
    }

    fn maybe_map_commitment(
        &self,
        requested_commitment: CommitmentConfig,
    ) -> Result<CommitmentConfig, RpcError> {
        if matches!(
            requested_commitment.commitment,
            CommitmentLevel::Finalized | CommitmentLevel::Confirmed | CommitmentLevel::Processed
        ) && self.use_deprecated_commitment()?
        {
            return Ok(CommitmentConfig::use_deprecated_commitment(
                requested_commitment,
            ));
        }
        Ok(requested_commitment)
    }

    #[allow(deprecated)]
    fn maybe_map_request(&self, mut request: RpcRequest) -> Result<RpcRequest, RpcError> {
        if self.get_node_version()? < semver::Version::new(1, 7, 0) {
            request = match request {
                RpcRequest::GetBlock => RpcRequest::GetConfirmedBlock,
                RpcRequest::GetBlocks => RpcRequest::GetConfirmedBlocks,
                RpcRequest::GetBlocksWithLimit => RpcRequest::GetConfirmedBlocksWithLimit,
                RpcRequest::GetSignaturesForAddress => {
                    RpcRequest::GetConfirmedSignaturesForAddress2
                }
                RpcRequest::GetTransaction => RpcRequest::GetConfirmedTransaction,
                _ => request,
            };
        }
        Ok(request)
    }

    pub fn confirm_transaction(&self, signature: &Signature) -> ClientResult<bool> {
        Ok(self
            .confirm_transaction_with_commitment(signature, self.commitment())?
            .value)
    }

    pub fn confirm_transaction_with_commitment(
        &self,
        signature: &Signature,
        commitment_config: CommitmentConfig,
    ) -> RpcResult<bool> {
        let Response { context, value } = self.get_signature_statuses(&[*signature])?;

        Ok(Response {
            context,
            value: value[0]
                .as_ref()
                .filter(|result| result.satisfies_commitment(commitment_config))
                .map(|result| result.status.is_ok())
                .unwrap_or_default(),
        })
    }

    pub fn send_transaction(&self, transaction: &Transaction) -> ClientResult<Signature> {
        self.send_transaction_with_config(
            transaction,
            RpcSendTransactionConfig {
                preflight_commitment: Some(
                    self.maybe_map_commitment(self.commitment())?.commitment,
                ),
                ..RpcSendTransactionConfig::default()
            },
        )
    }

    fn default_cluster_transaction_encoding(&self) -> Result<UiTransactionEncoding, RpcError> {
        if self.get_node_version()? < semver::Version::new(1, 3, 16) {
            Ok(UiTransactionEncoding::Base58)
        } else {
            Ok(UiTransactionEncoding::Base64)
        }
    }

    pub fn send_transaction_with_config(
        &self,
        transaction: &Transaction,
        config: RpcSendTransactionConfig,
    ) -> ClientResult<Signature> {
        let encoding = if let Some(encoding) = config.encoding {
            encoding
        } else {
            self.default_cluster_transaction_encoding()?
        };
        let preflight_commitment = CommitmentConfig {
            commitment: config.preflight_commitment.unwrap_or_default(),
        };
        let preflight_commitment = self.maybe_map_commitment(preflight_commitment)?;
        let config = RpcSendTransactionConfig {
            encoding: Some(encoding),
            preflight_commitment: Some(preflight_commitment.commitment),
            ..config
        };
        let serialized_encoded = serialize_encode_transaction(transaction, encoding)?;
        let signature_base58_str: String = match self.send(
            RpcRequest::SendTransaction,
            json!([serialized_encoded, config]),
        ) {
            Ok(signature_base58_str) => signature_base58_str,
            Err(err) => {
                if let ClientErrorKind::RpcError(RpcError::RpcResponseError {
                    code,
                    message,
                    data,
                }) = &err.kind
                {
                    debug!("{} {}", code, message);
                    if let RpcResponseErrorData::SendTransactionPreflightFailure(
                        RpcSimulateTransactionResult {
                            logs: Some(logs), ..
                        },
                    ) = data
                    {
                        for (i, log) in logs.iter().enumerate() {
                            debug!("{:>3}: {}", i + 1, log);
                        }
                        debug!("");
                    }
                }
                return Err(err);
            }
        };

        let signature = signature_base58_str
            .parse::<Signature>()
            .map_err(|err| Into::<ClientError>::into(RpcError::ParseError(err.to_string())))?;
        // A mismatching RPC response signature indicates an issue with the RPC node, and
        // should not be passed along to confirmation methods. The transaction may or may
        // not have been submitted to the cluster, so callers should verify the success of
        // the correct transaction signature independently.
        if signature != transaction.signatures[0] {
            Err(RpcError::RpcRequestError(format!(
                "RPC node returned mismatched signature {:?}, expected {:?}",
                signature, transaction.signatures[0]
            ))
            .into())
        } else {
            Ok(transaction.signatures[0])
        }
    }

    pub fn simulate_transaction(
        &self,
        transaction: &Transaction,
    ) -> RpcResult<RpcSimulateTransactionResult> {
        self.simulate_transaction_with_config(
            transaction,
            RpcSimulateTransactionConfig {
                commitment: Some(self.commitment()),
                ..RpcSimulateTransactionConfig::default()
            },
        )
    }

    pub fn simulate_transaction_with_config(
        &self,
        transaction: &Transaction,
        config: RpcSimulateTransactionConfig,
    ) -> RpcResult<RpcSimulateTransactionResult> {
        let encoding = if let Some(encoding) = config.encoding {
            encoding
        } else {
            self.default_cluster_transaction_encoding()?
        };
        let commitment = config.commitment.unwrap_or_default();
        let commitment = self.maybe_map_commitment(commitment)?;
        let config = RpcSimulateTransactionConfig {
            encoding: Some(encoding),
            commitment: Some(commitment),
            ..config
        };
        let serialized_encoded = serialize_encode_transaction(transaction, encoding)?;
        self.send(
            RpcRequest::SimulateTransaction,
            json!([serialized_encoded, config]),
        )
    }

    pub fn get_snapshot_slot(&self) -> ClientResult<Slot> {
        self.send(RpcRequest::GetSnapshotSlot, Value::Null)
    }

    pub fn get_signature_status(
        &self,
        signature: &Signature,
    ) -> ClientResult<Option<transaction::Result<()>>> {
        self.get_signature_status_with_commitment(signature, self.commitment())
    }

    pub fn get_signature_statuses(
        &self,
        signatures: &[Signature],
    ) -> RpcResult<Vec<Option<TransactionStatus>>> {
        let signatures: Vec<_> = signatures.iter().map(|s| s.to_string()).collect();
        self.send(RpcRequest::GetSignatureStatuses, json!([signatures]))
    }

    pub fn get_signature_statuses_with_history(
        &self,
        signatures: &[Signature],
    ) -> RpcResult<Vec<Option<TransactionStatus>>> {
        let signatures: Vec<_> = signatures.iter().map(|s| s.to_string()).collect();
        self.send(
            RpcRequest::GetSignatureStatuses,
            json!([signatures, {
                "searchTransactionHistory": true
            }]),
        )
    }

    pub fn get_signature_status_with_commitment(
        &self,
        signature: &Signature,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<Option<transaction::Result<()>>> {
        let result: Response<Vec<Option<TransactionStatus>>> = self.send(
            RpcRequest::GetSignatureStatuses,
            json!([[signature.to_string()]]),
        )?;
        Ok(result.value[0]
            .clone()
            .filter(|result| result.satisfies_commitment(commitment_config))
            .map(|status_meta| status_meta.status))
    }

    pub fn get_signature_status_with_commitment_and_history(
        &self,
        signature: &Signature,
        commitment_config: CommitmentConfig,
        search_transaction_history: bool,
    ) -> ClientResult<Option<transaction::Result<()>>> {
        let result: Response<Vec<Option<TransactionStatus>>> = self.send(
            RpcRequest::GetSignatureStatuses,
            json!([[signature.to_string()], {
                "searchTransactionHistory": search_transaction_history
            }]),
        )?;
        Ok(result.value[0]
            .clone()
            .filter(|result| result.satisfies_commitment(commitment_config))
            .map(|status_meta| status_meta.status))
    }

    pub fn get_slot(&self) -> ClientResult<Slot> {
        self.get_slot_with_commitment(self.commitment())
    }

    pub fn get_slot_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<Slot> {
        self.send(
            RpcRequest::GetSlot,
            json!([self.maybe_map_commitment(commitment_config)?]),
        )
    }

    pub fn get_block_height(&self) -> ClientResult<u64> {
        self.get_block_height_with_commitment(self.commitment())
    }

    pub fn get_block_height_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<u64> {
        self.send(
            RpcRequest::GetBlockHeight,
            json!([self.maybe_map_commitment(commitment_config)?]),
        )
    }

    pub fn get_slot_leaders(&self, start_slot: Slot, limit: u64) -> ClientResult<Vec<Pubkey>> {
        self.send(RpcRequest::GetSlotLeaders, json!([start_slot, limit]))
            .and_then(|slot_leaders: Vec<String>| {
                slot_leaders
                    .iter()
                    .map(|slot_leader| {
                        Pubkey::from_str(slot_leader).map_err(|err| {
                            ClientErrorKind::Custom(format!(
                                "pubkey deserialization failed: {}",
                                err
                            ))
                            .into()
                        })
                    })
                    .collect()
            })
    }

    /// Get block production for the current epoch
    pub fn get_block_production(&self) -> RpcResult<RpcBlockProduction> {
        self.send(RpcRequest::GetBlockProduction, Value::Null)
    }

    pub fn get_block_production_with_config(
        &self,
        config: RpcBlockProductionConfig,
    ) -> RpcResult<RpcBlockProduction> {
        self.send(RpcRequest::GetBlockProduction, json!(config))
    }

    pub fn get_stake_activation(
        &self,
        stake_account: Pubkey,
        epoch: Option<Epoch>,
    ) -> ClientResult<RpcStakeActivation> {
        self.send(
            RpcRequest::GetStakeActivation,
            json!([
                stake_account.to_string(),
                RpcEpochConfig {
                    epoch,
                    commitment: Some(self.commitment()),
                }
            ]),
        )
    }

    pub fn supply(&self) -> RpcResult<RpcSupply> {
        self.supply_with_commitment(self.commitment())
    }

    pub fn supply_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> RpcResult<RpcSupply> {
        self.send(
            RpcRequest::GetSupply,
            json!([self.maybe_map_commitment(commitment_config)?]),
        )
    }

    pub fn get_largest_accounts_with_config(
        &self,
        config: RpcLargestAccountsConfig,
    ) -> RpcResult<Vec<RpcAccountBalance>> {
        let commitment = config.commitment.unwrap_or_default();
        let commitment = self.maybe_map_commitment(commitment)?;
        let config = RpcLargestAccountsConfig {
            commitment: Some(commitment),
            ..config
        };
        self.send(RpcRequest::GetLargestAccounts, json!([config]))
    }

    pub fn get_vote_accounts(&self) -> ClientResult<RpcVoteAccountStatus> {
        self.get_vote_accounts_with_commitment(self.commitment())
    }

    pub fn get_vote_accounts_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<RpcVoteAccountStatus> {
        self.get_vote_accounts_with_config(RpcGetVoteAccountsConfig {
            commitment: Some(self.maybe_map_commitment(commitment_config)?),
            ..RpcGetVoteAccountsConfig::default()
        })
    }

    pub fn get_vote_accounts_with_config(
        &self,
        config: RpcGetVoteAccountsConfig,
    ) -> ClientResult<RpcVoteAccountStatus> {
        self.send(RpcRequest::GetVoteAccounts, json!([config]))
    }

    pub fn wait_for_max_stake(
        &self,
        commitment: CommitmentConfig,
        max_stake_percent: f32,
    ) -> ClientResult<()> {
        let mut current_percent;
        loop {
            let vote_accounts = self.get_vote_accounts_with_commitment(commitment)?;

            let mut max = 0;
            let total_active_stake = vote_accounts
                .current
                .iter()
                .chain(vote_accounts.delinquent.iter())
                .map(|vote_account| {
                    max = std::cmp::max(max, vote_account.activated_stake);
                    vote_account.activated_stake
                })
                .sum::<u64>();
            current_percent = 100f32 * max as f32 / total_active_stake as f32;
            if current_percent < max_stake_percent {
                break;
            }
            info!(
                "Waiting for stake to drop below {} current: {:.1}",
                max_stake_percent, current_percent
            );
            sleep(Duration::from_secs(10));
        }
        Ok(())
    }

    pub fn get_cluster_nodes(&self) -> ClientResult<Vec<RpcContactInfo>> {
        self.send(RpcRequest::GetClusterNodes, Value::Null)
    }

    pub fn get_block(&self, slot: Slot) -> ClientResult<EncodedConfirmedBlock> {
        self.get_block_with_encoding(slot, UiTransactionEncoding::Json)
    }

    pub fn get_block_with_encoding(
        &self,
        slot: Slot,
        encoding: UiTransactionEncoding,
    ) -> ClientResult<EncodedConfirmedBlock> {
        self.send(
            self.maybe_map_request(RpcRequest::GetBlock)?,
            json!([slot, encoding]),
        )
    }

    pub fn get_block_with_config(
        &self,
        slot: Slot,
        config: RpcBlockConfig,
    ) -> ClientResult<UiConfirmedBlock> {
        self.send(
            self.maybe_map_request(RpcRequest::GetBlock)?,
            json!([slot, config]),
        )
    }

    #[deprecated(since = "1.7.0", note = "Please use RpcClient::get_block() instead")]
    #[allow(deprecated)]
    pub fn get_confirmed_block(&self, slot: Slot) -> ClientResult<EncodedConfirmedBlock> {
        self.get_confirmed_block_with_encoding(slot, UiTransactionEncoding::Json)
    }

    #[deprecated(
        since = "1.7.0",
        note = "Please use RpcClient::get_block_with_encoding() instead"
    )]
    #[allow(deprecated)]
    pub fn get_confirmed_block_with_encoding(
        &self,
        slot: Slot,
        encoding: UiTransactionEncoding,
    ) -> ClientResult<EncodedConfirmedBlock> {
        self.send(RpcRequest::GetConfirmedBlock, json!([slot, encoding]))
    }

    #[deprecated(
        since = "1.7.0",
        note = "Please use RpcClient::get_block_with_config() instead"
    )]
    #[allow(deprecated)]
    pub fn get_confirmed_block_with_config(
        &self,
        slot: Slot,
        config: RpcConfirmedBlockConfig,
    ) -> ClientResult<UiConfirmedBlock> {
        self.send(RpcRequest::GetConfirmedBlock, json!([slot, config]))
    }

    pub fn get_blocks(&self, start_slot: Slot, end_slot: Option<Slot>) -> ClientResult<Vec<Slot>> {
        self.send(
            self.maybe_map_request(RpcRequest::GetBlocks)?,
            json!([start_slot, end_slot]),
        )
    }

    pub fn get_blocks_with_commitment(
        &self,
        start_slot: Slot,
        end_slot: Option<Slot>,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<Vec<Slot>> {
        let json = if end_slot.is_some() {
            json!([
                start_slot,
                end_slot,
                self.maybe_map_commitment(commitment_config)?
            ])
        } else {
            json!([start_slot, self.maybe_map_commitment(commitment_config)?])
        };
        self.send(self.maybe_map_request(RpcRequest::GetBlocks)?, json)
    }

    pub fn get_blocks_with_limit(&self, start_slot: Slot, limit: usize) -> ClientResult<Vec<Slot>> {
        self.send(
            self.maybe_map_request(RpcRequest::GetBlocksWithLimit)?,
            json!([start_slot, limit]),
        )
    }

    pub fn get_blocks_with_limit_and_commitment(
        &self,
        start_slot: Slot,
        limit: usize,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<Vec<Slot>> {
        self.send(
            self.maybe_map_request(RpcRequest::GetBlocksWithLimit)?,
            json!([
                start_slot,
                limit,
                self.maybe_map_commitment(commitment_config)?
            ]),
        )
    }

    #[deprecated(since = "1.7.0", note = "Please use RpcClient::get_blocks() instead")]
    #[allow(deprecated)]
    pub fn get_confirmed_blocks(
        &self,
        start_slot: Slot,
        end_slot: Option<Slot>,
    ) -> ClientResult<Vec<Slot>> {
        self.send(
            RpcRequest::GetConfirmedBlocks,
            json!([start_slot, end_slot]),
        )
    }

    #[deprecated(
        since = "1.7.0",
        note = "Please use RpcClient::get_blocks_with_commitment() instead"
    )]
    #[allow(deprecated)]
    pub fn get_confirmed_blocks_with_commitment(
        &self,
        start_slot: Slot,
        end_slot: Option<Slot>,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<Vec<Slot>> {
        let json = if end_slot.is_some() {
            json!([
                start_slot,
                end_slot,
                self.maybe_map_commitment(commitment_config)?
            ])
        } else {
            json!([start_slot, self.maybe_map_commitment(commitment_config)?])
        };
        self.send(RpcRequest::GetConfirmedBlocks, json)
    }

    #[deprecated(
        since = "1.7.0",
        note = "Please use RpcClient::get_blocks_with_limit() instead"
    )]
    #[allow(deprecated)]
    pub fn get_confirmed_blocks_with_limit(
        &self,
        start_slot: Slot,
        limit: usize,
    ) -> ClientResult<Vec<Slot>> {
        self.send(
            RpcRequest::GetConfirmedBlocksWithLimit,
            json!([start_slot, limit]),
        )
    }

    #[deprecated(
        since = "1.7.0",
        note = "Please use RpcClient::get_blocks_with_limit_and_commitment() instead"
    )]
    #[allow(deprecated)]
    pub fn get_confirmed_blocks_with_limit_and_commitment(
        &self,
        start_slot: Slot,
        limit: usize,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<Vec<Slot>> {
        self.send(
            RpcRequest::GetConfirmedBlocksWithLimit,
            json!([
                start_slot,
                limit,
                self.maybe_map_commitment(commitment_config)?
            ]),
        )
    }

    pub fn get_signatures_for_address(
        &self,
        address: &Pubkey,
    ) -> ClientResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        self.get_signatures_for_address_with_config(
            address,
            GetConfirmedSignaturesForAddress2Config::default(),
        )
    }

    pub fn get_signatures_for_address_with_config(
        &self,
        address: &Pubkey,
        config: GetConfirmedSignaturesForAddress2Config,
    ) -> ClientResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        let config = RpcSignaturesForAddressConfig {
            before: config.before.map(|signature| signature.to_string()),
            until: config.until.map(|signature| signature.to_string()),
            limit: config.limit,
            commitment: config.commitment,
        };

        let result: Vec<RpcConfirmedTransactionStatusWithSignature> = self.send(
            self.maybe_map_request(RpcRequest::GetSignaturesForAddress)?,
            json!([address.to_string(), config]),
        )?;

        Ok(result)
    }

    #[deprecated(
        since = "1.7.0",
        note = "Please use RpcClient::get_signatures_for_address() instead"
    )]
    #[allow(deprecated)]
    pub fn get_confirmed_signatures_for_address2(
        &self,
        address: &Pubkey,
    ) -> ClientResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        self.get_confirmed_signatures_for_address2_with_config(
            address,
            GetConfirmedSignaturesForAddress2Config::default(),
        )
    }

    #[deprecated(
        since = "1.7.0",
        note = "Please use RpcClient::get_signatures_for_address_with_config() instead"
    )]
    #[allow(deprecated)]
    pub fn get_confirmed_signatures_for_address2_with_config(
        &self,
        address: &Pubkey,
        config: GetConfirmedSignaturesForAddress2Config,
    ) -> ClientResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        let config = RpcGetConfirmedSignaturesForAddress2Config {
            before: config.before.map(|signature| signature.to_string()),
            until: config.until.map(|signature| signature.to_string()),
            limit: config.limit,
            commitment: config.commitment,
        };

        let result: Vec<RpcConfirmedTransactionStatusWithSignature> = self.send(
            RpcRequest::GetConfirmedSignaturesForAddress2,
            json!([address.to_string(), config]),
        )?;

        Ok(result)
    }

    pub fn get_transaction(
        &self,
        signature: &Signature,
        encoding: UiTransactionEncoding,
    ) -> ClientResult<EncodedConfirmedTransaction> {
        self.send(
            self.maybe_map_request(RpcRequest::GetTransaction)?,
            json!([signature.to_string(), encoding]),
        )
    }

    pub fn get_transaction_with_config(
        &self,
        signature: &Signature,
        config: RpcTransactionConfig,
    ) -> ClientResult<EncodedConfirmedTransaction> {
        self.send(
            self.maybe_map_request(RpcRequest::GetTransaction)?,
            json!([signature.to_string(), config]),
        )
    }

    #[deprecated(
        since = "1.7.0",
        note = "Please use RpcClient::get_transaction() instead"
    )]
    #[allow(deprecated)]
    pub fn get_confirmed_transaction(
        &self,
        signature: &Signature,
        encoding: UiTransactionEncoding,
    ) -> ClientResult<EncodedConfirmedTransaction> {
        self.send(
            RpcRequest::GetConfirmedTransaction,
            json!([signature.to_string(), encoding]),
        )
    }

    #[deprecated(
        since = "1.7.0",
        note = "Please use RpcClient::get_transaction_with_config() instead"
    )]
    #[allow(deprecated)]
    pub fn get_confirmed_transaction_with_config(
        &self,
        signature: &Signature,
        config: RpcConfirmedTransactionConfig,
    ) -> ClientResult<EncodedConfirmedTransaction> {
        self.send(
            RpcRequest::GetConfirmedTransaction,
            json!([signature.to_string(), config]),
        )
    }

    pub fn get_block_time(&self, slot: Slot) -> ClientResult<UnixTimestamp> {
        let request = RpcRequest::GetBlockTime;
        let response = self.sender.send(request, json!([slot]));

        response
            .map(|result_json| {
                if result_json.is_null() {
                    return Err(RpcError::ForUser(format!("Block Not Found: slot={}", slot)).into());
                }
                let result = serde_json::from_value(result_json)
                    .map_err(|err| ClientError::new_with_request(err.into(), request))?;
                trace!("Response block timestamp {:?} {:?}", slot, result);
                Ok(result)
            })
            .map_err(|err| err.into_with_request(request))?
    }

    pub fn get_epoch_info(&self) -> ClientResult<EpochInfo> {
        self.get_epoch_info_with_commitment(self.commitment())
    }

    pub fn get_epoch_info_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<EpochInfo> {
        self.send(
            RpcRequest::GetEpochInfo,
            json!([self.maybe_map_commitment(commitment_config)?]),
        )
    }

    pub fn get_leader_schedule(
        &self,
        slot: Option<Slot>,
    ) -> ClientResult<Option<RpcLeaderSchedule>> {
        self.get_leader_schedule_with_commitment(slot, self.commitment())
    }

    pub fn get_leader_schedule_with_commitment(
        &self,
        slot: Option<Slot>,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<Option<RpcLeaderSchedule>> {
        self.get_leader_schedule_with_config(
            slot,
            RpcLeaderScheduleConfig {
                commitment: Some(self.maybe_map_commitment(commitment_config)?),
                ..RpcLeaderScheduleConfig::default()
            },
        )
    }

    pub fn get_leader_schedule_with_config(
        &self,
        slot: Option<Slot>,
        config: RpcLeaderScheduleConfig,
    ) -> ClientResult<Option<RpcLeaderSchedule>> {
        self.send(RpcRequest::GetLeaderSchedule, json!([slot, config]))
    }

    pub fn get_epoch_schedule(&self) -> ClientResult<EpochSchedule> {
        self.send(RpcRequest::GetEpochSchedule, Value::Null)
    }

    pub fn get_recent_performance_samples(
        &self,
        limit: Option<usize>,
    ) -> ClientResult<Vec<RpcPerfSample>> {
        self.send(RpcRequest::GetRecentPerformanceSamples, json!([limit]))
    }

    pub fn get_identity(&self) -> ClientResult<Pubkey> {
        let rpc_identity: RpcIdentity = self.send(RpcRequest::GetIdentity, Value::Null)?;

        rpc_identity.identity.parse::<Pubkey>().map_err(|_| {
            ClientError::new_with_request(
                RpcError::ParseError("Pubkey".to_string()).into(),
                RpcRequest::GetIdentity,
            )
        })
    }

    pub fn get_inflation_governor(&self) -> ClientResult<RpcInflationGovernor> {
        self.send(RpcRequest::GetInflationGovernor, Value::Null)
    }

    pub fn get_inflation_rate(&self) -> ClientResult<RpcInflationRate> {
        self.send(RpcRequest::GetInflationRate, Value::Null)
    }

    pub fn get_inflation_reward(
        &self,
        addresses: &[Pubkey],
        epoch: Option<Epoch>,
    ) -> ClientResult<Vec<Option<RpcInflationReward>>> {
        let addresses: Vec<_> = addresses
            .iter()
            .map(|address| address.to_string())
            .collect();
        self.send(
            RpcRequest::GetInflationReward,
            json!([
                addresses,
                RpcEpochConfig {
                    epoch,
                    commitment: Some(self.commitment()),
                }
            ]),
        )
    }

    pub fn get_version(&self) -> ClientResult<RpcVersionInfo> {
        self.send(RpcRequest::GetVersion, Value::Null)
    }

    pub fn minimum_ledger_slot(&self) -> ClientResult<Slot> {
        self.send(RpcRequest::MinimumLedgerSlot, Value::Null)
    }

    pub fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
    ) -> ClientResult<Signature> {
        const SEND_RETRIES: usize = 1;
        const GET_STATUS_RETRIES: usize = usize::MAX;

        'sending: for _ in 0..SEND_RETRIES {
            let signature = self.send_transaction(transaction)?;

            let recent_blockhash = if uses_durable_nonce(transaction).is_some() {
                let (recent_blockhash, ..) = self
                    .get_recent_blockhash_with_commitment(CommitmentConfig::processed())?
                    .value;
                recent_blockhash
            } else {
                transaction.message.recent_blockhash
            };

            for status_retry in 0..GET_STATUS_RETRIES {
                match self.get_signature_status(&signature)? {
                    Some(Ok(_)) => return Ok(signature),
                    Some(Err(e)) => return Err(e.into()),
                    None => {
                        let fee_calculator = self
                            .get_fee_calculator_for_blockhash_with_commitment(
                                &recent_blockhash,
                                CommitmentConfig::processed(),
                            )?
                            .value;
                        if fee_calculator.is_none() {
                            // Block hash is not found by some reason
                            break 'sending;
                        } else if cfg!(not(test))
                            // Ignore sleep at last step.
                            && status_retry < GET_STATUS_RETRIES
                        {
                            // Retry twice a second
                            sleep(Duration::from_millis(500));
                            continue;
                        }
                    }
                }
            }
        }

        Err(RpcError::ForUser(
            "unable to confirm transaction. \
             This can happen in situations such as transaction expiration \
             and insufficient fee-payer funds"
                .to_string(),
        )
        .into())
    }

    /// Note that `get_account` returns `Err(..)` if the account does not exist whereas
    /// `get_account_with_commitment` returns `Ok(None)` if the account does not exist.
    pub fn get_account(&self, pubkey: &Pubkey) -> ClientResult<Account> {
        self.get_account_with_commitment(pubkey, self.commitment())?
            .value
            .ok_or_else(|| RpcError::ForUser(format!("AccountNotFound: pubkey={}", pubkey)).into())
    }

    pub fn get_account_with_commitment(
        &self,
        pubkey: &Pubkey,
        commitment_config: CommitmentConfig,
    ) -> RpcResult<Option<Account>> {
        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64Zstd),
            commitment: Some(self.maybe_map_commitment(commitment_config)?),
            data_slice: None,
        };
        let response = self.sender.send(
            RpcRequest::GetAccountInfo,
            json!([pubkey.to_string(), config]),
        );

        response
            .map(|result_json| {
                if result_json.is_null() {
                    return Err(
                        RpcError::ForUser(format!("AccountNotFound: pubkey={}", pubkey)).into(),
                    );
                }
                let Response {
                    context,
                    value: rpc_account,
                } = serde_json::from_value::<Response<Option<UiAccount>>>(result_json)?;
                trace!("Response account {:?} {:?}", pubkey, rpc_account);
                let account = rpc_account.and_then(|rpc_account| rpc_account.decode());
                Ok(Response {
                    context,
                    value: account,
                })
            })
            .map_err(|err| {
                Into::<ClientError>::into(RpcError::ForUser(format!(
                    "AccountNotFound: pubkey={}: {}",
                    pubkey, err
                )))
            })?
    }

    pub fn get_max_retransmit_slot(&self) -> ClientResult<Slot> {
        self.send(RpcRequest::GetMaxRetransmitSlot, Value::Null)
    }

    pub fn get_max_shred_insert_slot(&self) -> ClientResult<Slot> {
        self.send(RpcRequest::GetMaxShredInsertSlot, Value::Null)
    }

    pub fn get_multiple_accounts(&self, pubkeys: &[Pubkey]) -> ClientResult<Vec<Option<Account>>> {
        Ok(self
            .get_multiple_accounts_with_commitment(pubkeys, self.commitment())?
            .value)
    }

    pub fn get_multiple_accounts_with_commitment(
        &self,
        pubkeys: &[Pubkey],
        commitment_config: CommitmentConfig,
    ) -> RpcResult<Vec<Option<Account>>> {
        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64Zstd),
            commitment: Some(self.maybe_map_commitment(commitment_config)?),
            data_slice: None,
        };
        let pubkeys: Vec<_> = pubkeys.iter().map(|pubkey| pubkey.to_string()).collect();
        let response = self.send(RpcRequest::GetMultipleAccounts, json!([pubkeys, config]))?;
        let Response {
            context,
            value: accounts,
        } = serde_json::from_value::<Response<Vec<Option<UiAccount>>>>(response)?;
        let accounts: Vec<Option<Account>> = accounts
            .into_iter()
            .map(|rpc_account| rpc_account.map(|a| a.decode()).flatten())
            .collect();
        Ok(Response {
            context,
            value: accounts,
        })
    }

    pub fn get_account_data(&self, pubkey: &Pubkey) -> ClientResult<Vec<u8>> {
        Ok(self.get_account(pubkey)?.data)
    }

    pub fn get_minimum_balance_for_rent_exemption(&self, data_len: usize) -> ClientResult<u64> {
        let request = RpcRequest::GetMinimumBalanceForRentExemption;
        let minimum_balance_json = self
            .sender
            .send(request, json!([data_len]))
            .map_err(|err| err.into_with_request(request))?;

        let minimum_balance: u64 = serde_json::from_value(minimum_balance_json)
            .map_err(|err| ClientError::new_with_request(err.into(), request))?;
        trace!(
            "Response minimum balance {:?} {:?}",
            data_len,
            minimum_balance
        );
        Ok(minimum_balance)
    }

    /// Request the balance of the account `pubkey`.
    pub fn get_balance(&self, pubkey: &Pubkey) -> ClientResult<u64> {
        Ok(self
            .get_balance_with_commitment(pubkey, self.commitment())?
            .value)
    }

    pub fn get_balance_with_commitment(
        &self,
        pubkey: &Pubkey,
        commitment_config: CommitmentConfig,
    ) -> RpcResult<u64> {
        self.send(
            RpcRequest::GetBalance,
            json!([
                pubkey.to_string(),
                self.maybe_map_commitment(commitment_config)?
            ]),
        )
    }

    pub fn get_program_accounts(&self, pubkey: &Pubkey) -> ClientResult<Vec<(Pubkey, Account)>> {
        self.get_program_accounts_with_config(
            pubkey,
            RpcProgramAccountsConfig {
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64Zstd),
                    ..RpcAccountInfoConfig::default()
                },
                ..RpcProgramAccountsConfig::default()
            },
        )
    }

    pub fn get_program_accounts_with_config(
        &self,
        pubkey: &Pubkey,
        config: RpcProgramAccountsConfig,
    ) -> ClientResult<Vec<(Pubkey, Account)>> {
        let commitment = config
            .account_config
            .commitment
            .unwrap_or_else(|| self.commitment());
        let commitment = self.maybe_map_commitment(commitment)?;
        let account_config = RpcAccountInfoConfig {
            commitment: Some(commitment),
            ..config.account_config
        };
        let config = RpcProgramAccountsConfig {
            account_config,
            ..config
        };
        let accounts: Vec<RpcKeyedAccount> = self.send(
            RpcRequest::GetProgramAccounts,
            json!([pubkey.to_string(), config]),
        )?;
        parse_keyed_accounts(accounts, RpcRequest::GetProgramAccounts)
    }

    /// Request the transaction count.
    pub fn get_transaction_count(&self) -> ClientResult<u64> {
        self.get_transaction_count_with_commitment(self.commitment())
    }

    pub fn get_transaction_count_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<u64> {
        self.send(
            RpcRequest::GetTransactionCount,
            json!([self.maybe_map_commitment(commitment_config)?]),
        )
    }

    pub fn get_recent_blockhash(&self) -> ClientResult<(Hash, FeeCalculator)> {
        let (blockhash, fee_calculator, _last_valid_slot) = self
            .get_recent_blockhash_with_commitment(self.commitment())?
            .value;
        Ok((blockhash, fee_calculator))
    }

    pub fn get_recent_blockhash_with_commitment(
        &self,
        commitment_config: CommitmentConfig,
    ) -> RpcResult<(Hash, FeeCalculator, Slot)> {
        let (context, blockhash, fee_calculator, last_valid_slot) = if let Ok(Response {
            context,
            value:
                RpcFees {
                    blockhash,
                    fee_calculator,
                    last_valid_slot,
                    ..
                },
        }) = self
            .send::<Response<RpcFees>>(
                RpcRequest::GetFees,
                json!([self.maybe_map_commitment(commitment_config)?]),
            ) {
            (context, blockhash, fee_calculator, last_valid_slot)
        } else if let Ok(Response {
            context,
            value:
                DeprecatedRpcFees {
                    blockhash,
                    fee_calculator,
                    last_valid_slot,
                },
        }) = self.send::<Response<DeprecatedRpcFees>>(
            RpcRequest::GetFees,
            json!([self.maybe_map_commitment(commitment_config)?]),
        ) {
            (context, blockhash, fee_calculator, last_valid_slot)
        } else if let Ok(Response {
            context,
            value:
                RpcBlockhashFeeCalculator {
                    blockhash,
                    fee_calculator,
                },
        }) = self.send::<Response<RpcBlockhashFeeCalculator>>(
            RpcRequest::GetRecentBlockhash,
            json!([self.maybe_map_commitment(commitment_config)?]),
        ) {
            (context, blockhash, fee_calculator, 0)
        } else {
            return Err(ClientError::new_with_request(
                RpcError::ParseError("RpcBlockhashFeeCalculator or RpcFees".to_string()).into(),
                RpcRequest::GetRecentBlockhash,
            ));
        };

        let blockhash = blockhash.parse().map_err(|_| {
            ClientError::new_with_request(
                RpcError::ParseError("Hash".to_string()).into(),
                RpcRequest::GetRecentBlockhash,
            )
        })?;
        Ok(Response {
            context,
            value: (blockhash, fee_calculator, last_valid_slot),
        })
    }

    pub fn get_fee_calculator_for_blockhash(
        &self,
        blockhash: &Hash,
    ) -> ClientResult<Option<FeeCalculator>> {
        Ok(self
            .get_fee_calculator_for_blockhash_with_commitment(blockhash, self.commitment())?
            .value)
    }

    pub fn get_fee_calculator_for_blockhash_with_commitment(
        &self,
        blockhash: &Hash,
        commitment_config: CommitmentConfig,
    ) -> RpcResult<Option<FeeCalculator>> {
        let Response { context, value } = self.send::<Response<Option<RpcFeeCalculator>>>(
            RpcRequest::GetFeeCalculatorForBlockhash,
            json!([
                blockhash.to_string(),
                self.maybe_map_commitment(commitment_config)?
            ]),
        )?;

        Ok(Response {
            context,
            value: value.map(|rf| rf.fee_calculator),
        })
    }

    pub fn get_fee_rate_governor(&self) -> RpcResult<FeeRateGovernor> {
        let Response {
            context,
            value: RpcFeeRateGovernor { fee_rate_governor },
        } =
            self.send::<Response<RpcFeeRateGovernor>>(RpcRequest::GetFeeRateGovernor, Value::Null)?;

        Ok(Response {
            context,
            value: fee_rate_governor,
        })
    }

    pub fn get_new_blockhash(&self, blockhash: &Hash) -> ClientResult<(Hash, FeeCalculator)> {
        let mut num_retries = 0;
        let start = Instant::now();
        while start.elapsed().as_secs() < 5 {
            if let Ok((new_blockhash, fee_calculator)) = self.get_recent_blockhash() {
                if new_blockhash != *blockhash {
                    return Ok((new_blockhash, fee_calculator));
                }
            }
            debug!("Got same blockhash ({:?}), will retry...", blockhash);

            // Retry ~twice during a slot
            sleep(Duration::from_millis(DEFAULT_MS_PER_SLOT / 2));
            num_retries += 1;
        }
        Err(RpcError::ForUser(format!(
            "Unable to get new blockhash after {}ms (retried {} times), stuck at {}",
            start.elapsed().as_millis(),
            num_retries,
            blockhash
        ))
        .into())
    }

    pub fn get_first_available_block(&self) -> ClientResult<Slot> {
        self.send(RpcRequest::GetFirstAvailableBlock, Value::Null)
    }

    pub fn get_genesis_hash(&self) -> ClientResult<Hash> {
        let hash_str: String = self.send(RpcRequest::GetGenesisHash, Value::Null)?;
        let hash = hash_str.parse().map_err(|_| {
            ClientError::new_with_request(
                RpcError::ParseError("Hash".to_string()).into(),
                RpcRequest::GetGenesisHash,
            )
        })?;
        Ok(hash)
    }

    pub fn get_health(&self) -> ClientResult<()> {
        self.send::<String>(RpcRequest::GetHealth, Value::Null)
            .map(|_| ())
    }

    pub fn get_token_account(&self, pubkey: &Pubkey) -> ClientResult<Option<UiTokenAccount>> {
        Ok(self
            .get_token_account_with_commitment(pubkey, self.commitment())?
            .value)
    }

    pub fn get_token_account_with_commitment(
        &self,
        pubkey: &Pubkey,
        commitment_config: CommitmentConfig,
    ) -> RpcResult<Option<UiTokenAccount>> {
        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::JsonParsed),
            commitment: Some(self.maybe_map_commitment(commitment_config)?),
            data_slice: None,
        };
        let response = self.sender.send(
            RpcRequest::GetAccountInfo,
            json!([pubkey.to_string(), config]),
        );

        response
            .map(|result_json| {
                if result_json.is_null() {
                    return Err(
                        RpcError::ForUser(format!("AccountNotFound: pubkey={}", pubkey)).into(),
                    );
                }
                let Response {
                    context,
                    value: rpc_account,
                } = serde_json::from_value::<Response<Option<UiAccount>>>(result_json)?;
                trace!("Response account {:?} {:?}", pubkey, rpc_account);
                let response = {
                    if let Some(rpc_account) = rpc_account {
                        if let UiAccountData::Json(account_data) = rpc_account.data {
                            let token_account_type: TokenAccountType =
                                serde_json::from_value(account_data.parsed)?;
                            if let TokenAccountType::Account(token_account) = token_account_type {
                                return Ok(Response {
                                    context,
                                    value: Some(token_account),
                                });
                            }
                        }
                    }
                    Err(Into::<ClientError>::into(RpcError::ForUser(format!(
                        "Account could not be parsed as token account: pubkey={}",
                        pubkey
                    ))))
                };
                response?
            })
            .map_err(|err| {
                Into::<ClientError>::into(RpcError::ForUser(format!(
                    "AccountNotFound: pubkey={}: {}",
                    pubkey, err
                )))
            })?
    }

    pub fn get_token_account_balance(&self, pubkey: &Pubkey) -> ClientResult<UiTokenAmount> {
        Ok(self
            .get_token_account_balance_with_commitment(pubkey, self.commitment())?
            .value)
    }

    pub fn get_token_account_balance_with_commitment(
        &self,
        pubkey: &Pubkey,
        commitment_config: CommitmentConfig,
    ) -> RpcResult<UiTokenAmount> {
        self.send(
            RpcRequest::GetTokenAccountBalance,
            json!([
                pubkey.to_string(),
                self.maybe_map_commitment(commitment_config)?
            ]),
        )
    }

    pub fn get_token_accounts_by_delegate(
        &self,
        delegate: &Pubkey,
        token_account_filter: TokenAccountsFilter,
    ) -> ClientResult<Vec<RpcKeyedAccount>> {
        Ok(self
            .get_token_accounts_by_delegate_with_commitment(
                delegate,
                token_account_filter,
                self.commitment(),
            )?
            .value)
    }

    pub fn get_token_accounts_by_delegate_with_commitment(
        &self,
        delegate: &Pubkey,
        token_account_filter: TokenAccountsFilter,
        commitment_config: CommitmentConfig,
    ) -> RpcResult<Vec<RpcKeyedAccount>> {
        let token_account_filter = match token_account_filter {
            TokenAccountsFilter::Mint(mint) => RpcTokenAccountsFilter::Mint(mint.to_string()),
            TokenAccountsFilter::ProgramId(program_id) => {
                RpcTokenAccountsFilter::ProgramId(program_id.to_string())
            }
        };

        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::JsonParsed),
            commitment: Some(self.maybe_map_commitment(commitment_config)?),
            data_slice: None,
        };

        self.send(
            RpcRequest::GetTokenAccountsByOwner,
            json!([delegate.to_string(), token_account_filter, config]),
        )
    }

    pub fn get_token_accounts_by_owner(
        &self,
        owner: &Pubkey,
        token_account_filter: TokenAccountsFilter,
    ) -> ClientResult<Vec<RpcKeyedAccount>> {
        Ok(self
            .get_token_accounts_by_owner_with_commitment(
                owner,
                token_account_filter,
                self.commitment(),
            )?
            .value)
    }

    pub fn get_token_accounts_by_owner_with_commitment(
        &self,
        owner: &Pubkey,
        token_account_filter: TokenAccountsFilter,
        commitment_config: CommitmentConfig,
    ) -> RpcResult<Vec<RpcKeyedAccount>> {
        let token_account_filter = match token_account_filter {
            TokenAccountsFilter::Mint(mint) => RpcTokenAccountsFilter::Mint(mint.to_string()),
            TokenAccountsFilter::ProgramId(program_id) => {
                RpcTokenAccountsFilter::ProgramId(program_id.to_string())
            }
        };

        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::JsonParsed),
            commitment: Some(self.maybe_map_commitment(commitment_config)?),
            data_slice: None,
        };

        self.send(
            RpcRequest::GetTokenAccountsByOwner,
            json!([owner.to_string(), token_account_filter, config]),
        )
    }

    pub fn get_token_supply(&self, mint: &Pubkey) -> ClientResult<UiTokenAmount> {
        Ok(self
            .get_token_supply_with_commitment(mint, self.commitment())?
            .value)
    }

    pub fn get_token_supply_with_commitment(
        &self,
        mint: &Pubkey,
        commitment_config: CommitmentConfig,
    ) -> RpcResult<UiTokenAmount> {
        self.send(
            RpcRequest::GetTokenSupply,
            json!([
                mint.to_string(),
                self.maybe_map_commitment(commitment_config)?
            ]),
        )
    }

    pub fn request_airdrop(&self, pubkey: &Pubkey, lamports: u64) -> ClientResult<Signature> {
        self.request_airdrop_with_config(
            pubkey,
            lamports,
            RpcRequestAirdropConfig {
                commitment: Some(self.commitment()),
                ..RpcRequestAirdropConfig::default()
            },
        )
    }

    pub fn request_airdrop_with_blockhash(
        &self,
        pubkey: &Pubkey,
        lamports: u64,
        recent_blockhash: &Hash,
    ) -> ClientResult<Signature> {
        self.request_airdrop_with_config(
            pubkey,
            lamports,
            RpcRequestAirdropConfig {
                commitment: Some(self.commitment()),
                recent_blockhash: Some(recent_blockhash.to_string()),
            },
        )
    }

    pub fn request_airdrop_with_config(
        &self,
        pubkey: &Pubkey,
        lamports: u64,
        config: RpcRequestAirdropConfig,
    ) -> ClientResult<Signature> {
        let commitment = config.commitment.unwrap_or_default();
        let commitment = self.maybe_map_commitment(commitment)?;
        let config = RpcRequestAirdropConfig {
            commitment: Some(commitment),
            ..config
        };
        self.send(
            RpcRequest::RequestAirdrop,
            json!([pubkey.to_string(), lamports, config]),
        )
        .and_then(|signature: String| {
            Signature::from_str(&signature).map_err(|err| {
                ClientErrorKind::Custom(format!("signature deserialization failed: {}", err)).into()
            })
        })
        .map_err(|_| {
            RpcError::ForUser(
                "airdrop request failed. \
                This can happen when the rate limit is reached."
                    .to_string(),
            )
            .into()
        })
    }

    fn poll_balance_with_timeout_and_commitment(
        &self,
        pubkey: &Pubkey,
        polling_frequency: &Duration,
        timeout: &Duration,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<u64> {
        let now = Instant::now();
        loop {
            match self.get_balance_with_commitment(pubkey, commitment_config) {
                Ok(bal) => {
                    return Ok(bal.value);
                }
                Err(e) => {
                    sleep(*polling_frequency);
                    if now.elapsed() > *timeout {
                        return Err(e);
                    }
                }
            };
        }
    }

    pub fn poll_get_balance_with_commitment(
        &self,
        pubkey: &Pubkey,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<u64> {
        self.poll_balance_with_timeout_and_commitment(
            pubkey,
            &Duration::from_millis(100),
            &Duration::from_secs(1),
            commitment_config,
        )
    }

    pub fn wait_for_balance_with_commitment(
        &self,
        pubkey: &Pubkey,
        expected_balance: Option<u64>,
        commitment_config: CommitmentConfig,
    ) -> Option<u64> {
        const LAST: usize = 30;
        for run in 0..LAST {
            let balance_result = self.poll_get_balance_with_commitment(pubkey, commitment_config);
            if expected_balance.is_none() {
                return balance_result.ok();
            }
            trace!(
                "wait_for_balance_with_commitment [{}] {:?} {:?}",
                run,
                balance_result,
                expected_balance
            );
            if let (Some(expected_balance), Ok(balance_result)) = (expected_balance, balance_result)
            {
                if expected_balance == balance_result {
                    return Some(balance_result);
                }
            }
        }
        None
    }

    /// Poll the server to confirm a transaction.
    pub fn poll_for_signature(&self, signature: &Signature) -> ClientResult<()> {
        self.poll_for_signature_with_commitment(signature, self.commitment())
    }

    /// Poll the server to confirm a transaction.
    pub fn poll_for_signature_with_commitment(
        &self,
        signature: &Signature,
        commitment_config: CommitmentConfig,
    ) -> ClientResult<()> {
        let now = Instant::now();
        loop {
            if let Ok(Some(_)) =
                self.get_signature_status_with_commitment(signature, commitment_config)
            {
                break;
            }
            if now.elapsed().as_secs() > 15 {
                return Err(RpcError::ForUser(format!(
                    "signature not found after {} seconds",
                    now.elapsed().as_secs()
                ))
                .into());
            }
            sleep(Duration::from_millis(250));
        }
        Ok(())
    }

    /// Poll the server to confirm a transaction.
    pub fn poll_for_signature_confirmation(
        &self,
        signature: &Signature,
        min_confirmed_blocks: usize,
    ) -> ClientResult<usize> {
        let mut now = Instant::now();
        let mut confirmed_blocks = 0;
        loop {
            let response = self.get_num_blocks_since_signature_confirmation(signature);
            match response {
                Ok(count) => {
                    if confirmed_blocks != count {
                        info!(
                            "signature {} confirmed {} out of {} after {} ms",
                            signature,
                            count,
                            min_confirmed_blocks,
                            now.elapsed().as_millis()
                        );
                        now = Instant::now();
                        confirmed_blocks = count;
                    }
                    if count >= min_confirmed_blocks {
                        break;
                    }
                }
                Err(err) => {
                    debug!("check_confirmations request failed: {:?}", err);
                }
            };
            if now.elapsed().as_secs() > 20 {
                info!(
                    "signature {} confirmed {} out of {} failed after {} ms",
                    signature,
                    confirmed_blocks,
                    min_confirmed_blocks,
                    now.elapsed().as_millis()
                );
                if confirmed_blocks > 0 {
                    return Ok(confirmed_blocks);
                } else {
                    return Err(RpcError::ForUser(format!(
                        "signature not found after {} seconds",
                        now.elapsed().as_secs()
                    ))
                    .into());
                }
            }
            sleep(Duration::from_millis(250));
        }
        Ok(confirmed_blocks)
    }

    pub fn get_num_blocks_since_signature_confirmation(
        &self,
        signature: &Signature,
    ) -> ClientResult<usize> {
        let result: Response<Vec<Option<TransactionStatus>>> = self.send(
            RpcRequest::GetSignatureStatuses,
            json!([[signature.to_string()]]),
        )?;

        let confirmations = result.value[0]
            .clone()
            .ok_or_else(|| {
                ClientError::new_with_request(
                    ClientErrorKind::Custom("signature not found".to_string()),
                    RpcRequest::GetSignatureStatuses,
                )
            })?
            .confirmations
            .unwrap_or(MAX_LOCKOUT_HISTORY + 1);
        Ok(confirmations)
    }

    pub fn send_and_confirm_transaction_with_spinner(
        &self,
        transaction: &Transaction,
    ) -> ClientResult<Signature> {
        self.send_and_confirm_transaction_with_spinner_and_commitment(
            transaction,
            self.commitment(),
        )
    }

    pub fn send_and_confirm_transaction_with_spinner_and_commitment(
        &self,
        transaction: &Transaction,
        commitment: CommitmentConfig,
    ) -> ClientResult<Signature> {
        self.send_and_confirm_transaction_with_spinner_and_config(
            transaction,
            commitment,
            RpcSendTransactionConfig {
                preflight_commitment: Some(commitment.commitment),
                ..RpcSendTransactionConfig::default()
            },
        )
    }

    pub fn send_and_confirm_transaction_with_spinner_and_config(
        &self,
        transaction: &Transaction,
        commitment: CommitmentConfig,
        config: RpcSendTransactionConfig,
    ) -> ClientResult<Signature> {
        let recent_blockhash = if uses_durable_nonce(transaction).is_some() {
            self.get_recent_blockhash_with_commitment(CommitmentConfig::processed())?
                .value
                .0
        } else {
            transaction.message.recent_blockhash
        };
        let signature = self.send_transaction_with_config(transaction, config)?;
        self.confirm_transaction_with_spinner(&signature, &recent_blockhash, commitment)?;
        Ok(signature)
    }

    pub fn confirm_transaction_with_spinner(
        &self,
        signature: &Signature,
        recent_blockhash: &Hash,
        commitment: CommitmentConfig,
    ) -> ClientResult<()> {
        let desired_confirmations = if commitment.is_finalized() {
            MAX_LOCKOUT_HISTORY + 1
        } else {
            1
        };
        let mut confirmations = 0;

        let progress_bar = new_spinner_progress_bar();

        progress_bar.set_message(&format!(
            "[{}/{}] Finalizing transaction {}",
            confirmations, desired_confirmations, signature,
        ));

        let now = Instant::now();
        let confirm_transaction_initial_timeout = self
            .config
            .confirm_transaction_initial_timeout
            .unwrap_or_default();
        let (signature, status) = loop {
            // Get recent commitment in order to count confirmations for successful transactions
            let status = self
                .get_signature_status_with_commitment(signature, CommitmentConfig::processed())?;
            if status.is_none() {
                let blockhash_not_found = self
                    .get_fee_calculator_for_blockhash_with_commitment(
                        recent_blockhash,
                        CommitmentConfig::processed(),
                    )?
                    .value
                    .is_none();
                if blockhash_not_found && now.elapsed() >= confirm_transaction_initial_timeout {
                    break (signature, status);
                }
            } else {
                break (signature, status);
            }

            if cfg!(not(test)) {
                sleep(Duration::from_millis(500));
            }
        };
        if let Some(result) = status {
            if let Err(err) = result {
                return Err(err.into());
            }
        } else {
            return Err(RpcError::ForUser(
                "unable to confirm transaction. \
                                      This can happen in situations such as transaction expiration \
                                      and insufficient fee-payer funds"
                    .to_string(),
            )
            .into());
        }
        let now = Instant::now();
        loop {
            // Return when specified commitment is reached
            // Failed transactions have already been eliminated, `is_some` check is sufficient
            if self
                .get_signature_status_with_commitment(signature, commitment)?
                .is_some()
            {
                progress_bar.set_message("Transaction confirmed");
                progress_bar.finish_and_clear();
                return Ok(());
            }

            progress_bar.set_message(&format!(
                "[{}/{}] Finalizing transaction {}",
                min(confirmations + 1, desired_confirmations),
                desired_confirmations,
                signature,
            ));
            sleep(Duration::from_millis(500));
            confirmations = self
                .get_num_blocks_since_signature_confirmation(signature)
                .unwrap_or(confirmations);
            if now.elapsed().as_secs() >= MAX_HASH_AGE_IN_SECONDS as u64 {
                return Err(
                    RpcError::ForUser("transaction not finalized. \
                                      This can happen when a transaction lands in an abandoned fork. \
                                      Please retry.".to_string()).into(),
                );
            }
        }
    }

    pub fn send<T>(&self, request: RpcRequest, params: Value) -> ClientResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        assert!(params.is_array() || params.is_null());
        let response = self
            .sender
            .send(request, params)
            .map_err(|err| err.into_with_request(request))?;
        serde_json::from_value(response)
            .map_err(|err| ClientError::new_with_request(err.into(), request))
    }
}

fn serialize_encode_transaction(
    transaction: &Transaction,
    encoding: UiTransactionEncoding,
) -> ClientResult<String> {
    let serialized = serialize(transaction)
        .map_err(|e| ClientErrorKind::Custom(format!("transaction serialization failed: {}", e)))?;
    let encoded = match encoding {
        UiTransactionEncoding::Base58 => bs58::encode(serialized).into_string(),
        UiTransactionEncoding::Base64 => base64::encode(serialized),
        _ => {
            return Err(ClientErrorKind::Custom(format!(
                "unsupported transaction encoding: {}. Supported encodings: base58, base64",
                encoding
            ))
            .into())
        }
    };
    Ok(encoded)
}

#[derive(Debug, Default)]
pub struct GetConfirmedSignaturesForAddress2Config {
    pub before: Option<Signature>,
    pub until: Option<Signature>,
    pub limit: Option<usize>,
    pub commitment: Option<CommitmentConfig>,
}

fn new_spinner_progress_bar() -> ProgressBar {
    let progress_bar = ProgressBar::new(42);
    progress_bar
        .set_style(ProgressStyle::default_spinner().template("{spinner:.green} {wide_msg}"));
    progress_bar.enable_steady_tick(100);
    progress_bar
}

fn get_rpc_request_str(rpc_addr: SocketAddr, tls: bool) -> String {
    if tls {
        format!("https://{}", rpc_addr)
    } else {
        format!("http://{}", rpc_addr)
    }
}

fn parse_keyed_accounts(
    accounts: Vec<RpcKeyedAccount>,
    request: RpcRequest,
) -> ClientResult<Vec<(Pubkey, Account)>> {
    let mut pubkey_accounts: Vec<(Pubkey, Account)> = Vec::new();
    for RpcKeyedAccount { pubkey, account } in accounts.into_iter() {
        let pubkey = pubkey.parse().map_err(|_| {
            ClientError::new_with_request(
                RpcError::ParseError("Pubkey".to_string()).into(),
                request,
            )
        })?;
        pubkey_accounts.push((
            pubkey,
            account.decode().ok_or_else(|| {
                ClientError::new_with_request(
                    RpcError::ParseError("Account from rpc".to_string()).into(),
                    request,
                )
            })?,
        ));
    }
    Ok(pubkey_accounts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{client_error::ClientErrorKind, mock_sender::PUBKEY};
    use assert_matches::assert_matches;
    use jsonrpc_core::{futures::prelude::*, Error, IoHandler, Params};
    use jsonrpc_http_server::{AccessControlAllowOrigin, DomainsValidation, ServerBuilder};
    use serde_json::Number;
    use solana_sdk::{
        instruction::InstructionError, signature::Keypair, system_transaction,
        transaction::TransactionError,
    };
    use std::{io, sync::mpsc::channel, thread};

    #[test]
    fn test_send() {
        _test_send();
    }

    #[tokio::test(flavor = "current_thread")]
    #[should_panic(expected = "can call blocking only when running on the multi-threaded runtime")]
    async fn test_send_async_current_thread_should_panic() {
        _test_send();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_async_multi_thread() {
        _test_send();
    }

    fn _test_send() {
        let (sender, receiver) = channel();
        thread::spawn(move || {
            let rpc_addr = "0.0.0.0:0".parse().unwrap();
            let mut io = IoHandler::default();
            // Successful request
            io.add_method("getBalance", |_params: Params| {
                future::ok(Value::Number(Number::from(50)))
            });
            // Failed request
            io.add_method("getRecentBlockhash", |params: Params| {
                if params != Params::None {
                    future::err(Error::invalid_request())
                } else {
                    future::ok(Value::String(
                        "deadbeefXjn8o3yroDHxUtKsZZgoy4GPkPPXfouKNHhx".to_string(),
                    ))
                }
            });

            let server = ServerBuilder::new(io)
                .threads(1)
                .cors(DomainsValidation::AllowOnly(vec![
                    AccessControlAllowOrigin::Any,
                ]))
                .start_http(&rpc_addr)
                .expect("Unable to start RPC server");
            sender.send(*server.address()).unwrap();
            server.wait();
        });

        let rpc_addr = receiver.recv().unwrap();
        let rpc_client = RpcClient::new_socket(rpc_addr);

        let balance: u64 = rpc_client
            .send(
                RpcRequest::GetBalance,
                json!(["deadbeefXjn8o3yroDHxUtKsZZgoy4GPkPPXfouKNHhx"]),
            )
            .unwrap();
        assert_eq!(balance, 50);

        let blockhash: String = rpc_client
            .send(RpcRequest::GetRecentBlockhash, Value::Null)
            .unwrap();
        assert_eq!(blockhash, "deadbeefXjn8o3yroDHxUtKsZZgoy4GPkPPXfouKNHhx");

        // Send erroneous parameter
        let blockhash: ClientResult<String> =
            rpc_client.send(RpcRequest::GetRecentBlockhash, json!(["parameter"]));
        assert!(blockhash.is_err());
    }

    #[test]
    fn test_send_transaction() {
        let rpc_client = RpcClient::new_mock("succeeds".to_string());

        let key = Keypair::new();
        let to = solana_sdk::pubkey::new_rand();
        let blockhash = Hash::default();
        let tx = system_transaction::transfer(&key, &to, 50, blockhash);

        let signature = rpc_client.send_transaction(&tx);
        assert_eq!(signature.unwrap(), tx.signatures[0]);

        let rpc_client = RpcClient::new_mock("fails".to_string());

        let signature = rpc_client.send_transaction(&tx);
        assert!(signature.is_err());

        // Test bad signature returned from rpc node
        let rpc_client = RpcClient::new_mock("malicious".to_string());
        let signature = rpc_client.send_transaction(&tx);
        assert!(signature.is_err());
    }
    #[test]
    fn test_get_recent_blockhash() {
        let rpc_client = RpcClient::new_mock("succeeds".to_string());

        let expected_blockhash: Hash = PUBKEY.parse().unwrap();

        let (blockhash, _fee_calculator) = rpc_client.get_recent_blockhash().expect("blockhash ok");
        assert_eq!(blockhash, expected_blockhash);

        let rpc_client = RpcClient::new_mock("fails".to_string());

        assert!(rpc_client.get_recent_blockhash().is_err());
    }

    #[test]
    fn test_get_signature_status() {
        let signature = Signature::default();

        let rpc_client = RpcClient::new_mock("succeeds".to_string());
        let status = rpc_client.get_signature_status(&signature).unwrap();
        assert_eq!(status, Some(Ok(())));

        let rpc_client = RpcClient::new_mock("sig_not_found".to_string());
        let status = rpc_client.get_signature_status(&signature).unwrap();
        assert_eq!(status, None);

        let rpc_client = RpcClient::new_mock("account_in_use".to_string());
        let status = rpc_client.get_signature_status(&signature).unwrap();
        assert_eq!(status, Some(Err(TransactionError::AccountInUse)));
    }

    #[test]
    fn test_send_and_confirm_transaction() {
        let rpc_client = RpcClient::new_mock("succeeds".to_string());

        let key = Keypair::new();
        let to = solana_sdk::pubkey::new_rand();
        let blockhash = Hash::default();
        let tx = system_transaction::transfer(&key, &to, 50, blockhash);
        let result = rpc_client.send_and_confirm_transaction(&tx);
        result.unwrap();

        let rpc_client = RpcClient::new_mock("account_in_use".to_string());
        let result = rpc_client.send_and_confirm_transaction(&tx);
        assert!(result.is_err());

        let rpc_client = RpcClient::new_mock("instruction_error".to_string());
        let result = rpc_client.send_and_confirm_transaction(&tx);
        assert_matches!(
            result.unwrap_err().kind(),
            ClientErrorKind::TransactionError(TransactionError::InstructionError(
                0,
                InstructionError::UninitializedAccount
            ))
        );

        let rpc_client = RpcClient::new_mock("sig_not_found".to_string());
        let result = rpc_client.send_and_confirm_transaction(&tx);
        if let ClientErrorKind::Io(err) = result.unwrap_err().kind() {
            assert_eq!(err.kind(), io::ErrorKind::Other);
        }
    }

    #[test]
    fn test_rpc_client_thread() {
        let rpc_client = RpcClient::new_mock("succeeds".to_string());
        thread::spawn(move || rpc_client);
    }
}
