use crate::{
    cluster_query::*, feature::*, inflation::*, memo::*, nonce::*, program::*, spend_utils::*,
    stake::*, validator_info::*, vote::*,
};
use clap::{value_t_or_exit, App, AppSettings, Arg, ArgMatches, SubCommand};
use log::*;
use num_traits::FromPrimitive;
use serde_json::{self, Value};
use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_clap_utils::{
    self,
    fee_payer::{fee_payer_arg, FEE_PAYER_ARG},
    input_parsers::*,
    input_validators::*,
    keypair::*,
    memo::{memo_arg, MEMO_ARG},
    nonce::*,
    offline::*,
};
use solana_cli_output::{
    display::{build_balance_message, println_name_value},
    return_signers_with_config, CliAccount, CliSignature, CliSignatureVerificationStatus,
    CliTransaction, CliTransactionConfirmation, CliValidatorsSortOrder, OutputFormat,
    ReturnSignersConfig,
};
use solana_client::{
    blockhash_query::BlockhashQuery,
    client_error::{ClientError, ClientErrorKind, Result as ClientResult},
    nonce_utils,
    rpc_client::RpcClient,
    rpc_config::{
        RpcLargestAccountsFilter, RpcSendTransactionConfig, RpcTransactionConfig,
        RpcTransactionLogsFilter,
    },
    rpc_request::{RpcError, RpcResponseErrorData},
    rpc_response::{RpcKeyedAccount, RpcSimulateTransactionResult},
};
use solana_remote_wallet::remote_wallet::RemoteWalletManager;
use solana_sdk::{
    clock::{Epoch, Slot},
    commitment_config::CommitmentConfig,
    decode_error::DecodeError,
    hash::Hash,
    instruction::InstructionError,
    message::Message,
    pubkey::Pubkey,
    signature::{Signature, Signer, SignerError},
    stake::{
        self,
        instruction::LockupArgs,
        state::{Lockup, StakeAuthorize},
    },
    system_instruction::{self, SystemError},
    system_program,
    transaction::{Transaction, TransactionError},
};
use solana_transaction_status::{EncodedTransaction, UiTransactionEncoding};
use solana_vote_program::vote_state::VoteAuthorize;
use std::{
    collections::HashMap, error, fmt::Write as FmtWrite, fs::File, io::Write, str::FromStr,
    sync::Arc, time::Duration,
};
use thiserror::Error;

pub const DEFAULT_RPC_TIMEOUT_SECONDS: &str = "30";
pub const DEFAULT_CONFIRM_TX_TIMEOUT_SECONDS: &str = "5";
const CHECKED: bool = true;

#[derive(Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum CliCommand {
    // Cluster Query Commands
    Catchup {
        node_pubkey: Option<Pubkey>,
        node_json_rpc_url: Option<String>,
        follow: bool,
        our_localhost_port: Option<u16>,
        log: bool,
    },
    ClusterDate,
    ClusterVersion,
    CreateAddressWithSeed {
        from_pubkey: Option<Pubkey>,
        seed: String,
        program_id: Pubkey,
    },
    Feature(FeatureCliCommand),
    Inflation(InflationCliCommand),
    Fees {
        blockhash: Option<Hash>,
    },
    FirstAvailableBlock,
    GetBlock {
        slot: Option<Slot>,
    },
    GetBlockTime {
        slot: Option<Slot>,
    },
    GetEpoch,
    GetEpochInfo,
    GetGenesisHash,
    GetSlot,
    GetBlockHeight,
    GetTransactionCount,
    LargestAccounts {
        filter: Option<RpcLargestAccountsFilter>,
    },
    LeaderSchedule {
        epoch: Option<Epoch>,
    },
    LiveSlots,
    Logs {
        filter: RpcTransactionLogsFilter,
    },
    Ping {
        lamports: u64,
        interval: Duration,
        count: Option<u64>,
        timeout: Duration,
        blockhash: Option<Hash>,
        print_timestamp: bool,
    },
    Rent {
        data_length: usize,
        use_lamports_unit: bool,
    },
    ShowBlockProduction {
        epoch: Option<Epoch>,
        slot_limit: Option<u64>,
    },
    ShowGossip,
    ShowStakes {
        use_lamports_unit: bool,
        vote_account_pubkeys: Option<Vec<Pubkey>>,
    },
    ShowValidators {
        use_lamports_unit: bool,
        sort_order: CliValidatorsSortOrder,
        reverse_sort: bool,
        number_validators: bool,
    },
    Supply {
        print_accounts: bool,
    },
    TotalSupply,
    TransactionHistory {
        address: Pubkey,
        before: Option<Signature>,
        until: Option<Signature>,
        limit: usize,
        show_transactions: bool,
    },
    WaitForMaxStake {
        max_stake_percent: f32,
    },
    // Nonce commands
    AuthorizeNonceAccount {
        nonce_account: Pubkey,
        nonce_authority: SignerIndex,
        memo: Option<String>,
        new_authority: Pubkey,
    },
    CreateNonceAccount {
        nonce_account: SignerIndex,
        seed: Option<String>,
        nonce_authority: Option<Pubkey>,
        memo: Option<String>,
        amount: SpendAmount,
    },
    GetNonce(Pubkey),
    NewNonce {
        nonce_account: Pubkey,
        nonce_authority: SignerIndex,
        memo: Option<String>,
    },
    ShowNonceAccount {
        nonce_account_pubkey: Pubkey,
        use_lamports_unit: bool,
    },
    WithdrawFromNonceAccount {
        nonce_account: Pubkey,
        nonce_authority: SignerIndex,
        memo: Option<String>,
        destination_account_pubkey: Pubkey,
        lamports: u64,
    },
    // Program Deployment
    Deploy {
        program_location: String,
        address: Option<SignerIndex>,
        use_deprecated_loader: bool,
        allow_excessive_balance: bool,
    },
    Program(ProgramCliCommand),
    // Stake Commands
    CreateStakeAccount {
        stake_account: SignerIndex,
        seed: Option<String>,
        staker: Option<Pubkey>,
        withdrawer: Option<Pubkey>,
        lockup: Lockup,
        amount: SpendAmount,
        sign_only: bool,
        dump_transaction_message: bool,
        blockhash_query: BlockhashQuery,
        nonce_account: Option<Pubkey>,
        nonce_authority: SignerIndex,
        memo: Option<String>,
        fee_payer: SignerIndex,
        from: SignerIndex,
    },
    DeactivateStake {
        stake_account_pubkey: Pubkey,
        stake_authority: SignerIndex,
        sign_only: bool,
        dump_transaction_message: bool,
        blockhash_query: BlockhashQuery,
        nonce_account: Option<Pubkey>,
        nonce_authority: SignerIndex,
        memo: Option<String>,
        seed: Option<String>,
        fee_payer: SignerIndex,
    },
    DelegateStake {
        stake_account_pubkey: Pubkey,
        vote_account_pubkey: Pubkey,
        stake_authority: SignerIndex,
        force: bool,
        sign_only: bool,
        dump_transaction_message: bool,
        blockhash_query: BlockhashQuery,
        nonce_account: Option<Pubkey>,
        nonce_authority: SignerIndex,
        memo: Option<String>,
        fee_payer: SignerIndex,
    },
    SplitStake {
        stake_account_pubkey: Pubkey,
        stake_authority: SignerIndex,
        sign_only: bool,
        dump_transaction_message: bool,
        blockhash_query: BlockhashQuery,
        nonce_account: Option<Pubkey>,
        nonce_authority: SignerIndex,
        memo: Option<String>,
        split_stake_account: SignerIndex,
        seed: Option<String>,
        lamports: u64,
        fee_payer: SignerIndex,
    },
    MergeStake {
        stake_account_pubkey: Pubkey,
        source_stake_account_pubkey: Pubkey,
        stake_authority: SignerIndex,
        sign_only: bool,
        dump_transaction_message: bool,
        blockhash_query: BlockhashQuery,
        nonce_account: Option<Pubkey>,
        nonce_authority: SignerIndex,
        memo: Option<String>,
        fee_payer: SignerIndex,
    },
    ShowStakeHistory {
        use_lamports_unit: bool,
        limit_results: usize,
    },
    ShowStakeAccount {
        pubkey: Pubkey,
        use_lamports_unit: bool,
        with_rewards: Option<usize>,
    },
    StakeAuthorize {
        stake_account_pubkey: Pubkey,
        new_authorizations: Vec<(StakeAuthorize, Pubkey, SignerIndex)>,
        sign_only: bool,
        dump_transaction_message: bool,
        blockhash_query: BlockhashQuery,
        nonce_account: Option<Pubkey>,
        nonce_authority: SignerIndex,
        memo: Option<String>,
        fee_payer: SignerIndex,
        custodian: Option<SignerIndex>,
        no_wait: bool,
    },
    StakeSetLockup {
        stake_account_pubkey: Pubkey,
        lockup: LockupArgs,
        custodian: SignerIndex,
        sign_only: bool,
        dump_transaction_message: bool,
        blockhash_query: BlockhashQuery,
        nonce_account: Option<Pubkey>,
        nonce_authority: SignerIndex,
        memo: Option<String>,
        fee_payer: SignerIndex,
    },
    WithdrawStake {
        stake_account_pubkey: Pubkey,
        destination_account_pubkey: Pubkey,
        amount: SpendAmount,
        withdraw_authority: SignerIndex,
        custodian: Option<SignerIndex>,
        sign_only: bool,
        dump_transaction_message: bool,
        blockhash_query: BlockhashQuery,
        nonce_account: Option<Pubkey>,
        nonce_authority: SignerIndex,
        memo: Option<String>,
        seed: Option<String>,
        fee_payer: SignerIndex,
    },
    // Validator Info Commands
    GetValidatorInfo(Option<Pubkey>),
    SetValidatorInfo {
        validator_info: Value,
        force_keybase: bool,
        info_pubkey: Option<Pubkey>,
    },
    // Vote Commands
    CreateVoteAccount {
        vote_account: SignerIndex,
        seed: Option<String>,
        identity_account: SignerIndex,
        authorized_voter: Option<Pubkey>,
        authorized_withdrawer: Option<Pubkey>,
        commission: u8,
        memo: Option<String>,
    },
    ShowVoteAccount {
        pubkey: Pubkey,
        use_lamports_unit: bool,
        with_rewards: Option<usize>,
    },
    WithdrawFromVoteAccount {
        vote_account_pubkey: Pubkey,
        destination_account_pubkey: Pubkey,
        withdraw_authority: SignerIndex,
        withdraw_amount: SpendAmount,
        memo: Option<String>,
    },
    VoteAuthorize {
        vote_account_pubkey: Pubkey,
        new_authorized_pubkey: Pubkey,
        vote_authorize: VoteAuthorize,
        memo: Option<String>,
        authorized: SignerIndex,
        new_authorized: Option<SignerIndex>,
    },
    VoteUpdateValidator {
        vote_account_pubkey: Pubkey,
        new_identity_account: SignerIndex,
        withdraw_authority: SignerIndex,
        memo: Option<String>,
    },
    VoteUpdateCommission {
        vote_account_pubkey: Pubkey,
        commission: u8,
        withdraw_authority: SignerIndex,
        memo: Option<String>,
    },
    // Wallet Commands
    Address,
    Airdrop {
        pubkey: Option<Pubkey>,
        lamports: u64,
    },
    Balance {
        pubkey: Option<Pubkey>,
        use_lamports_unit: bool,
    },
    Confirm(Signature),
    DecodeTransaction(Transaction),
    ResolveSigner(Option<String>),
    ShowAccount {
        pubkey: Pubkey,
        output_file: Option<String>,
        use_lamports_unit: bool,
    },
    Transfer {
        amount: SpendAmount,
        to: Pubkey,
        from: SignerIndex,
        sign_only: bool,
        dump_transaction_message: bool,
        allow_unfunded_recipient: bool,
        no_wait: bool,
        blockhash_query: BlockhashQuery,
        nonce_account: Option<Pubkey>,
        nonce_authority: SignerIndex,
        memo: Option<String>,
        fee_payer: SignerIndex,
        derived_address_seed: Option<String>,
        derived_address_program_id: Option<Pubkey>,
    },
}

#[derive(Debug, PartialEq)]
pub struct CliCommandInfo {
    pub command: CliCommand,
    pub signers: CliSigners,
}

#[derive(Debug, Error)]
pub enum CliError {
    #[error("Bad parameter: {0}")]
    BadParameter(String),
    #[error(transparent)]
    ClientError(#[from] ClientError),
    #[error("Command not recognized: {0}")]
    CommandNotRecognized(String),
    #[error("Account {1} has insufficient funds for fee ({0} SOL)")]
    InsufficientFundsForFee(f64, Pubkey),
    #[error("Account {1} has insufficient funds for spend ({0} SOL)")]
    InsufficientFundsForSpend(f64, Pubkey),
    #[error("Account {2} has insufficient funds for spend ({0} SOL) + fee ({1} SOL)")]
    InsufficientFundsForSpendAndFee(f64, f64, Pubkey),
    #[error(transparent)]
    InvalidNonce(nonce_utils::Error),
    #[error("Dynamic program error: {0}")]
    DynamicProgramError(String),
    #[error("RPC request error: {0}")]
    RpcRequestError(String),
    #[error("Keypair file not found: {0}")]
    KeypairFileNotFound(String),
}

impl From<Box<dyn error::Error>> for CliError {
    fn from(error: Box<dyn error::Error>) -> Self {
        CliError::DynamicProgramError(error.to_string())
    }
}

impl From<nonce_utils::Error> for CliError {
    fn from(error: nonce_utils::Error) -> Self {
        match error {
            nonce_utils::Error::Client(client_error) => Self::RpcRequestError(client_error),
            _ => Self::InvalidNonce(error),
        }
    }
}

pub enum SettingType {
    Explicit,
    Computed,
    SystemDefault,
}

pub struct CliConfig<'a> {
    pub command: CliCommand,
    pub json_rpc_url: String,
    pub websocket_url: String,
    pub signers: Vec<&'a dyn Signer>,
    pub keypair_path: String,
    pub rpc_client: Option<Arc<RpcClient>>,
    pub rpc_timeout: Duration,
    pub verbose: bool,
    pub output_format: OutputFormat,
    pub commitment: CommitmentConfig,
    pub send_transaction_config: RpcSendTransactionConfig,
    pub confirm_transaction_initial_timeout: Duration,
    pub address_labels: HashMap<String, String>,
}

impl CliConfig<'_> {
    fn default_keypair_path() -> String {
        solana_cli_config::Config::default().keypair_path
    }

    fn default_json_rpc_url() -> String {
        solana_cli_config::Config::default().json_rpc_url
    }

    fn default_websocket_url() -> String {
        solana_cli_config::Config::default().websocket_url
    }

    fn default_commitment() -> CommitmentConfig {
        CommitmentConfig::confirmed()
    }

    fn first_nonempty_setting(
        settings: std::vec::Vec<(SettingType, String)>,
    ) -> (SettingType, String) {
        settings
            .into_iter()
            .find(|(_, value)| !value.is_empty())
            .expect("no nonempty setting")
    }

    fn first_setting_is_some<T>(
        settings: std::vec::Vec<(SettingType, Option<T>)>,
    ) -> (SettingType, T) {
        let (setting_type, setting_option) = settings
            .into_iter()
            .find(|(_, value)| value.is_some())
            .expect("all settings none");
        (setting_type, setting_option.unwrap())
    }

    pub fn compute_websocket_url_setting(
        websocket_cmd_url: &str,
        websocket_cfg_url: &str,
        json_rpc_cmd_url: &str,
        json_rpc_cfg_url: &str,
    ) -> (SettingType, String) {
        Self::first_nonempty_setting(vec![
            (SettingType::Explicit, websocket_cmd_url.to_string()),
            (SettingType::Explicit, websocket_cfg_url.to_string()),
            (
                SettingType::Computed,
                solana_cli_config::Config::compute_websocket_url(&normalize_to_url_if_moniker(
                    json_rpc_cmd_url,
                )),
            ),
            (
                SettingType::Computed,
                solana_cli_config::Config::compute_websocket_url(&normalize_to_url_if_moniker(
                    json_rpc_cfg_url,
                )),
            ),
            (SettingType::SystemDefault, Self::default_websocket_url()),
        ])
    }

    pub fn compute_json_rpc_url_setting(
        json_rpc_cmd_url: &str,
        json_rpc_cfg_url: &str,
    ) -> (SettingType, String) {
        let (setting_type, url_or_moniker) = Self::first_nonempty_setting(vec![
            (SettingType::Explicit, json_rpc_cmd_url.to_string()),
            (SettingType::Explicit, json_rpc_cfg_url.to_string()),
            (SettingType::SystemDefault, Self::default_json_rpc_url()),
        ]);
        (setting_type, normalize_to_url_if_moniker(&url_or_moniker))
    }

    pub fn compute_keypair_path_setting(
        keypair_cmd_path: &str,
        keypair_cfg_path: &str,
    ) -> (SettingType, String) {
        Self::first_nonempty_setting(vec![
            (SettingType::Explicit, keypair_cmd_path.to_string()),
            (SettingType::Explicit, keypair_cfg_path.to_string()),
            (SettingType::SystemDefault, Self::default_keypair_path()),
        ])
    }

    pub fn compute_commitment_config(
        commitment_cmd: &str,
        commitment_cfg: &str,
    ) -> (SettingType, CommitmentConfig) {
        Self::first_setting_is_some(vec![
            (
                SettingType::Explicit,
                CommitmentConfig::from_str(commitment_cmd).ok(),
            ),
            (
                SettingType::Explicit,
                CommitmentConfig::from_str(commitment_cfg).ok(),
            ),
            (SettingType::SystemDefault, Some(Self::default_commitment())),
        ])
    }

    pub(crate) fn pubkey(&self) -> Result<Pubkey, SignerError> {
        if !self.signers.is_empty() {
            self.signers[0].try_pubkey()
        } else {
            Err(SignerError::Custom(
                "Default keypair must be set if pubkey arg not provided".to_string(),
            ))
        }
    }

    pub fn recent_for_tests() -> Self {
        Self {
            commitment: CommitmentConfig::processed(),
            send_transaction_config: RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentConfig::processed().commitment),
                ..RpcSendTransactionConfig::default()
            },
            ..Self::default()
        }
    }
}

impl Default for CliConfig<'_> {
    fn default() -> CliConfig<'static> {
        CliConfig {
            command: CliCommand::Balance {
                pubkey: Some(Pubkey::default()),
                use_lamports_unit: false,
            },
            json_rpc_url: Self::default_json_rpc_url(),
            websocket_url: Self::default_websocket_url(),
            signers: Vec::new(),
            keypair_path: Self::default_keypair_path(),
            rpc_client: None,
            rpc_timeout: Duration::from_secs(u64::from_str(DEFAULT_RPC_TIMEOUT_SECONDS).unwrap()),
            verbose: false,
            output_format: OutputFormat::Display,
            commitment: CommitmentConfig::confirmed(),
            send_transaction_config: RpcSendTransactionConfig::default(),
            confirm_transaction_initial_timeout: Duration::from_secs(
                u64::from_str(DEFAULT_CONFIRM_TX_TIMEOUT_SECONDS).unwrap(),
            ),
            address_labels: HashMap::new(),
        }
    }
}

pub fn parse_command(
    matches: &ArgMatches<'_>,
    default_signer: &DefaultSigner,
    wallet_manager: &mut Option<Arc<RemoteWalletManager>>,
) -> Result<CliCommandInfo, Box<dyn error::Error>> {
    let response = match matches.subcommand() {
        // Cluster Query Commands
        ("catchup", Some(matches)) => parse_catchup(matches, wallet_manager),
        ("cluster-date", Some(_matches)) => Ok(CliCommandInfo {
            command: CliCommand::ClusterDate,
            signers: vec![],
        }),
        ("cluster-version", Some(_matches)) => Ok(CliCommandInfo {
            command: CliCommand::ClusterVersion,
            signers: vec![],
        }),
        ("create-address-with-seed", Some(matches)) => {
            parse_create_address_with_seed(matches, default_signer, wallet_manager)
        }
        ("feature", Some(matches)) => {
            parse_feature_subcommand(matches, default_signer, wallet_manager)
        }
        ("fees", Some(matches)) => {
            let blockhash = value_of::<Hash>(matches, "blockhash");
            Ok(CliCommandInfo {
                command: CliCommand::Fees { blockhash },
                signers: vec![],
            })
        }
        ("first-available-block", Some(_matches)) => Ok(CliCommandInfo {
            command: CliCommand::FirstAvailableBlock,
            signers: vec![],
        }),
        ("block", Some(matches)) => parse_get_block(matches),
        ("block-time", Some(matches)) => parse_get_block_time(matches),
        ("epoch-info", Some(matches)) => parse_get_epoch_info(matches),
        ("genesis-hash", Some(_matches)) => Ok(CliCommandInfo {
            command: CliCommand::GetGenesisHash,
            signers: vec![],
        }),
        ("epoch", Some(matches)) => parse_get_epoch(matches),
        ("slot", Some(matches)) => parse_get_slot(matches),
        ("block-height", Some(matches)) => parse_get_block_height(matches),
        ("inflation", Some(matches)) => {
            parse_inflation_subcommand(matches, default_signer, wallet_manager)
        }
        ("largest-accounts", Some(matches)) => parse_largest_accounts(matches),
        ("supply", Some(matches)) => parse_supply(matches),
        ("total-supply", Some(matches)) => parse_total_supply(matches),
        ("transaction-count", Some(matches)) => parse_get_transaction_count(matches),
        ("leader-schedule", Some(matches)) => parse_leader_schedule(matches),
        ("ping", Some(matches)) => parse_cluster_ping(matches, default_signer, wallet_manager),
        ("live-slots", Some(_matches)) => Ok(CliCommandInfo {
            command: CliCommand::LiveSlots,
            signers: vec![],
        }),
        ("logs", Some(matches)) => parse_logs(matches, wallet_manager),
        ("block-production", Some(matches)) => parse_show_block_production(matches),
        ("gossip", Some(_matches)) => Ok(CliCommandInfo {
            command: CliCommand::ShowGossip,
            signers: vec![],
        }),
        ("stakes", Some(matches)) => parse_show_stakes(matches, wallet_manager),
        ("validators", Some(matches)) => parse_show_validators(matches),
        ("transaction-history", Some(matches)) => {
            parse_transaction_history(matches, wallet_manager)
        }
        // Nonce Commands
        ("authorize-nonce-account", Some(matches)) => {
            parse_authorize_nonce_account(matches, default_signer, wallet_manager)
        }
        ("create-nonce-account", Some(matches)) => {
            parse_nonce_create_account(matches, default_signer, wallet_manager)
        }
        ("nonce", Some(matches)) => parse_get_nonce(matches, wallet_manager),
        ("new-nonce", Some(matches)) => parse_new_nonce(matches, default_signer, wallet_manager),
        ("nonce-account", Some(matches)) => parse_show_nonce_account(matches, wallet_manager),
        ("withdraw-from-nonce-account", Some(matches)) => {
            parse_withdraw_from_nonce_account(matches, default_signer, wallet_manager)
        }
        // Program Deployment
        ("deploy", Some(matches)) => {
            let (address_signer, _address) = signer_of(matches, "address_signer", wallet_manager)?;
            let mut signers = vec![default_signer.signer_from_path(matches, wallet_manager)?];
            let address = address_signer.map(|signer| {
                signers.push(signer);
                1
            });

            Ok(CliCommandInfo {
                command: CliCommand::Deploy {
                    program_location: matches.value_of("program_location").unwrap().to_string(),
                    address,
                    use_deprecated_loader: matches.is_present("use_deprecated_loader"),
                    allow_excessive_balance: matches.is_present("allow_excessive_balance"),
                },
                signers,
            })
        }
        ("program", Some(matches)) => {
            parse_program_subcommand(matches, default_signer, wallet_manager)
        }
        ("wait-for-max-stake", Some(matches)) => {
            let max_stake_percent = value_t_or_exit!(matches, "max_percent", f32);
            Ok(CliCommandInfo {
                command: CliCommand::WaitForMaxStake { max_stake_percent },
                signers: vec![],
            })
        }
        // Stake Commands
        ("create-stake-account", Some(matches)) => {
            parse_create_stake_account(matches, default_signer, wallet_manager)
        }
        ("delegate-stake", Some(matches)) => {
            parse_stake_delegate_stake(matches, default_signer, wallet_manager)
        }
        ("withdraw-stake", Some(matches)) => {
            parse_stake_withdraw_stake(matches, default_signer, wallet_manager)
        }
        ("deactivate-stake", Some(matches)) => {
            parse_stake_deactivate_stake(matches, default_signer, wallet_manager)
        }
        ("split-stake", Some(matches)) => {
            parse_split_stake(matches, default_signer, wallet_manager)
        }
        ("merge-stake", Some(matches)) => {
            parse_merge_stake(matches, default_signer, wallet_manager)
        }
        ("stake-authorize", Some(matches)) => {
            parse_stake_authorize(matches, default_signer, wallet_manager)
        }
        ("stake-set-lockup", Some(matches)) => {
            parse_stake_set_lockup(matches, default_signer, wallet_manager)
        }
        ("stake-account", Some(matches)) => parse_show_stake_account(matches, wallet_manager),
        ("stake-history", Some(matches)) => parse_show_stake_history(matches),
        // Validator Info Commands
        ("validator-info", Some(matches)) => match matches.subcommand() {
            ("publish", Some(matches)) => {
                parse_validator_info_command(matches, default_signer, wallet_manager)
            }
            ("get", Some(matches)) => parse_get_validator_info_command(matches),
            _ => unreachable!(),
        },
        // Vote Commands
        ("create-vote-account", Some(matches)) => {
            parse_create_vote_account(matches, default_signer, wallet_manager)
        }
        ("vote-update-validator", Some(matches)) => {
            parse_vote_update_validator(matches, default_signer, wallet_manager)
        }
        ("vote-update-commission", Some(matches)) => {
            parse_vote_update_commission(matches, default_signer, wallet_manager)
        }
        ("vote-authorize-voter", Some(matches)) => parse_vote_authorize(
            matches,
            default_signer,
            wallet_manager,
            VoteAuthorize::Voter,
            !CHECKED,
        ),
        ("vote-authorize-withdrawer", Some(matches)) => parse_vote_authorize(
            matches,
            default_signer,
            wallet_manager,
            VoteAuthorize::Withdrawer,
            !CHECKED,
        ),
        ("vote-authorize-voter-checked", Some(matches)) => parse_vote_authorize(
            matches,
            default_signer,
            wallet_manager,
            VoteAuthorize::Voter,
            CHECKED,
        ),
        ("vote-authorize-withdrawer-checked", Some(matches)) => parse_vote_authorize(
            matches,
            default_signer,
            wallet_manager,
            VoteAuthorize::Withdrawer,
            CHECKED,
        ),
        ("vote-account", Some(matches)) => parse_vote_get_account_command(matches, wallet_manager),
        ("withdraw-from-vote-account", Some(matches)) => {
            parse_withdraw_from_vote_account(matches, default_signer, wallet_manager)
        }
        // Wallet Commands
        ("address", Some(matches)) => Ok(CliCommandInfo {
            command: CliCommand::Address,
            signers: vec![default_signer.signer_from_path(matches, wallet_manager)?],
        }),
        ("airdrop", Some(matches)) => {
            let pubkey = pubkey_of_signer(matches, "to", wallet_manager)?;
            let signers = if pubkey.is_some() {
                vec![]
            } else {
                vec![default_signer.signer_from_path(matches, wallet_manager)?]
            };
            let lamports = lamports_of_sol(matches, "amount").unwrap();
            Ok(CliCommandInfo {
                command: CliCommand::Airdrop { pubkey, lamports },
                signers,
            })
        }
        ("balance", Some(matches)) => {
            let pubkey = pubkey_of_signer(matches, "pubkey", wallet_manager)?;
            let signers = if pubkey.is_some() {
                vec![]
            } else {
                vec![default_signer.signer_from_path(matches, wallet_manager)?]
            };
            Ok(CliCommandInfo {
                command: CliCommand::Balance {
                    pubkey,
                    use_lamports_unit: matches.is_present("lamports"),
                },
                signers,
            })
        }
        ("confirm", Some(matches)) => match matches.value_of("signature").unwrap().parse() {
            Ok(signature) => Ok(CliCommandInfo {
                command: CliCommand::Confirm(signature),
                signers: vec![],
            }),
            _ => Err(CliError::BadParameter("Invalid signature".to_string())),
        },
        ("decode-transaction", Some(matches)) => {
            let blob = value_t_or_exit!(matches, "transaction", String);
            let encoding = match matches.value_of("encoding").unwrap() {
                "base58" => UiTransactionEncoding::Base58,
                "base64" => UiTransactionEncoding::Base64,
                _ => unreachable!(),
            };

            let encoded_transaction = EncodedTransaction::Binary(blob, encoding);
            if let Some(transaction) = encoded_transaction.decode() {
                Ok(CliCommandInfo {
                    command: CliCommand::DecodeTransaction(transaction),
                    signers: vec![],
                })
            } else {
                Err(CliError::BadParameter(
                    "Unable to decode transaction".to_string(),
                ))
            }
        }
        ("account", Some(matches)) => {
            let account_pubkey =
                pubkey_of_signer(matches, "account_pubkey", wallet_manager)?.unwrap();
            let output_file = matches.value_of("output_file");
            let use_lamports_unit = matches.is_present("lamports");
            Ok(CliCommandInfo {
                command: CliCommand::ShowAccount {
                    pubkey: account_pubkey,
                    output_file: output_file.map(ToString::to_string),
                    use_lamports_unit,
                },
                signers: vec![],
            })
        }
        ("resolve-signer", Some(matches)) => {
            let signer_path = resolve_signer(matches, "signer", wallet_manager)?;
            Ok(CliCommandInfo {
                command: CliCommand::ResolveSigner(signer_path),
                signers: vec![],
            })
        }
        ("transfer", Some(matches)) => {
            let amount = SpendAmount::new_from_matches(matches, "amount");
            let to = pubkey_of_signer(matches, "to", wallet_manager)?.unwrap();
            let sign_only = matches.is_present(SIGN_ONLY_ARG.name);
            let dump_transaction_message = matches.is_present(DUMP_TRANSACTION_MESSAGE.name);
            let no_wait = matches.is_present("no_wait");
            let blockhash_query = BlockhashQuery::new_from_matches(matches);
            let nonce_account = pubkey_of_signer(matches, NONCE_ARG.name, wallet_manager)?;
            let (nonce_authority, nonce_authority_pubkey) =
                signer_of(matches, NONCE_AUTHORITY_ARG.name, wallet_manager)?;
            let memo = matches.value_of(MEMO_ARG.name).map(String::from);
            let (fee_payer, fee_payer_pubkey) =
                signer_of(matches, FEE_PAYER_ARG.name, wallet_manager)?;
            let (from, from_pubkey) = signer_of(matches, "from", wallet_manager)?;
            let allow_unfunded_recipient = matches.is_present("allow_unfunded_recipient");

            let mut bulk_signers = vec![fee_payer, from];
            if nonce_account.is_some() {
                bulk_signers.push(nonce_authority);
            }

            let signer_info =
                default_signer.generate_unique_signers(bulk_signers, matches, wallet_manager)?;

            let derived_address_seed = matches
                .value_of("derived_address_seed")
                .map(|s| s.to_string());
            let derived_address_program_id =
                resolve_derived_address_program_id(matches, "derived_address_program_id");

            Ok(CliCommandInfo {
                command: CliCommand::Transfer {
                    amount,
                    to,
                    sign_only,
                    dump_transaction_message,
                    allow_unfunded_recipient,
                    no_wait,
                    blockhash_query,
                    nonce_account,
                    nonce_authority: signer_info.index_of(nonce_authority_pubkey).unwrap(),
                    memo,
                    fee_payer: signer_info.index_of(fee_payer_pubkey).unwrap(),
                    from: signer_info.index_of(from_pubkey).unwrap(),
                    derived_address_seed,
                    derived_address_program_id,
                },
                signers: signer_info.signers,
            })
        }
        ("rent", Some(matches)) => {
            let data_length = value_of::<RentLengthValue>(matches, "data_length")
                .unwrap()
                .length();
            let use_lamports_unit = matches.is_present("lamports");
            Ok(CliCommandInfo {
                command: CliCommand::Rent {
                    data_length,
                    use_lamports_unit,
                },
                signers: vec![],
            })
        }
        //
        ("", None) => {
            eprintln!("{}", matches.usage());
            Err(CliError::CommandNotRecognized(
                "no subcommand given".to_string(),
            ))
        }
        _ => unreachable!(),
    }?;
    Ok(response)
}

pub type ProcessResult = Result<String, Box<dyn std::error::Error>>;

fn resolve_derived_address_program_id(matches: &ArgMatches<'_>, arg_name: &str) -> Option<Pubkey> {
    matches.value_of(arg_name).and_then(|v| match v {
        "NONCE" => Some(system_program::id()),
        "STAKE" => Some(stake::program::id()),
        "VOTE" => Some(solana_vote_program::id()),
        _ => pubkey_of(matches, arg_name),
    })
}

pub fn parse_create_address_with_seed(
    matches: &ArgMatches<'_>,
    default_signer: &DefaultSigner,
    wallet_manager: &mut Option<Arc<RemoteWalletManager>>,
) -> Result<CliCommandInfo, CliError> {
    let from_pubkey = pubkey_of_signer(matches, "from", wallet_manager)?;
    let signers = if from_pubkey.is_some() {
        vec![]
    } else {
        vec![default_signer.signer_from_path(matches, wallet_manager)?]
    };

    let program_id = resolve_derived_address_program_id(matches, "program_id").unwrap();

    let seed = matches.value_of("seed").unwrap().to_string();

    Ok(CliCommandInfo {
        command: CliCommand::CreateAddressWithSeed {
            from_pubkey,
            seed,
            program_id,
        },
        signers,
    })
}

fn process_create_address_with_seed(
    config: &CliConfig,
    from_pubkey: Option<&Pubkey>,
    seed: &str,
    program_id: &Pubkey,
) -> ProcessResult {
    let from_pubkey = if let Some(pubkey) = from_pubkey {
        *pubkey
    } else {
        config.pubkey()?
    };
    let address = Pubkey::create_with_seed(&from_pubkey, seed, program_id)?;
    Ok(address.to_string())
}

fn process_airdrop(
    rpc_client: &RpcClient,
    config: &CliConfig,
    pubkey: &Option<Pubkey>,
    lamports: u64,
) -> ProcessResult {
    let pubkey = if let Some(pubkey) = pubkey {
        *pubkey
    } else {
        config.pubkey()?
    };
    println!(
        "Requesting airdrop of {}",
        build_balance_message(lamports, false, true),
    );

    let pre_balance = rpc_client.get_balance(&pubkey)?;

    let result = request_and_confirm_airdrop(rpc_client, config, &pubkey, lamports);
    if let Ok(signature) = result {
        let signature_cli_message = log_instruction_custom_error::<SystemError>(result, config)?;
        println!("{}", signature_cli_message);

        let current_balance = rpc_client.get_balance(&pubkey)?;

        if current_balance < pre_balance.saturating_add(lamports) {
            println!("Balance unchanged");
            println!("Run `solana confirm -v {:?}` for more info", signature);
            Ok("".to_string())
        } else {
            Ok(build_balance_message(current_balance, false, true))
        }
    } else {
        log_instruction_custom_error::<SystemError>(result, config)
    }
}

fn process_balance(
    rpc_client: &RpcClient,
    config: &CliConfig,
    pubkey: &Option<Pubkey>,
    use_lamports_unit: bool,
) -> ProcessResult {
    let pubkey = if let Some(pubkey) = pubkey {
        *pubkey
    } else {
        config.pubkey()?
    };
    let balance = rpc_client.get_balance(&pubkey)?;
    Ok(build_balance_message(balance, use_lamports_unit, true))
}

fn process_confirm(
    rpc_client: &RpcClient,
    config: &CliConfig,
    signature: &Signature,
) -> ProcessResult {
    match rpc_client.get_signature_statuses_with_history(&[*signature]) {
        Ok(status) => {
            let cli_transaction = if let Some(transaction_status) = &status.value[0] {
                let mut transaction = None;
                let mut get_transaction_error = None;
                if config.verbose {
                    match rpc_client.get_transaction_with_config(
                        signature,
                        RpcTransactionConfig {
                            encoding: Some(UiTransactionEncoding::Base64),
                            commitment: Some(CommitmentConfig::confirmed()),
                        },
                    ) {
                        Ok(confirmed_transaction) => {
                            let decoded_transaction = confirmed_transaction
                                .transaction
                                .transaction
                                .decode()
                                .expect("Successful decode");
                            let json_transaction = EncodedTransaction::encode(
                                decoded_transaction.clone(),
                                UiTransactionEncoding::Json,
                            );

                            transaction = Some(CliTransaction {
                                transaction: json_transaction,
                                meta: confirmed_transaction.transaction.meta,
                                block_time: confirmed_transaction.block_time,
                                slot: Some(confirmed_transaction.slot),
                                decoded_transaction,
                                prefix: "  ".to_string(),
                                sigverify_status: vec![],
                            });
                        }
                        Err(err) => {
                            get_transaction_error = Some(format!("{:?}", err));
                        }
                    }
                }
                CliTransactionConfirmation {
                    confirmation_status: Some(transaction_status.confirmation_status()),
                    transaction,
                    get_transaction_error,
                    err: transaction_status.err.clone(),
                }
            } else {
                CliTransactionConfirmation {
                    confirmation_status: None,
                    transaction: None,
                    get_transaction_error: None,
                    err: None,
                }
            };
            Ok(config.output_format.formatted_string(&cli_transaction))
        }
        Err(err) => Err(CliError::RpcRequestError(format!("Unable to confirm: {}", err)).into()),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn process_decode_transaction(config: &CliConfig, transaction: &Transaction) -> ProcessResult {
    let sigverify_status = CliSignatureVerificationStatus::verify_transaction(transaction);
    let decode_transaction = CliTransaction {
        decoded_transaction: transaction.clone(),
        transaction: EncodedTransaction::encode(transaction.clone(), UiTransactionEncoding::Json),
        meta: None,
        block_time: None,
        slot: None,
        prefix: "".to_string(),
        sigverify_status,
    };
    Ok(config.output_format.formatted_string(&decode_transaction))
}

fn process_show_account(
    rpc_client: &RpcClient,
    config: &CliConfig,
    account_pubkey: &Pubkey,
    output_file: &Option<String>,
    use_lamports_unit: bool,
) -> ProcessResult {
    let account = rpc_client.get_account(account_pubkey)?;
    let data = account.data.clone();
    let cli_account = CliAccount {
        keyed_account: RpcKeyedAccount {
            pubkey: account_pubkey.to_string(),
            account: UiAccount::encode(
                account_pubkey,
                &account,
                UiAccountEncoding::Base64,
                None,
                None,
            ),
        },
        use_lamports_unit,
    };

    let mut account_string = config.output_format.formatted_string(&cli_account);

    if config.output_format == OutputFormat::Display
        || config.output_format == OutputFormat::DisplayVerbose
    {
        if let Some(output_file) = output_file {
            let mut f = File::create(output_file)?;
            f.write_all(&data)?;
            writeln!(&mut account_string)?;
            writeln!(&mut account_string, "Wrote account data to {}", output_file)?;
        } else if !data.is_empty() {
            use pretty_hex::*;
            writeln!(&mut account_string, "{:?}", data.hex_dump())?;
        }
    }

    Ok(account_string)
}

#[allow(clippy::too_many_arguments)]
fn process_transfer(
    rpc_client: &RpcClient,
    config: &CliConfig,
    amount: SpendAmount,
    to: &Pubkey,
    from: SignerIndex,
    sign_only: bool,
    dump_transaction_message: bool,
    allow_unfunded_recipient: bool,
    no_wait: bool,
    blockhash_query: &BlockhashQuery,
    nonce_account: Option<&Pubkey>,
    nonce_authority: SignerIndex,
    memo: Option<&String>,
    fee_payer: SignerIndex,
    derived_address_seed: Option<String>,
    derived_address_program_id: Option<&Pubkey>,
) -> ProcessResult {
    let from = config.signers[from];
    let mut from_pubkey = from.pubkey();

    let (recent_blockhash, fee_calculator) =
        blockhash_query.get_blockhash_and_fee_calculator(rpc_client, config.commitment)?;

    if !sign_only && !allow_unfunded_recipient {
        let recipient_balance = rpc_client
            .get_balance_with_commitment(to, config.commitment)?
            .value;
        if recipient_balance == 0 {
            return Err(format!(
                "The recipient address ({}) is not funded. \
                                Add `--allow-unfunded-recipient` to complete the transfer \
                               ",
                to
            )
            .into());
        }
    }

    let nonce_authority = config.signers[nonce_authority];
    let fee_payer = config.signers[fee_payer];

    let derived_parts = derived_address_seed.zip(derived_address_program_id);
    let with_seed = if let Some((seed, program_id)) = derived_parts {
        let base_pubkey = from_pubkey;
        from_pubkey = Pubkey::create_with_seed(&base_pubkey, &seed, program_id)?;
        Some((base_pubkey, seed, program_id, from_pubkey))
    } else {
        None
    };

    let build_message = |lamports| {
        let ixs = if let Some((base_pubkey, seed, program_id, from_pubkey)) = with_seed.as_ref() {
            vec![system_instruction::transfer_with_seed(
                from_pubkey,
                base_pubkey,
                seed.clone(),
                program_id,
                to,
                lamports,
            )]
            .with_memo(memo)
        } else {
            vec![system_instruction::transfer(&from_pubkey, to, lamports)].with_memo(memo)
        };

        if let Some(nonce_account) = &nonce_account {
            Message::new_with_nonce(
                ixs,
                Some(&fee_payer.pubkey()),
                nonce_account,
                &nonce_authority.pubkey(),
            )
        } else {
            Message::new(&ixs, Some(&fee_payer.pubkey()))
        }
    };

    let (message, _) = resolve_spend_tx_and_check_account_balances(
        rpc_client,
        sign_only,
        amount,
        &fee_calculator,
        &from_pubkey,
        &fee_payer.pubkey(),
        build_message,
        config.commitment,
    )?;
    let mut tx = Transaction::new_unsigned(message);

    if sign_only {
        tx.try_partial_sign(&config.signers, recent_blockhash)?;
        return_signers_with_config(
            &tx,
            &config.output_format,
            &ReturnSignersConfig {
                dump_transaction_message,
            },
        )
    } else {
        if let Some(nonce_account) = &nonce_account {
            let nonce_account = nonce_utils::get_account_with_commitment(
                rpc_client,
                nonce_account,
                config.commitment,
            )?;
            check_nonce_account(&nonce_account, &nonce_authority.pubkey(), &recent_blockhash)?;
        }

        tx.try_sign(&config.signers, recent_blockhash)?;
        let result = if no_wait {
            rpc_client.send_transaction(&tx)
        } else {
            rpc_client.send_and_confirm_transaction_with_spinner(&tx)
        };
        log_instruction_custom_error::<SystemError>(result, config)
    }
}

pub fn process_command(config: &CliConfig) -> ProcessResult {
    if config.verbose && config.output_format == OutputFormat::DisplayVerbose {
        println_name_value("RPC URL:", &config.json_rpc_url);
        println_name_value("Default Signer Path:", &config.keypair_path);
        if config.keypair_path.starts_with("usb://") {
            let pubkey = config
                .pubkey()
                .map(|pubkey| format!("{:?}", pubkey))
                .unwrap_or_else(|_| "Unavailable".to_string());
            println_name_value("Pubkey:", &pubkey);
        }
        println_name_value("Commitment:", &config.commitment.commitment.to_string());
    }

    let rpc_client = if config.rpc_client.is_none() {
        Arc::new(RpcClient::new_with_timeouts_and_commitment(
            config.json_rpc_url.to_string(),
            config.rpc_timeout,
            config.commitment,
            config.confirm_transaction_initial_timeout,
        ))
    } else {
        // Primarily for testing
        config.rpc_client.as_ref().unwrap().clone()
    };

    match &config.command {
        // Cluster Query Commands
        // Get address of this client
        CliCommand::Address => Ok(format!("{}", config.pubkey()?)),
        // Return software version of solana-cli and cluster entrypoint node
        CliCommand::Catchup {
            node_pubkey,
            node_json_rpc_url,
            follow,
            our_localhost_port,
            log,
        } => process_catchup(
            &rpc_client,
            config,
            *node_pubkey,
            node_json_rpc_url.clone(),
            *follow,
            *our_localhost_port,
            *log,
        ),
        CliCommand::ClusterDate => process_cluster_date(&rpc_client, config),
        CliCommand::ClusterVersion => process_cluster_version(&rpc_client, config),
        CliCommand::CreateAddressWithSeed {
            from_pubkey,
            seed,
            program_id,
        } => process_create_address_with_seed(config, from_pubkey.as_ref(), seed, program_id),
        CliCommand::Fees { ref blockhash } => process_fees(&rpc_client, config, blockhash.as_ref()),
        CliCommand::Feature(feature_subcommand) => {
            process_feature_subcommand(&rpc_client, config, feature_subcommand)
        }
        CliCommand::FirstAvailableBlock => process_first_available_block(&rpc_client),
        CliCommand::GetBlock { slot } => process_get_block(&rpc_client, config, *slot),
        CliCommand::GetBlockTime { slot } => process_get_block_time(&rpc_client, config, *slot),
        CliCommand::GetEpoch => process_get_epoch(&rpc_client, config),
        CliCommand::GetEpochInfo => process_get_epoch_info(&rpc_client, config),
        CliCommand::GetGenesisHash => process_get_genesis_hash(&rpc_client),
        CliCommand::GetSlot => process_get_slot(&rpc_client, config),
        CliCommand::GetBlockHeight => process_get_block_height(&rpc_client, config),
        CliCommand::LargestAccounts { filter } => {
            process_largest_accounts(&rpc_client, config, filter.clone())
        }
        CliCommand::GetTransactionCount => process_get_transaction_count(&rpc_client, config),
        CliCommand::Inflation(inflation_subcommand) => {
            process_inflation_subcommand(&rpc_client, config, inflation_subcommand)
        }
        CliCommand::LeaderSchedule { epoch } => {
            process_leader_schedule(&rpc_client, config, *epoch)
        }
        CliCommand::LiveSlots => process_live_slots(config),
        CliCommand::Logs { filter } => process_logs(config, filter),
        CliCommand::Ping {
            lamports,
            interval,
            count,
            timeout,
            blockhash,
            print_timestamp,
        } => process_ping(
            &rpc_client,
            config,
            *lamports,
            interval,
            count,
            timeout,
            blockhash,
            *print_timestamp,
        ),
        CliCommand::Rent {
            data_length,
            use_lamports_unit,
        } => process_calculate_rent(&rpc_client, config, *data_length, *use_lamports_unit),
        CliCommand::ShowBlockProduction { epoch, slot_limit } => {
            process_show_block_production(&rpc_client, config, *epoch, *slot_limit)
        }
        CliCommand::ShowGossip => process_show_gossip(&rpc_client, config),
        CliCommand::ShowStakes {
            use_lamports_unit,
            vote_account_pubkeys,
        } => process_show_stakes(
            &rpc_client,
            config,
            *use_lamports_unit,
            vote_account_pubkeys.as_deref(),
        ),
        CliCommand::WaitForMaxStake { max_stake_percent } => {
            process_wait_for_max_stake(&rpc_client, config, *max_stake_percent)
        }
        CliCommand::ShowValidators {
            use_lamports_unit,
            sort_order,
            reverse_sort,
            number_validators,
        } => process_show_validators(
            &rpc_client,
            config,
            *use_lamports_unit,
            *sort_order,
            *reverse_sort,
            *number_validators,
        ),
        CliCommand::Supply { print_accounts } => {
            process_supply(&rpc_client, config, *print_accounts)
        }
        CliCommand::TotalSupply => process_total_supply(&rpc_client, config),
        CliCommand::TransactionHistory {
            address,
            before,
            until,
            limit,
            show_transactions,
        } => process_transaction_history(
            &rpc_client,
            config,
            address,
            *before,
            *until,
            *limit,
            *show_transactions,
        ),

        // Nonce Commands

        // Assign authority to nonce account
        CliCommand::AuthorizeNonceAccount {
            nonce_account,
            nonce_authority,
            memo,
            new_authority,
        } => process_authorize_nonce_account(
            &rpc_client,
            config,
            nonce_account,
            *nonce_authority,
            memo.as_ref(),
            new_authority,
        ),
        // Create nonce account
        CliCommand::CreateNonceAccount {
            nonce_account,
            seed,
            nonce_authority,
            memo,
            amount,
        } => process_create_nonce_account(
            &rpc_client,
            config,
            *nonce_account,
            seed.clone(),
            *nonce_authority,
            memo.as_ref(),
            *amount,
        ),
        // Get the current nonce
        CliCommand::GetNonce(nonce_account_pubkey) => {
            process_get_nonce(&rpc_client, config, nonce_account_pubkey)
        }
        // Get a new nonce
        CliCommand::NewNonce {
            nonce_account,
            nonce_authority,
            memo,
        } => process_new_nonce(
            &rpc_client,
            config,
            nonce_account,
            *nonce_authority,
            memo.as_ref(),
        ),
        // Show the contents of a nonce account
        CliCommand::ShowNonceAccount {
            nonce_account_pubkey,
            use_lamports_unit,
        } => process_show_nonce_account(
            &rpc_client,
            config,
            nonce_account_pubkey,
            *use_lamports_unit,
        ),
        // Withdraw lamports from a nonce account
        CliCommand::WithdrawFromNonceAccount {
            nonce_account,
            nonce_authority,
            memo,
            destination_account_pubkey,
            lamports,
        } => process_withdraw_from_nonce_account(
            &rpc_client,
            config,
            nonce_account,
            *nonce_authority,
            memo.as_ref(),
            destination_account_pubkey,
            *lamports,
        ),

        // Program Deployment

        // Deploy a custom program to the chain
        CliCommand::Deploy {
            program_location,
            address,
            use_deprecated_loader,
            allow_excessive_balance,
        } => process_deploy(
            rpc_client,
            config,
            program_location,
            *address,
            *use_deprecated_loader,
            *allow_excessive_balance,
        ),
        CliCommand::Program(program_subcommand) => {
            process_program_subcommand(rpc_client, config, program_subcommand)
        }

        // Stake Commands

        // Create stake account
        CliCommand::CreateStakeAccount {
            stake_account,
            seed,
            staker,
            withdrawer,
            lockup,
            amount,
            sign_only,
            dump_transaction_message,
            blockhash_query,
            ref nonce_account,
            nonce_authority,
            memo,
            fee_payer,
            from,
        } => process_create_stake_account(
            &rpc_client,
            config,
            *stake_account,
            seed,
            staker,
            withdrawer,
            lockup,
            *amount,
            *sign_only,
            *dump_transaction_message,
            blockhash_query,
            nonce_account.as_ref(),
            *nonce_authority,
            memo.as_ref(),
            *fee_payer,
            *from,
        ),
        CliCommand::DeactivateStake {
            stake_account_pubkey,
            stake_authority,
            sign_only,
            dump_transaction_message,
            blockhash_query,
            nonce_account,
            nonce_authority,
            memo,
            seed,
            fee_payer,
        } => process_deactivate_stake_account(
            &rpc_client,
            config,
            stake_account_pubkey,
            *stake_authority,
            *sign_only,
            *dump_transaction_message,
            blockhash_query,
            *nonce_account,
            *nonce_authority,
            memo.as_ref(),
            seed.as_ref(),
            *fee_payer,
        ),
        CliCommand::DelegateStake {
            stake_account_pubkey,
            vote_account_pubkey,
            stake_authority,
            force,
            sign_only,
            dump_transaction_message,
            blockhash_query,
            nonce_account,
            nonce_authority,
            memo,
            fee_payer,
        } => process_delegate_stake(
            &rpc_client,
            config,
            stake_account_pubkey,
            vote_account_pubkey,
            *stake_authority,
            *force,
            *sign_only,
            *dump_transaction_message,
            blockhash_query,
            *nonce_account,
            *nonce_authority,
            memo.as_ref(),
            *fee_payer,
        ),
        CliCommand::SplitStake {
            stake_account_pubkey,
            stake_authority,
            sign_only,
            dump_transaction_message,
            blockhash_query,
            nonce_account,
            nonce_authority,
            memo,
            split_stake_account,
            seed,
            lamports,
            fee_payer,
        } => process_split_stake(
            &rpc_client,
            config,
            stake_account_pubkey,
            *stake_authority,
            *sign_only,
            *dump_transaction_message,
            blockhash_query,
            *nonce_account,
            *nonce_authority,
            memo.as_ref(),
            *split_stake_account,
            seed,
            *lamports,
            *fee_payer,
        ),
        CliCommand::MergeStake {
            stake_account_pubkey,
            source_stake_account_pubkey,
            stake_authority,
            sign_only,
            dump_transaction_message,
            blockhash_query,
            nonce_account,
            nonce_authority,
            memo,
            fee_payer,
        } => process_merge_stake(
            &rpc_client,
            config,
            stake_account_pubkey,
            source_stake_account_pubkey,
            *stake_authority,
            *sign_only,
            *dump_transaction_message,
            blockhash_query,
            *nonce_account,
            *nonce_authority,
            memo.as_ref(),
            *fee_payer,
        ),
        CliCommand::ShowStakeAccount {
            pubkey: stake_account_pubkey,
            use_lamports_unit,
            with_rewards,
        } => process_show_stake_account(
            &rpc_client,
            config,
            stake_account_pubkey,
            *use_lamports_unit,
            *with_rewards,
        ),
        CliCommand::ShowStakeHistory {
            use_lamports_unit,
            limit_results,
        } => process_show_stake_history(&rpc_client, config, *use_lamports_unit, *limit_results),
        CliCommand::StakeAuthorize {
            stake_account_pubkey,
            ref new_authorizations,
            sign_only,
            dump_transaction_message,
            blockhash_query,
            nonce_account,
            nonce_authority,
            memo,
            fee_payer,
            custodian,
            no_wait,
        } => process_stake_authorize(
            &rpc_client,
            config,
            stake_account_pubkey,
            new_authorizations,
            *custodian,
            *sign_only,
            *dump_transaction_message,
            blockhash_query,
            *nonce_account,
            *nonce_authority,
            memo.as_ref(),
            *fee_payer,
            *no_wait,
        ),
        CliCommand::StakeSetLockup {
            stake_account_pubkey,
            mut lockup,
            custodian,
            sign_only,
            dump_transaction_message,
            blockhash_query,
            nonce_account,
            nonce_authority,
            memo,
            fee_payer,
        } => process_stake_set_lockup(
            &rpc_client,
            config,
            stake_account_pubkey,
            &mut lockup,
            *custodian,
            *sign_only,
            *dump_transaction_message,
            blockhash_query,
            *nonce_account,
            *nonce_authority,
            memo.as_ref(),
            *fee_payer,
        ),
        CliCommand::WithdrawStake {
            stake_account_pubkey,
            destination_account_pubkey,
            amount,
            withdraw_authority,
            custodian,
            sign_only,
            dump_transaction_message,
            blockhash_query,
            ref nonce_account,
            nonce_authority,
            memo,
            seed,
            fee_payer,
        } => process_withdraw_stake(
            &rpc_client,
            config,
            stake_account_pubkey,
            destination_account_pubkey,
            *amount,
            *withdraw_authority,
            *custodian,
            *sign_only,
            *dump_transaction_message,
            blockhash_query,
            nonce_account.as_ref(),
            *nonce_authority,
            memo.as_ref(),
            seed.as_ref(),
            *fee_payer,
        ),

        // Validator Info Commands

        // Return all or single validator info
        CliCommand::GetValidatorInfo(info_pubkey) => {
            process_get_validator_info(&rpc_client, config, *info_pubkey)
        }
        // Publish validator info
        CliCommand::SetValidatorInfo {
            validator_info,
            force_keybase,
            info_pubkey,
        } => process_set_validator_info(
            &rpc_client,
            config,
            validator_info,
            *force_keybase,
            *info_pubkey,
        ),

        // Vote Commands

        // Create vote account
        CliCommand::CreateVoteAccount {
            vote_account,
            seed,
            identity_account,
            authorized_voter,
            authorized_withdrawer,
            commission,
            memo,
        } => process_create_vote_account(
            &rpc_client,
            config,
            *vote_account,
            seed,
            *identity_account,
            authorized_voter,
            authorized_withdrawer,
            *commission,
            memo.as_ref(),
        ),
        CliCommand::ShowVoteAccount {
            pubkey: vote_account_pubkey,
            use_lamports_unit,
            with_rewards,
        } => process_show_vote_account(
            &rpc_client,
            config,
            vote_account_pubkey,
            *use_lamports_unit,
            *with_rewards,
        ),
        CliCommand::WithdrawFromVoteAccount {
            vote_account_pubkey,
            withdraw_authority,
            withdraw_amount,
            destination_account_pubkey,
            memo,
        } => process_withdraw_from_vote_account(
            &rpc_client,
            config,
            vote_account_pubkey,
            *withdraw_authority,
            *withdraw_amount,
            destination_account_pubkey,
            memo.as_ref(),
        ),
        CliCommand::VoteAuthorize {
            vote_account_pubkey,
            new_authorized_pubkey,
            vote_authorize,
            memo,
            authorized,
            new_authorized,
        } => process_vote_authorize(
            &rpc_client,
            config,
            vote_account_pubkey,
            new_authorized_pubkey,
            *vote_authorize,
            *authorized,
            *new_authorized,
            memo.as_ref(),
        ),
        CliCommand::VoteUpdateValidator {
            vote_account_pubkey,
            new_identity_account,
            withdraw_authority,
            memo,
        } => process_vote_update_validator(
            &rpc_client,
            config,
            vote_account_pubkey,
            *new_identity_account,
            *withdraw_authority,
            memo.as_ref(),
        ),
        CliCommand::VoteUpdateCommission {
            vote_account_pubkey,
            commission,
            withdraw_authority,
            memo,
        } => process_vote_update_commission(
            &rpc_client,
            config,
            vote_account_pubkey,
            *commission,
            *withdraw_authority,
            memo.as_ref(),
        ),

        // Wallet Commands

        // Request an airdrop from Solana Faucet;
        CliCommand::Airdrop { pubkey, lamports } => {
            process_airdrop(&rpc_client, config, pubkey, *lamports)
        }
        // Check client balance
        CliCommand::Balance {
            pubkey,
            use_lamports_unit,
        } => process_balance(&rpc_client, config, pubkey, *use_lamports_unit),
        // Confirm the last client transaction by signature
        CliCommand::Confirm(signature) => process_confirm(&rpc_client, config, signature),
        CliCommand::DecodeTransaction(transaction) => {
            process_decode_transaction(config, transaction)
        }
        CliCommand::ResolveSigner(path) => {
            if let Some(path) = path {
                Ok(path.to_string())
            } else {
                Ok("Signer is valid".to_string())
            }
        }
        CliCommand::ShowAccount {
            pubkey,
            output_file,
            use_lamports_unit,
        } => process_show_account(&rpc_client, config, pubkey, output_file, *use_lamports_unit),
        CliCommand::Transfer {
            amount,
            to,
            from,
            sign_only,
            dump_transaction_message,
            allow_unfunded_recipient,
            no_wait,
            ref blockhash_query,
            ref nonce_account,
            nonce_authority,
            memo,
            fee_payer,
            derived_address_seed,
            ref derived_address_program_id,
        } => process_transfer(
            &rpc_client,
            config,
            *amount,
            to,
            *from,
            *sign_only,
            *dump_transaction_message,
            *allow_unfunded_recipient,
            *no_wait,
            blockhash_query,
            nonce_account.as_ref(),
            *nonce_authority,
            memo.as_ref(),
            *fee_payer,
            derived_address_seed.clone(),
            derived_address_program_id.as_ref(),
        ),
    }
}

pub fn request_and_confirm_airdrop(
    rpc_client: &RpcClient,
    config: &CliConfig,
    to_pubkey: &Pubkey,
    lamports: u64,
) -> ClientResult<Signature> {
    let (recent_blockhash, _fee_calculator) = rpc_client.get_recent_blockhash()?;
    let signature =
        rpc_client.request_airdrop_with_blockhash(to_pubkey, lamports, &recent_blockhash)?;
    rpc_client.confirm_transaction_with_spinner(
        &signature,
        &recent_blockhash,
        config.commitment,
    )?;
    Ok(signature)
}

pub fn log_instruction_custom_error<E>(
    result: ClientResult<Signature>,
    config: &CliConfig,
) -> ProcessResult
where
    E: 'static + std::error::Error + DecodeError<E> + FromPrimitive,
{
    match result {
        Err(err) => {
            // If transaction simulation returns a known Custom InstructionError, decode it
            if let ClientErrorKind::RpcError(RpcError::RpcResponseError {
                data:
                    RpcResponseErrorData::SendTransactionPreflightFailure(
                        RpcSimulateTransactionResult {
                            err:
                                Some(TransactionError::InstructionError(
                                    _,
                                    InstructionError::Custom(code),
                                )),
                            ..
                        },
                    ),
                ..
            }) = err.kind()
            {
                if let Some(specific_error) = E::decode_custom_error_to_enum(*code) {
                    return Err(specific_error.into());
                }
            }
            // If the transaction was instead submitted and returned a known Custom
            // InstructionError, decode it
            if let ClientErrorKind::TransactionError(TransactionError::InstructionError(
                _,
                InstructionError::Custom(code),
            )) = err.kind()
            {
                if let Some(specific_error) = E::decode_custom_error_to_enum(*code) {
                    return Err(specific_error.into());
                }
            }
            Err(err.into())
        }
        Ok(sig) => {
            let signature = CliSignature {
                signature: sig.clone().to_string(),
            };
            Ok(config.output_format.formatted_string(&signature))
        }
    }
}

pub fn app<'ab, 'v>(name: &str, about: &'ab str, version: &'v str) -> App<'ab, 'v> {
    App::new(name)
        .about(about)
        .version(version)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("address")
                .about("Get your public key")
                .arg(
                    Arg::with_name("confirm_key")
                        .long("confirm-key")
                        .takes_value(false)
                        .help("Confirm key on device; only relevant if using remote wallet"),
                ),
        )
        .cluster_query_subcommands()
        .feature_subcommands()
        .inflation_subcommands()
        .nonce_subcommands()
        .program_subcommands()
        .stake_subcommands()
        .subcommand(
            SubCommand::with_name("airdrop")
                .about("Request lamports")
                .arg(
                    Arg::with_name("faucet_host")
                        .long("faucet-host")
                        .value_name("URL")
                        .takes_value(true)
                        .help("Faucet host to use [default: the --url host]"),
                )
                .arg(
                    Arg::with_name("faucet_port")
                        .long("faucet-port")
                        .value_name("PORT_NUMBER")
                        .takes_value(true)
                        .default_value(solana_faucet::faucet::FAUCET_PORT_STR)
                        .help("Faucet port to use"),
                )
                .arg(
                    Arg::with_name("amount")
                        .index(1)
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_amount)
                        .required(true)
                        .help("The airdrop amount to request, in SOL"),
                )
                .arg(
                    pubkey!(Arg::with_name("to")
                        .index(2)
                        .value_name("RECIPIENT_ADDRESS"),
                        "The account address of airdrop recipient. "),
                ),
        )
        .subcommand(
            SubCommand::with_name("balance")
                .about("Get your balance")
                .arg(
                    pubkey!(Arg::with_name("pubkey")
                        .index(1)
                        .value_name("ACCOUNT_ADDRESS"),
                        "The account address of the balance to check. ")
                )
                .arg(
                    Arg::with_name("lamports")
                        .long("lamports")
                        .takes_value(false)
                        .help("Display balance in lamports instead of SOL"),
                ),
        )
        .subcommand(
            SubCommand::with_name("confirm")
                .about("Confirm transaction by signature")
                .arg(
                    Arg::with_name("signature")
                        .index(1)
                        .value_name("TRANSACTION_SIGNATURE")
                        .takes_value(true)
                        .required(true)
                        .help("The transaction signature to confirm"),
                )
                .after_help(// Formatted specifically for the manually-indented heredoc string
                   "Note: This will show more detailed information for finalized transactions with verbose mode (-v/--verbose).\
                  \n\
                  \nAccount modes:\
                  \n  |srwx|\
                  \n    s: signed\
                  \n    r: readable (always true)\
                  \n    w: writable\
                  \n    x: program account (inner instructions excluded)\
                   "
                ),
        )
        .subcommand(
            SubCommand::with_name("decode-transaction")
                .about("Decode a serialized transaction")
                .arg(
                    Arg::with_name("transaction")
                        .index(1)
                        .value_name("TRANSACTION")
                        .takes_value(true)
                        .required(true)
                        .help("transaction to decode"),
                )
                .arg(
                    Arg::with_name("encoding")
                        .index(2)
                        .value_name("ENCODING")
                        .possible_values(&["base58", "base64"]) // Subset of `UiTransactionEncoding` enum
                        .default_value("base58")
                        .takes_value(true)
                        .required(true)
                        .help("transaction encoding"),
                ),
        )
        .subcommand(
            SubCommand::with_name("create-address-with-seed")
                .about("Generate a derived account address with a seed")
                .arg(
                    Arg::with_name("seed")
                        .index(1)
                        .value_name("SEED_STRING")
                        .takes_value(true)
                        .required(true)
                        .validator(is_derived_address_seed)
                        .help("The seed.  Must not take more than 32 bytes to encode as utf-8"),
                )
                .arg(
                    Arg::with_name("program_id")
                        .index(2)
                        .value_name("PROGRAM_ID")
                        .takes_value(true)
                        .required(true)
                        .help(
                            "The program_id that the address will ultimately be used for, \n\
                             or one of NONCE, STAKE, and VOTE keywords",
                        ),
                )
                .arg(
                    pubkey!(Arg::with_name("from")
                        .long("from")
                        .value_name("FROM_PUBKEY")
                        .required(false),
                        "From (base) key, [default: cli config keypair]. "),
                ),
        )
        .subcommand(
            SubCommand::with_name("deploy")
                .about("Deploy a program")
                .arg(
                    Arg::with_name("program_location")
                        .index(1)
                        .value_name("PROGRAM_FILEPATH")
                        .takes_value(true)
                        .required(true)
                        .help("/path/to/program.o"),
                )
                .arg(
                    Arg::with_name("address_signer")
                        .index(2)
                        .value_name("PROGRAM_ADDRESS_SIGNER")
                        .takes_value(true)
                        .validator(is_valid_signer)
                        .help("The signer for the desired address of the program [default: new random address]")
                )
                .arg(
                    Arg::with_name("use_deprecated_loader")
                        .long("use-deprecated-loader")
                        .takes_value(false)
                        .hidden(true) // Don't document this argument to discourage its use
                        .help("Use the deprecated BPF loader")
                )
                .arg(
                    Arg::with_name("allow_excessive_balance")
                        .long("allow-excessive-deploy-account-balance")
                        .takes_value(false)
                        .help("Use the designated program id, even if the account already holds a large balance of SOL")
                ),
        )
        .subcommand(
            SubCommand::with_name("resolve-signer")
                .about("Checks that a signer is valid, and returns its specific path; useful for signers that may be specified generally, eg. usb://ledger")
                .arg(
                    Arg::with_name("signer")
                        .index(1)
                        .value_name("SIGNER_KEYPAIR")
                        .takes_value(true)
                        .required(true)
                        .validator(is_valid_signer)
                        .help("The signer path to resolve")
                )
        )
        .subcommand(
            SubCommand::with_name("transfer")
                .about("Transfer funds between system accounts")
                .alias("pay")
                .arg(
                    pubkey!(Arg::with_name("to")
                        .index(1)
                        .value_name("RECIPIENT_ADDRESS")
                        .required(true),
                        "The account address of recipient. "),
                )
                .arg(
                    Arg::with_name("amount")
                        .index(2)
                        .value_name("AMOUNT")
                        .takes_value(true)
                        .validator(is_amount_or_all)
                        .required(true)
                        .help("The amount to send, in SOL; accepts keyword ALL"),
                )
                .arg(
                    pubkey!(Arg::with_name("from")
                        .long("from")
                        .value_name("FROM_ADDRESS"),
                        "Source account of funds (if different from client local account). "),
                )
                .arg(
                    Arg::with_name("no_wait")
                        .long("no-wait")
                        .takes_value(false)
                        .help("Return signature immediately after submitting the transaction, instead of waiting for confirmations"),
                )
                .arg(
                    Arg::with_name("derived_address_seed")
                        .long("derived-address-seed")
                        .takes_value(true)
                        .value_name("SEED_STRING")
                        .requires("derived_address_program_id")
                        .validator(is_derived_address_seed)
                        .hidden(true)
                )
                .arg(
                    Arg::with_name("derived_address_program_id")
                        .long("derived-address-program-id")
                        .takes_value(true)
                        .value_name("PROGRAM_ID")
                        .requires("derived_address_seed")
                        .hidden(true)
                )
                .arg(
                    Arg::with_name("allow_unfunded_recipient")
                        .long("allow-unfunded-recipient")
                        .takes_value(false)
                        .help("Complete the transfer even if the recipient address is not funded")
                )
                .offline_args()
                .nonce_args(false)
                .arg(memo_arg())
                .arg(fee_payer_arg()),
        )
        .subcommand(
            SubCommand::with_name("account")
                .about("Show the contents of an account")
                .alias("account")
                .arg(
                    pubkey!(Arg::with_name("account_pubkey")
                        .index(1)
                        .value_name("ACCOUNT_ADDRESS")
                        .required(true),
                        "Account key URI. ")
                )
                .arg(
                    Arg::with_name("output_file")
                        .long("output-file")
                        .short("o")
                        .value_name("FILEPATH")
                        .takes_value(true)
                        .help("Write the account data to this file"),
                )
                .arg(
                    Arg::with_name("lamports")
                        .long("lamports")
                        .takes_value(false)
                        .help("Display balance in lamports instead of SOL"),
                ),
        )
        .validator_info_subcommands()
        .vote_subcommands()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use solana_client::{
        blockhash_query,
        mock_sender::SIGNATURE,
        rpc_request::RpcRequest,
        rpc_response::{Response, RpcResponseContext},
    };
    use solana_sdk::{
        pubkey::Pubkey,
        signature::{keypair_from_seed, read_keypair_file, write_keypair_file, Keypair, Presigner},
        transaction::TransactionError,
    };
    use solana_transaction_status::TransactionConfirmationStatus;
    use std::path::PathBuf;

    fn make_tmp_path(name: &str) -> String {
        let out_dir = std::env::var("FARF_DIR").unwrap_or_else(|_| "farf".to_string());
        let keypair = Keypair::new();

        let path = format!("{}/tmp/{}-{}", out_dir, name, keypair.pubkey());

        // whack any possible collision
        let _ignored = std::fs::remove_dir_all(&path);
        // whack any possible collision
        let _ignored = std::fs::remove_file(&path);

        path
    }

    #[test]
    fn test_generate_unique_signers() {
        let matches = ArgMatches::default();

        let default_keypair = Keypair::new();
        let default_keypair_file = make_tmp_path("keypair_file");
        write_keypair_file(&default_keypair, &default_keypair_file).unwrap();

        let default_signer = DefaultSigner::new("keypair", &default_keypair_file);

        let signer_info = default_signer
            .generate_unique_signers(vec![], &matches, &mut None)
            .unwrap();
        assert_eq!(signer_info.signers.len(), 0);

        let signer_info = default_signer
            .generate_unique_signers(vec![None, None], &matches, &mut None)
            .unwrap();
        assert_eq!(signer_info.signers.len(), 1);
        assert_eq!(signer_info.index_of(None), Some(0));
        assert_eq!(
            signer_info.index_of(Some(solana_sdk::pubkey::new_rand())),
            None
        );

        let keypair0 = keypair_from_seed(&[1u8; 32]).unwrap();
        let keypair0_pubkey = keypair0.pubkey();
        let keypair0_clone = keypair_from_seed(&[1u8; 32]).unwrap();
        let keypair0_clone_pubkey = keypair0.pubkey();
        let signers = vec![None, Some(keypair0.into()), Some(keypair0_clone.into())];
        let signer_info = default_signer
            .generate_unique_signers(signers, &matches, &mut None)
            .unwrap();
        assert_eq!(signer_info.signers.len(), 2);
        assert_eq!(signer_info.index_of(None), Some(0));
        assert_eq!(signer_info.index_of(Some(keypair0_pubkey)), Some(1));
        assert_eq!(signer_info.index_of(Some(keypair0_clone_pubkey)), Some(1));

        let keypair0 = keypair_from_seed(&[1u8; 32]).unwrap();
        let keypair0_pubkey = keypair0.pubkey();
        let keypair0_clone = keypair_from_seed(&[1u8; 32]).unwrap();
        let signers = vec![Some(keypair0.into()), Some(keypair0_clone.into())];
        let signer_info = default_signer
            .generate_unique_signers(signers, &matches, &mut None)
            .unwrap();
        assert_eq!(signer_info.signers.len(), 1);
        assert_eq!(signer_info.index_of(Some(keypair0_pubkey)), Some(0));

        // Signers with the same pubkey are not distinct
        let keypair0 = keypair_from_seed(&[2u8; 32]).unwrap();
        let keypair0_pubkey = keypair0.pubkey();
        let keypair1 = keypair_from_seed(&[3u8; 32]).unwrap();
        let keypair1_pubkey = keypair1.pubkey();
        let message = vec![0, 1, 2, 3];
        let presigner0 = Presigner::new(&keypair0.pubkey(), &keypair0.sign_message(&message));
        let presigner0_pubkey = presigner0.pubkey();
        let presigner1 = Presigner::new(&keypair1.pubkey(), &keypair1.sign_message(&message));
        let presigner1_pubkey = presigner1.pubkey();
        let signers = vec![
            Some(keypair0.into()),
            Some(presigner0.into()),
            Some(presigner1.into()),
            Some(keypair1.into()),
        ];
        let signer_info = default_signer
            .generate_unique_signers(signers, &matches, &mut None)
            .unwrap();
        assert_eq!(signer_info.signers.len(), 2);
        assert_eq!(signer_info.index_of(Some(keypair0_pubkey)), Some(0));
        assert_eq!(signer_info.index_of(Some(keypair1_pubkey)), Some(1));
        assert_eq!(signer_info.index_of(Some(presigner0_pubkey)), Some(0));
        assert_eq!(signer_info.index_of(Some(presigner1_pubkey)), Some(1));
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_cli_parse_command() {
        let test_commands = app("test", "desc", "version");

        let pubkey = solana_sdk::pubkey::new_rand();
        let pubkey_string = format!("{}", pubkey);

        let default_keypair = Keypair::new();
        let keypair_file = make_tmp_path("keypair_file");
        write_keypair_file(&default_keypair, &keypair_file).unwrap();
        let keypair = read_keypair_file(&keypair_file).unwrap();
        let default_signer = DefaultSigner::new("", &keypair_file);
        // Test Airdrop Subcommand
        let test_airdrop =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "airdrop", "50", &pubkey_string]);
        assert_eq!(
            parse_command(&test_airdrop, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Airdrop {
                    pubkey: Some(pubkey),
                    lamports: 50_000_000_000,
                },
                signers: vec![],
            }
        );

        // Test Balance Subcommand, incl pubkey and keypair-file inputs
        let test_balance = test_commands.clone().get_matches_from(vec![
            "test",
            "balance",
            &keypair.pubkey().to_string(),
        ]);
        assert_eq!(
            parse_command(&test_balance, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Balance {
                    pubkey: Some(keypair.pubkey()),
                    use_lamports_unit: false,
                },
                signers: vec![],
            }
        );
        let test_balance = test_commands.clone().get_matches_from(vec![
            "test",
            "balance",
            &keypair_file,
            "--lamports",
        ]);
        assert_eq!(
            parse_command(&test_balance, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Balance {
                    pubkey: Some(keypair.pubkey()),
                    use_lamports_unit: true,
                },
                signers: vec![],
            }
        );
        let test_balance =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "balance", "--lamports"]);
        assert_eq!(
            parse_command(&test_balance, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Balance {
                    pubkey: None,
                    use_lamports_unit: true,
                },
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        // Test Confirm Subcommand
        let signature = Signature::new(&[1; 64]);
        let signature_string = format!("{:?}", signature);
        let test_confirm =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "confirm", &signature_string]);
        assert_eq!(
            parse_command(&test_confirm, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Confirm(signature),
                signers: vec![],
            }
        );
        let test_bad_signature = test_commands
            .clone()
            .get_matches_from(vec!["test", "confirm", "deadbeef"]);
        assert!(parse_command(&test_bad_signature, &default_signer, &mut None).is_err());

        // Test CreateAddressWithSeed
        let from_pubkey = Some(solana_sdk::pubkey::new_rand());
        let from_str = from_pubkey.unwrap().to_string();
        for (name, program_id) in &[
            ("STAKE", stake::program::id()),
            ("VOTE", solana_vote_program::id()),
            ("NONCE", system_program::id()),
        ] {
            let test_create_address_with_seed = test_commands.clone().get_matches_from(vec![
                "test",
                "create-address-with-seed",
                "seed",
                name,
                "--from",
                &from_str,
            ]);
            assert_eq!(
                parse_command(&test_create_address_with_seed, &default_signer, &mut None).unwrap(),
                CliCommandInfo {
                    command: CliCommand::CreateAddressWithSeed {
                        from_pubkey,
                        seed: "seed".to_string(),
                        program_id: *program_id
                    },
                    signers: vec![],
                }
            );
        }
        let test_create_address_with_seed = test_commands.clone().get_matches_from(vec![
            "test",
            "create-address-with-seed",
            "seed",
            "STAKE",
        ]);
        assert_eq!(
            parse_command(&test_create_address_with_seed, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::CreateAddressWithSeed {
                    from_pubkey: None,
                    seed: "seed".to_string(),
                    program_id: stake::program::id(),
                },
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        // Test Deploy Subcommand
        let test_command =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "deploy", "/Users/test/program.o"]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Deploy {
                    program_location: "/Users/test/program.o".to_string(),
                    address: None,
                    use_deprecated_loader: false,
                    allow_excessive_balance: false,
                },
                signers: vec![read_keypair_file(&keypair_file).unwrap().into()],
            }
        );

        let custom_address = Keypair::new();
        let custom_address_file = make_tmp_path("custom_address_file");
        write_keypair_file(&custom_address, &custom_address_file).unwrap();
        let test_command = test_commands.clone().get_matches_from(vec![
            "test",
            "deploy",
            "/Users/test/program.o",
            &custom_address_file,
        ]);
        assert_eq!(
            parse_command(&test_command, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Deploy {
                    program_location: "/Users/test/program.o".to_string(),
                    address: Some(1),
                    use_deprecated_loader: false,
                    allow_excessive_balance: false,
                },
                signers: vec![
                    read_keypair_file(&keypair_file).unwrap().into(),
                    read_keypair_file(&custom_address_file).unwrap().into(),
                ],
            }
        );

        // Test ResolveSigner Subcommand, SignerSource::Filepath
        let test_resolve_signer =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "resolve-signer", &keypair_file]);
        assert_eq!(
            parse_command(&test_resolve_signer, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::ResolveSigner(Some(keypair_file.clone())),
                signers: vec![],
            }
        );
        // Test ResolveSigner Subcommand, SignerSource::Pubkey (Presigner)
        let test_resolve_signer =
            test_commands
                .clone()
                .get_matches_from(vec!["test", "resolve-signer", &pubkey_string]);
        assert_eq!(
            parse_command(&test_resolve_signer, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::ResolveSigner(Some(pubkey.to_string())),
                signers: vec![],
            }
        );
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_cli_process_command() {
        // Success cases
        let mut config = CliConfig {
            rpc_client: Some(Arc::new(RpcClient::new_mock("succeeds".to_string()))),
            json_rpc_url: "http://127.0.0.1:8899".to_string(),
            ..CliConfig::default()
        };

        let keypair = Keypair::new();
        let pubkey = keypair.pubkey().to_string();
        config.signers = vec![&keypair];
        config.command = CliCommand::Address;
        assert_eq!(process_command(&config).unwrap(), pubkey);

        config.command = CliCommand::Balance {
            pubkey: None,
            use_lamports_unit: true,
        };
        assert_eq!(process_command(&config).unwrap(), "50 lamports");

        config.command = CliCommand::Balance {
            pubkey: None,
            use_lamports_unit: false,
        };
        assert_eq!(process_command(&config).unwrap(), "0.00000005 SOL");

        let good_signature = Signature::new(&bs58::decode(SIGNATURE).into_vec().unwrap());
        config.command = CliCommand::Confirm(good_signature);
        assert_eq!(
            process_command(&config).unwrap(),
            format!("{:?}", TransactionConfirmationStatus::Finalized)
        );

        let bob_keypair = Keypair::new();
        let bob_pubkey = bob_keypair.pubkey();
        let identity_keypair = Keypair::new();
        config.command = CliCommand::CreateVoteAccount {
            vote_account: 1,
            seed: None,
            identity_account: 2,
            authorized_voter: Some(bob_pubkey),
            authorized_withdrawer: Some(bob_pubkey),
            commission: 0,
            memo: None,
        };
        config.signers = vec![&keypair, &bob_keypair, &identity_keypair];
        let result = process_command(&config);
        assert!(result.is_ok());

        let new_authorized_pubkey = solana_sdk::pubkey::new_rand();
        config.signers = vec![&bob_keypair];
        config.command = CliCommand::VoteAuthorize {
            vote_account_pubkey: bob_pubkey,
            new_authorized_pubkey,
            vote_authorize: VoteAuthorize::Voter,
            memo: None,
            authorized: 0,
            new_authorized: None,
        };
        let result = process_command(&config);
        assert!(result.is_ok());

        let new_identity_keypair = Keypair::new();
        config.signers = vec![&keypair, &bob_keypair, &new_identity_keypair];
        config.command = CliCommand::VoteUpdateValidator {
            vote_account_pubkey: bob_pubkey,
            new_identity_account: 2,
            withdraw_authority: 1,
            memo: None,
        };
        let result = process_command(&config);
        assert!(result.is_ok());

        let bob_keypair = Keypair::new();
        let bob_pubkey = bob_keypair.pubkey();
        let custodian = solana_sdk::pubkey::new_rand();
        config.command = CliCommand::CreateStakeAccount {
            stake_account: 1,
            seed: None,
            staker: None,
            withdrawer: None,
            lockup: Lockup {
                epoch: 0,
                unix_timestamp: 0,
                custodian,
            },
            amount: SpendAmount::Some(30),
            sign_only: false,
            dump_transaction_message: false,
            blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
            nonce_account: None,
            nonce_authority: 0,
            memo: None,
            fee_payer: 0,
            from: 0,
        };
        config.signers = vec![&keypair, &bob_keypair];
        let result = process_command(&config);
        assert!(result.is_ok());

        let stake_account_pubkey = solana_sdk::pubkey::new_rand();
        let to_pubkey = solana_sdk::pubkey::new_rand();
        config.command = CliCommand::WithdrawStake {
            stake_account_pubkey,
            destination_account_pubkey: to_pubkey,
            amount: SpendAmount::All,
            withdraw_authority: 0,
            custodian: None,
            sign_only: false,
            dump_transaction_message: false,
            blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
            nonce_account: None,
            nonce_authority: 0,
            memo: None,
            seed: None,
            fee_payer: 0,
        };
        config.signers = vec![&keypair];
        let result = process_command(&config);
        assert!(result.is_ok());

        let stake_account_pubkey = solana_sdk::pubkey::new_rand();
        config.command = CliCommand::DeactivateStake {
            stake_account_pubkey,
            stake_authority: 0,
            sign_only: false,
            dump_transaction_message: false,
            blockhash_query: BlockhashQuery::default(),
            nonce_account: None,
            nonce_authority: 0,
            memo: None,
            seed: None,
            fee_payer: 0,
        };
        let result = process_command(&config);
        assert!(result.is_ok());

        let stake_account_pubkey = solana_sdk::pubkey::new_rand();
        let split_stake_account = Keypair::new();
        config.command = CliCommand::SplitStake {
            stake_account_pubkey,
            stake_authority: 0,
            sign_only: false,
            dump_transaction_message: false,
            blockhash_query: BlockhashQuery::default(),
            nonce_account: None,
            nonce_authority: 0,
            memo: None,
            split_stake_account: 1,
            seed: None,
            lamports: 30,
            fee_payer: 0,
        };
        config.signers = vec![&keypair, &split_stake_account];
        let result = process_command(&config);
        assert!(result.is_ok());

        let stake_account_pubkey = solana_sdk::pubkey::new_rand();
        let source_stake_account_pubkey = solana_sdk::pubkey::new_rand();
        let merge_stake_account = Keypair::new();
        config.command = CliCommand::MergeStake {
            stake_account_pubkey,
            source_stake_account_pubkey,
            stake_authority: 1,
            sign_only: false,
            dump_transaction_message: false,
            blockhash_query: BlockhashQuery::default(),
            nonce_account: None,
            nonce_authority: 0,
            memo: None,
            fee_payer: 0,
        };
        config.signers = vec![&keypair, &merge_stake_account];
        let result = process_command(&config);
        assert!(result.is_ok());

        config.command = CliCommand::GetSlot;
        assert_eq!(process_command(&config).unwrap(), "0");

        config.command = CliCommand::GetTransactionCount;
        assert_eq!(process_command(&config).unwrap(), "1234");

        // CreateAddressWithSeed
        let from_pubkey = solana_sdk::pubkey::new_rand();
        config.signers = vec![];
        config.command = CliCommand::CreateAddressWithSeed {
            from_pubkey: Some(from_pubkey),
            seed: "seed".to_string(),
            program_id: stake::program::id(),
        };
        let address = process_command(&config);
        let expected_address =
            Pubkey::create_with_seed(&from_pubkey, "seed", &stake::program::id()).unwrap();
        assert_eq!(address.unwrap(), expected_address.to_string());

        // Need airdrop cases
        let to = solana_sdk::pubkey::new_rand();
        config.signers = vec![&keypair];
        config.command = CliCommand::Airdrop {
            pubkey: Some(to),
            lamports: 50,
        };
        assert!(process_command(&config).is_ok());

        // sig_not_found case
        config.rpc_client = Some(Arc::new(RpcClient::new_mock("sig_not_found".to_string())));
        let missing_signature = Signature::new(&bs58::decode("5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW").into_vec().unwrap());
        config.command = CliCommand::Confirm(missing_signature);
        assert_eq!(process_command(&config).unwrap(), "Not found");

        // Tx error case
        config.rpc_client = Some(Arc::new(RpcClient::new_mock("account_in_use".to_string())));
        let any_signature = Signature::new(&bs58::decode(SIGNATURE).into_vec().unwrap());
        config.command = CliCommand::Confirm(any_signature);
        assert_eq!(
            process_command(&config).unwrap(),
            format!("Transaction failed: {}", TransactionError::AccountInUse)
        );

        // Failure cases
        config.rpc_client = Some(Arc::new(RpcClient::new_mock("fails".to_string())));

        config.command = CliCommand::Airdrop {
            pubkey: None,
            lamports: 50,
        };
        assert!(process_command(&config).is_err());

        config.command = CliCommand::Balance {
            pubkey: None,
            use_lamports_unit: false,
        };
        assert!(process_command(&config).is_err());

        let bob_keypair = Keypair::new();
        let identity_keypair = Keypair::new();
        config.command = CliCommand::CreateVoteAccount {
            vote_account: 1,
            seed: None,
            identity_account: 2,
            authorized_voter: Some(bob_pubkey),
            authorized_withdrawer: Some(bob_pubkey),
            commission: 0,
            memo: None,
        };
        config.signers = vec![&keypair, &bob_keypair, &identity_keypair];
        assert!(process_command(&config).is_err());

        config.command = CliCommand::VoteAuthorize {
            vote_account_pubkey: bob_pubkey,
            new_authorized_pubkey: bob_pubkey,
            vote_authorize: VoteAuthorize::Voter,
            memo: None,
            authorized: 0,
            new_authorized: None,
        };
        assert!(process_command(&config).is_err());

        config.command = CliCommand::VoteUpdateValidator {
            vote_account_pubkey: bob_pubkey,
            new_identity_account: 1,
            withdraw_authority: 1,
            memo: None,
        };
        assert!(process_command(&config).is_err());

        config.command = CliCommand::GetSlot;
        assert!(process_command(&config).is_err());

        config.command = CliCommand::GetTransactionCount;
        assert!(process_command(&config).is_err());
    }

    #[test]
    fn test_cli_deploy() {
        solana_logger::setup();
        let mut pathbuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pathbuf.push("tests");
        pathbuf.push("fixtures");
        pathbuf.push("noop");
        pathbuf.set_extension("so");

        // Success case
        let mut config = CliConfig::default();
        let account_info_response = json!(Response {
            context: RpcResponseContext { slot: 1 },
            value: Value::Null,
        });
        let mut mocks = HashMap::new();
        mocks.insert(RpcRequest::GetAccountInfo, account_info_response);
        let rpc_client = RpcClient::new_mock_with_mocks("".to_string(), mocks);

        config.rpc_client = Some(Arc::new(rpc_client));
        let default_keypair = Keypair::new();
        config.signers = vec![&default_keypair];

        config.command = CliCommand::Deploy {
            program_location: pathbuf.to_str().unwrap().to_string(),
            address: None,
            use_deprecated_loader: false,
            allow_excessive_balance: false,
        };
        config.output_format = OutputFormat::JsonCompact;
        let result = process_command(&config);
        let json: Value = serde_json::from_str(&result.unwrap()).unwrap();
        let program_id = json
            .as_object()
            .unwrap()
            .get("programId")
            .unwrap()
            .as_str()
            .unwrap();

        assert!(program_id.parse::<Pubkey>().is_ok());

        // Failure case
        config.command = CliCommand::Deploy {
            program_location: "bad/file/location.so".to_string(),
            address: None,
            use_deprecated_loader: false,
            allow_excessive_balance: false,
        };
        assert!(process_command(&config).is_err());
    }

    #[test]
    fn test_parse_transfer_subcommand() {
        let test_commands = app("test", "desc", "version");

        let default_keypair = Keypair::new();
        let default_keypair_file = make_tmp_path("keypair_file");
        write_keypair_file(&default_keypair, &default_keypair_file).unwrap();
        let default_signer = DefaultSigner::new("", &default_keypair_file);

        //Test Transfer Subcommand, SOL
        let from_keypair = keypair_from_seed(&[0u8; 32]).unwrap();
        let from_pubkey = from_keypair.pubkey();
        let from_string = from_pubkey.to_string();
        let to_keypair = keypair_from_seed(&[1u8; 32]).unwrap();
        let to_pubkey = to_keypair.pubkey();
        let to_string = to_pubkey.to_string();
        let test_transfer = test_commands
            .clone()
            .get_matches_from(vec!["test", "transfer", &to_string, "42"]);
        assert_eq!(
            parse_command(&test_transfer, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Transfer {
                    amount: SpendAmount::Some(42_000_000_000),
                    to: to_pubkey,
                    from: 0,
                    sign_only: false,
                    dump_transaction_message: false,
                    allow_unfunded_recipient: false,
                    no_wait: false,
                    blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
                    nonce_account: None,
                    nonce_authority: 0,
                    memo: None,
                    fee_payer: 0,
                    derived_address_seed: None,
                    derived_address_program_id: None,
                },
                signers: vec![read_keypair_file(&default_keypair_file).unwrap().into()],
            }
        );

        // Test Transfer ALL
        let test_transfer = test_commands
            .clone()
            .get_matches_from(vec!["test", "transfer", &to_string, "ALL"]);
        assert_eq!(
            parse_command(&test_transfer, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Transfer {
                    amount: SpendAmount::All,
                    to: to_pubkey,
                    from: 0,
                    sign_only: false,
                    dump_transaction_message: false,
                    allow_unfunded_recipient: false,
                    no_wait: false,
                    blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
                    nonce_account: None,
                    nonce_authority: 0,
                    memo: None,
                    fee_payer: 0,
                    derived_address_seed: None,
                    derived_address_program_id: None,
                },
                signers: vec![read_keypair_file(&default_keypair_file).unwrap().into()],
            }
        );

        // Test Transfer no-wait and --allow-unfunded-recipient
        let test_transfer = test_commands.clone().get_matches_from(vec![
            "test",
            "transfer",
            "--no-wait",
            "--allow-unfunded-recipient",
            &to_string,
            "42",
        ]);
        assert_eq!(
            parse_command(&test_transfer, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Transfer {
                    amount: SpendAmount::Some(42_000_000_000),
                    to: to_pubkey,
                    from: 0,
                    sign_only: false,
                    dump_transaction_message: false,
                    allow_unfunded_recipient: true,
                    no_wait: true,
                    blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
                    nonce_account: None,
                    nonce_authority: 0,
                    memo: None,
                    fee_payer: 0,
                    derived_address_seed: None,
                    derived_address_program_id: None,
                },
                signers: vec![read_keypair_file(&default_keypair_file).unwrap().into()],
            }
        );

        //Test Transfer Subcommand, offline sign
        let blockhash = Hash::new(&[1u8; 32]);
        let blockhash_string = blockhash.to_string();
        let test_transfer = test_commands.clone().get_matches_from(vec![
            "test",
            "transfer",
            &to_string,
            "42",
            "--blockhash",
            &blockhash_string,
            "--sign-only",
        ]);
        assert_eq!(
            parse_command(&test_transfer, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Transfer {
                    amount: SpendAmount::Some(42_000_000_000),
                    to: to_pubkey,
                    from: 0,
                    sign_only: true,
                    dump_transaction_message: false,
                    allow_unfunded_recipient: false,
                    no_wait: false,
                    blockhash_query: BlockhashQuery::None(blockhash),
                    nonce_account: None,
                    nonce_authority: 0,
                    memo: None,
                    fee_payer: 0,
                    derived_address_seed: None,
                    derived_address_program_id: None,
                },
                signers: vec![read_keypair_file(&default_keypair_file).unwrap().into()],
            }
        );

        //Test Transfer Subcommand, submit offline `from`
        let from_sig = from_keypair.sign_message(&[0u8]);
        let from_signer = format!("{}={}", from_pubkey, from_sig);
        let test_transfer = test_commands.clone().get_matches_from(vec![
            "test",
            "transfer",
            &to_string,
            "42",
            "--from",
            &from_string,
            "--fee-payer",
            &from_string,
            "--signer",
            &from_signer,
            "--blockhash",
            &blockhash_string,
        ]);
        assert_eq!(
            parse_command(&test_transfer, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Transfer {
                    amount: SpendAmount::Some(42_000_000_000),
                    to: to_pubkey,
                    from: 0,
                    sign_only: false,
                    dump_transaction_message: false,
                    allow_unfunded_recipient: false,
                    no_wait: false,
                    blockhash_query: BlockhashQuery::FeeCalculator(
                        blockhash_query::Source::Cluster,
                        blockhash
                    ),
                    nonce_account: None,
                    nonce_authority: 0,
                    memo: None,
                    fee_payer: 0,
                    derived_address_seed: None,
                    derived_address_program_id: None,
                },
                signers: vec![Presigner::new(&from_pubkey, &from_sig).into()],
            }
        );

        //Test Transfer Subcommand, with nonce
        let nonce_address = Pubkey::new(&[1u8; 32]);
        let nonce_address_string = nonce_address.to_string();
        let nonce_authority = keypair_from_seed(&[2u8; 32]).unwrap();
        let nonce_authority_file = make_tmp_path("nonce_authority_file");
        write_keypair_file(&nonce_authority, &nonce_authority_file).unwrap();
        let test_transfer = test_commands.clone().get_matches_from(vec![
            "test",
            "transfer",
            &to_string,
            "42",
            "--blockhash",
            &blockhash_string,
            "--nonce",
            &nonce_address_string,
            "--nonce-authority",
            &nonce_authority_file,
        ]);
        assert_eq!(
            parse_command(&test_transfer, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Transfer {
                    amount: SpendAmount::Some(42_000_000_000),
                    to: to_pubkey,
                    from: 0,
                    sign_only: false,
                    dump_transaction_message: false,
                    allow_unfunded_recipient: false,
                    no_wait: false,
                    blockhash_query: BlockhashQuery::FeeCalculator(
                        blockhash_query::Source::NonceAccount(nonce_address),
                        blockhash
                    ),
                    nonce_account: Some(nonce_address),
                    nonce_authority: 1,
                    memo: None,
                    fee_payer: 0,
                    derived_address_seed: None,
                    derived_address_program_id: None,
                },
                signers: vec![
                    read_keypair_file(&default_keypair_file).unwrap().into(),
                    read_keypair_file(&nonce_authority_file).unwrap().into()
                ],
            }
        );

        //Test Transfer Subcommand, with seed
        let derived_address_seed = "seed".to_string();
        let derived_address_program_id = "STAKE";
        let test_transfer = test_commands.clone().get_matches_from(vec![
            "test",
            "transfer",
            &to_string,
            "42",
            "--derived-address-seed",
            &derived_address_seed,
            "--derived-address-program-id",
            derived_address_program_id,
        ]);
        assert_eq!(
            parse_command(&test_transfer, &default_signer, &mut None).unwrap(),
            CliCommandInfo {
                command: CliCommand::Transfer {
                    amount: SpendAmount::Some(42_000_000_000),
                    to: to_pubkey,
                    from: 0,
                    sign_only: false,
                    dump_transaction_message: false,
                    allow_unfunded_recipient: false,
                    no_wait: false,
                    blockhash_query: BlockhashQuery::All(blockhash_query::Source::Cluster),
                    nonce_account: None,
                    nonce_authority: 0,
                    memo: None,
                    fee_payer: 0,
                    derived_address_seed: Some(derived_address_seed),
                    derived_address_program_id: Some(stake::program::id()),
                },
                signers: vec![read_keypair_file(&default_keypair_file).unwrap().into(),],
            }
        );
    }
}
