use {
    crate::{
        display::{
            build_balance_message, build_balance_message_with_config, format_labeled_address,
            unix_timestamp_to_string, writeln_name_value, writeln_transaction,
            BuildBalanceMessageConfig,
        },
        QuietDisplay, VerboseDisplay,
    },
    chrono::{Local, TimeZone},
    console::{style, Emoji},
    inflector::cases::titlecase::to_title_case,
    serde::{Deserialize, Serialize},
    serde_json::{Map, Value},
    solana_account_decoder::parse_token::UiTokenAccount,
    solana_clap_utils::keypair::SignOnly,
    solana_client::rpc_response::{
        RpcAccountBalance, RpcContactInfo, RpcInflationGovernor, RpcInflationRate, RpcKeyedAccount,
        RpcSupply, RpcVoteAccountInfo,
    },
    solana_sdk::{
        clock::{Epoch, Slot, UnixTimestamp},
        epoch_info::EpochInfo,
        hash::Hash,
        native_token::lamports_to_sol,
        pubkey::Pubkey,
        signature::Signature,
        stake::state::{Authorized, Lockup},
        stake_history::StakeHistoryEntry,
        transaction::{Transaction, TransactionError},
    },
    solana_transaction_status::{
        EncodedConfirmedBlock, EncodedTransaction, TransactionConfirmationStatus,
        UiTransactionStatusMeta,
    },
    solana_vote_program::{
        authorized_voters::AuthorizedVoters,
        vote_state::{BlockTimestamp, Lockout, MAX_EPOCH_CREDITS_HISTORY, MAX_LOCKOUT_HISTORY},
    },
    std::{
        collections::{BTreeMap, HashMap},
        fmt,
        str::FromStr,
        time::Duration,
    },
};

static WARNING: Emoji = Emoji("⚠️", "!");

#[derive(PartialEq)]
pub enum OutputFormat {
    Display,
    Json,
    JsonCompact,
    DisplayQuiet,
    DisplayVerbose,
}

impl OutputFormat {
    pub fn formatted_string<T>(&self, item: &T) -> String
    where
        T: Serialize + fmt::Display + QuietDisplay + VerboseDisplay,
    {
        match self {
            OutputFormat::Display => format!("{}", item),
            OutputFormat::DisplayQuiet => {
                let mut s = String::new();
                QuietDisplay::write_str(item, &mut s).unwrap();
                s
            }
            OutputFormat::DisplayVerbose => {
                let mut s = String::new();
                VerboseDisplay::write_str(item, &mut s).unwrap();
                s
            }
            OutputFormat::Json => serde_json::to_string_pretty(item).unwrap(),
            OutputFormat::JsonCompact => serde_json::to_value(item).unwrap().to_string(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct CliAccount {
    #[serde(flatten)]
    pub keyed_account: RpcKeyedAccount,
    #[serde(skip_serializing)]
    pub use_lamports_unit: bool,
}

impl QuietDisplay for CliAccount {}
impl VerboseDisplay for CliAccount {}

impl fmt::Display for CliAccount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln_name_value(f, "Public Key:", &self.keyed_account.pubkey)?;
        writeln_name_value(
            f,
            "Balance:",
            &build_balance_message(
                self.keyed_account.account.lamports,
                self.use_lamports_unit,
                true,
            ),
        )?;
        writeln_name_value(f, "Owner:", &self.keyed_account.account.owner)?;
        writeln_name_value(
            f,
            "Executable:",
            &self.keyed_account.account.executable.to_string(),
        )?;
        writeln_name_value(
            f,
            "Rent Epoch:",
            &self.keyed_account.account.rent_epoch.to_string(),
        )?;
        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct CliBlockProduction {
    pub epoch: Epoch,
    pub start_slot: Slot,
    pub end_slot: Slot,
    pub total_slots: usize,
    pub total_blocks_produced: usize,
    pub total_slots_skipped: usize,
    pub leaders: Vec<CliBlockProductionEntry>,
    pub individual_slot_status: Vec<CliSlotStatus>,
    #[serde(skip_serializing)]
    pub verbose: bool,
}

impl QuietDisplay for CliBlockProduction {}
impl VerboseDisplay for CliBlockProduction {}

impl fmt::Display for CliBlockProduction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln!(
            f,
            "{}",
            style(format!(
                "  {:<44}  {:>15}  {:>15}  {:>15}  {:>23}",
                "Identity",
                "Leader Slots",
                "Blocks Produced",
                "Skipped Slots",
                "Skipped Slot Percentage",
            ))
            .bold()
        )?;
        for leader in &self.leaders {
            writeln!(
                f,
                "  {:<44}  {:>15}  {:>15}  {:>15}  {:>22.2}%",
                leader.identity_pubkey,
                leader.leader_slots,
                leader.blocks_produced,
                leader.skipped_slots,
                leader.skipped_slots as f64 / leader.leader_slots as f64 * 100.
            )?;
        }
        writeln!(f)?;
        writeln!(
            f,
            "  {:<44}  {:>15}  {:>15}  {:>15}  {:>22.2}%",
            format!("Epoch {} total:", self.epoch),
            self.total_slots,
            self.total_blocks_produced,
            self.total_slots_skipped,
            self.total_slots_skipped as f64 / self.total_slots as f64 * 100.
        )?;
        writeln!(
            f,
            "  (using data from {} slots: {} to {})",
            self.total_slots, self.start_slot, self.end_slot
        )?;
        if self.verbose {
            writeln!(f)?;
            writeln!(f)?;
            writeln!(
                f,
                "{}",
                style(format!("  {:<15} {:<44}", "Slot", "Identity Pubkey")).bold(),
            )?;
            for status in &self.individual_slot_status {
                if status.skipped {
                    writeln!(
                        f,
                        "{}",
                        style(format!(
                            "  {:<15} {:<44} SKIPPED",
                            status.slot, status.leader
                        ))
                        .red()
                    )?;
                } else {
                    writeln!(
                        f,
                        "{}",
                        style(format!("  {:<15} {:<44}", status.slot, status.leader))
                    )?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliBlockProductionEntry {
    pub identity_pubkey: String,
    pub leader_slots: u64,
    pub blocks_produced: u64,
    pub skipped_slots: u64,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliSlotStatus {
    pub slot: Slot,
    pub leader: String,
    pub skipped: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliEpochInfo {
    #[serde(flatten)]
    pub epoch_info: EpochInfo,
    #[serde(skip)]
    pub average_slot_time_ms: u64,
    #[serde(skip)]
    pub start_block_time: Option<UnixTimestamp>,
    #[serde(skip)]
    pub current_block_time: Option<UnixTimestamp>,
}

impl QuietDisplay for CliEpochInfo {}
impl VerboseDisplay for CliEpochInfo {}

impl fmt::Display for CliEpochInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln_name_value(
            f,
            "Block height:",
            &self.epoch_info.block_height.to_string(),
        )?;
        writeln_name_value(f, "Slot:", &self.epoch_info.absolute_slot.to_string())?;
        writeln_name_value(f, "Epoch:", &self.epoch_info.epoch.to_string())?;
        if let Some(transaction_count) = &self.epoch_info.transaction_count {
            writeln_name_value(f, "Transaction Count:", &transaction_count.to_string())?;
        }
        let start_slot = self.epoch_info.absolute_slot - self.epoch_info.slot_index;
        let end_slot = start_slot + self.epoch_info.slots_in_epoch;
        writeln_name_value(
            f,
            "Epoch Slot Range:",
            &format!("[{}..{})", start_slot, end_slot),
        )?;
        writeln_name_value(
            f,
            "Epoch Completed Percent:",
            &format!(
                "{:>3.3}%",
                self.epoch_info.slot_index as f64 / self.epoch_info.slots_in_epoch as f64 * 100_f64
            ),
        )?;
        let remaining_slots_in_epoch = self.epoch_info.slots_in_epoch - self.epoch_info.slot_index;
        writeln_name_value(
            f,
            "Epoch Completed Slots:",
            &format!(
                "{}/{} ({} remaining)",
                self.epoch_info.slot_index,
                self.epoch_info.slots_in_epoch,
                remaining_slots_in_epoch
            ),
        )?;
        let (time_elapsed, annotation) = if let (Some(start_block_time), Some(current_block_time)) =
            (self.start_block_time, self.current_block_time)
        {
            (
                Duration::from_secs((current_block_time - start_block_time) as u64),
                None,
            )
        } else {
            (
                slot_to_duration(self.epoch_info.slot_index, self.average_slot_time_ms),
                Some("* estimated based on current slot durations"),
            )
        };
        let time_remaining = slot_to_duration(remaining_slots_in_epoch, self.average_slot_time_ms);
        writeln_name_value(
            f,
            "Epoch Completed Time:",
            &format!(
                "{}{}/{} ({} remaining)",
                humantime::format_duration(time_elapsed).to_string(),
                if annotation.is_some() { "*" } else { "" },
                humantime::format_duration(time_elapsed + time_remaining).to_string(),
                humantime::format_duration(time_remaining).to_string(),
            ),
        )?;
        if let Some(annotation) = annotation {
            writeln!(f)?;
            writeln!(f, "{}", annotation)?;
        }
        Ok(())
    }
}

fn slot_to_duration(slot: Slot, slot_time_ms: u64) -> Duration {
    Duration::from_secs((slot * slot_time_ms) / 1000)
}

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CliValidatorsStakeByVersion {
    pub current_validators: usize,
    pub delinquent_validators: usize,
    pub current_active_stake: u64,
    pub delinquent_active_stake: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
pub enum CliValidatorsSortOrder {
    Delinquent,
    Commission,
    EpochCredits,
    Identity,
    LastVote,
    Root,
    SkipRate,
    Stake,
    VoteAccount,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliValidators {
    pub total_active_stake: u64,
    pub total_current_stake: u64,
    pub total_delinquent_stake: u64,
    pub validators: Vec<CliValidator>,
    pub average_skip_rate: f64,
    pub average_stake_weighted_skip_rate: f64,
    #[serde(skip_serializing)]
    pub validators_sort_order: CliValidatorsSortOrder,
    #[serde(skip_serializing)]
    pub validators_reverse_sort: bool,
    #[serde(skip_serializing)]
    pub number_validators: bool,
    pub stake_by_version: BTreeMap<String, CliValidatorsStakeByVersion>,
    #[serde(skip_serializing)]
    pub use_lamports_unit: bool,
}

impl QuietDisplay for CliValidators {}
impl VerboseDisplay for CliValidators {}

impl fmt::Display for CliValidators {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn write_vote_account(
            f: &mut fmt::Formatter,
            validator: &CliValidator,
            total_active_stake: u64,
            use_lamports_unit: bool,
            highest_last_vote: u64,
            highest_root: u64,
        ) -> fmt::Result {
            fn non_zero_or_dash(v: u64, max_v: u64) -> String {
                if v == 0 {
                    "-         ".into()
                } else if v == max_v {
                    format!("{:>8} (  0)", v)
                } else if v > max_v.saturating_sub(100) {
                    format!("{:>8} ({:>3})", v, -(max_v.saturating_sub(v) as isize))
                } else {
                    format!("{:>8}      ", v)
                }
            }

            writeln!(
                f,
                "{} {:<44}  {:<44}  {:>3}%  {:>14}  {:>14} {:>7} {:>8}  {:>7}  {}",
                if validator.delinquent {
                    WARNING.to_string()
                } else {
                    "\u{a0}".to_string()
                },
                validator.identity_pubkey,
                validator.vote_account_pubkey,
                validator.commission,
                non_zero_or_dash(validator.last_vote, highest_last_vote),
                non_zero_or_dash(validator.root_slot, highest_root),
                if let Some(skip_rate) = validator.skip_rate {
                    format!("{:.2}%", skip_rate)
                } else {
                    "- ".to_string()
                },
                validator.epoch_credits,
                validator.version,
                if validator.activated_stake > 0 {
                    format!(
                        "{} ({:.2}%)",
                        build_balance_message(validator.activated_stake, use_lamports_unit, true),
                        100. * validator.activated_stake as f64 / total_active_stake as f64,
                    )
                } else {
                    "-".into()
                },
            )
        }

        let padding = if self.number_validators {
            ((self.validators.len() + 1) as f64).log10().floor() as usize + 1
        } else {
            0
        };
        let header = style(format!(
            "{:padding$} {:<44}  {:<38}  {}  {}  {} {}  {}  {}  {}",
            " ",
            "Identity",
            "Vote Account",
            "Commission",
            "Last Vote     ",
            "Root Slot   ",
            "Skip Rate",
            "Credits",
            "Version",
            "Active Stake",
            padding = padding + 1
        ))
        .bold();
        writeln!(f, "{}", header)?;

        let mut sorted_validators = self.validators.clone();
        match self.validators_sort_order {
            CliValidatorsSortOrder::Delinquent => {
                sorted_validators.sort_by_key(|a| a.delinquent);
            }
            CliValidatorsSortOrder::Commission => {
                sorted_validators.sort_by_key(|a| a.commission);
            }
            CliValidatorsSortOrder::EpochCredits => {
                sorted_validators.sort_by_key(|a| a.epoch_credits);
            }
            CliValidatorsSortOrder::Identity => {
                sorted_validators.sort_by(|a, b| a.identity_pubkey.cmp(&b.identity_pubkey));
            }
            CliValidatorsSortOrder::LastVote => {
                sorted_validators.sort_by_key(|a| a.last_vote);
            }
            CliValidatorsSortOrder::Root => {
                sorted_validators.sort_by_key(|a| a.root_slot);
            }
            CliValidatorsSortOrder::VoteAccount => {
                sorted_validators.sort_by(|a, b| a.vote_account_pubkey.cmp(&b.vote_account_pubkey));
            }
            CliValidatorsSortOrder::SkipRate => {
                sorted_validators.sort_by(|a, b| {
                    use std::cmp::Ordering;
                    match (a.skip_rate, b.skip_rate) {
                        (None, None) => Ordering::Equal,
                        (None, Some(_)) => Ordering::Greater,
                        (Some(_), None) => Ordering::Less,
                        (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
                    }
                });
            }
            CliValidatorsSortOrder::Stake => {
                sorted_validators.sort_by_key(|a| a.activated_stake);
            }
        }

        if self.validators_reverse_sort {
            sorted_validators.reverse();
        }

        let highest_root = sorted_validators
            .iter()
            .map(|v| v.root_slot)
            .max()
            .unwrap_or_default();
        let highest_last_vote = sorted_validators
            .iter()
            .map(|v| v.last_vote)
            .max()
            .unwrap_or_default();

        for (i, validator) in sorted_validators.iter().enumerate() {
            if padding > 0 {
                write!(f, "{:padding$}", i + 1, padding = padding)?;
            }
            write_vote_account(
                f,
                validator,
                self.total_active_stake,
                self.use_lamports_unit,
                highest_last_vote,
                highest_root,
            )?;
        }

        // The actual header has long scrolled away.  Print the header once more as a footer
        if self.validators.len() > 100 {
            writeln!(f, "{}", header)?;
        }

        writeln!(f)?;
        writeln_name_value(
            f,
            "Average Stake-Weighted Skip Rate:",
            &format!("{:.2}%", self.average_stake_weighted_skip_rate,),
        )?;
        writeln_name_value(
            f,
            "Average Unweighted Skip Rate:    ",
            &format!("{:.2}%", self.average_skip_rate),
        )?;

        writeln!(f)?;
        writeln_name_value(
            f,
            "Active Stake:",
            &build_balance_message(self.total_active_stake, self.use_lamports_unit, true),
        )?;
        if self.total_delinquent_stake > 0 {
            writeln_name_value(
                f,
                "Current Stake:",
                &format!(
                    "{} ({:0.2}%)",
                    &build_balance_message(self.total_current_stake, self.use_lamports_unit, true),
                    100. * self.total_current_stake as f64 / self.total_active_stake as f64
                ),
            )?;
            writeln_name_value(
                f,
                "Delinquent Stake:",
                &format!(
                    "{} ({:0.2}%)",
                    &build_balance_message(
                        self.total_delinquent_stake,
                        self.use_lamports_unit,
                        true
                    ),
                    100. * self.total_delinquent_stake as f64 / self.total_active_stake as f64
                ),
            )?;
        }

        writeln!(f)?;
        writeln!(f, "{}", style("Stake By Version:").bold())?;
        for (version, info) in self.stake_by_version.iter() {
            writeln!(
                f,
                "{:<8} - {:3} current validators ({:>5.2}%){}",
                version,
                info.current_validators,
                100. * info.current_active_stake as f64 / self.total_active_stake as f64,
                if info.delinquent_validators > 0 {
                    format!(
                        ", {:3} delinquent validators ({:>5.2}%)",
                        info.delinquent_validators,
                        100. * info.delinquent_active_stake as f64 / self.total_active_stake as f64
                    )
                } else {
                    "".to_string()
                },
            )?;
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CliValidator {
    pub identity_pubkey: String,
    pub vote_account_pubkey: String,
    pub commission: u8,
    pub last_vote: u64,
    pub root_slot: u64,
    pub credits: u64,       // lifetime credits
    pub epoch_credits: u64, // credits earned in the current epoch
    pub activated_stake: u64,
    pub version: String,
    pub delinquent: bool,
    pub skip_rate: Option<f64>,
}

impl CliValidator {
    pub fn new(
        vote_account: &RpcVoteAccountInfo,
        current_epoch: Epoch,
        version: String,
        skip_rate: Option<f64>,
        address_labels: &HashMap<String, String>,
    ) -> Self {
        Self::_new(
            vote_account,
            current_epoch,
            version,
            skip_rate,
            address_labels,
            false,
        )
    }

    pub fn new_delinquent(
        vote_account: &RpcVoteAccountInfo,
        current_epoch: Epoch,
        version: String,
        skip_rate: Option<f64>,
        address_labels: &HashMap<String, String>,
    ) -> Self {
        Self::_new(
            vote_account,
            current_epoch,
            version,
            skip_rate,
            address_labels,
            true,
        )
    }

    fn _new(
        vote_account: &RpcVoteAccountInfo,
        current_epoch: Epoch,
        version: String,
        skip_rate: Option<f64>,
        address_labels: &HashMap<String, String>,
        delinquent: bool,
    ) -> Self {
        let (credits, epoch_credits) = vote_account
            .epoch_credits
            .iter()
            .find_map(|(epoch, credits, pre_credits)| {
                if *epoch == current_epoch {
                    Some((*credits, credits.saturating_sub(*pre_credits)))
                } else {
                    None
                }
            })
            .unwrap_or((0, 0));
        Self {
            identity_pubkey: format_labeled_address(&vote_account.node_pubkey, address_labels),
            vote_account_pubkey: format_labeled_address(&vote_account.vote_pubkey, address_labels),
            commission: vote_account.commission,
            last_vote: vote_account.last_vote,
            root_slot: vote_account.root_slot,
            credits,
            epoch_credits,
            activated_stake: vote_account.activated_stake,
            version,
            delinquent,
            skip_rate,
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliNonceAccount {
    pub balance: u64,
    pub minimum_balance_for_rent_exemption: u64,
    pub nonce: Option<String>,
    pub lamports_per_signature: Option<u64>,
    pub authority: Option<String>,
    #[serde(skip_serializing)]
    pub use_lamports_unit: bool,
}

impl QuietDisplay for CliNonceAccount {}
impl VerboseDisplay for CliNonceAccount {}

impl fmt::Display for CliNonceAccount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "Balance: {}",
            build_balance_message(self.balance, self.use_lamports_unit, true)
        )?;
        writeln!(
            f,
            "Minimum Balance Required: {}",
            build_balance_message(
                self.minimum_balance_for_rent_exemption,
                self.use_lamports_unit,
                true
            )
        )?;
        let nonce = self.nonce.as_deref().unwrap_or("uninitialized");
        writeln!(f, "Nonce blockhash: {}", nonce)?;
        if let Some(fees) = self.lamports_per_signature {
            writeln!(f, "Fee: {} lamports per signature", fees)?;
        } else {
            writeln!(f, "Fees: uninitialized")?;
        }
        let authority = self.authority.as_deref().unwrap_or("uninitialized");
        writeln!(f, "Authority: {}", authority)
    }
}

#[derive(Serialize, Deserialize)]
pub struct CliStakeVec(Vec<CliKeyedStakeState>);

impl CliStakeVec {
    pub fn new(list: Vec<CliKeyedStakeState>) -> Self {
        Self(list)
    }
}

impl QuietDisplay for CliStakeVec {}
impl VerboseDisplay for CliStakeVec {
    fn write_str(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result {
        for state in &self.0 {
            writeln!(w)?;
            VerboseDisplay::write_str(state, w)?;
        }
        Ok(())
    }
}

impl fmt::Display for CliStakeVec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for state in &self.0 {
            writeln!(f)?;
            write!(f, "{}", state)?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliKeyedStakeState {
    pub stake_pubkey: String,
    #[serde(flatten)]
    pub stake_state: CliStakeState,
}

impl QuietDisplay for CliKeyedStakeState {}
impl VerboseDisplay for CliKeyedStakeState {
    fn write_str(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result {
        writeln!(w, "Stake Pubkey: {}", self.stake_pubkey)?;
        VerboseDisplay::write_str(&self.stake_state, w)
    }
}

impl fmt::Display for CliKeyedStakeState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Stake Pubkey: {}", self.stake_pubkey)?;
        write!(f, "{}", self.stake_state)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliEpochReward {
    pub epoch: Epoch,
    pub effective_slot: Slot,
    pub amount: u64,       // lamports
    pub post_balance: u64, // lamports
    pub percent_change: f64,
    pub apr: Option<f64>,
    pub commission: Option<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliKeyedEpochReward {
    pub address: String,
    pub reward: Option<CliEpochReward>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliEpochRewardshMetadata {
    pub epoch: Epoch,
    pub effective_slot: Slot,
    pub block_time: UnixTimestamp,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliKeyedEpochRewards {
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub epoch_metadata: Option<CliEpochRewardshMetadata>,
    pub rewards: Vec<CliKeyedEpochReward>,
}

impl QuietDisplay for CliKeyedEpochRewards {}
impl VerboseDisplay for CliKeyedEpochRewards {}

impl fmt::Display for CliKeyedEpochRewards {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.rewards.is_empty() {
            writeln!(f, "No rewards found in epoch")?;
            return Ok(());
        }

        if let Some(metadata) = &self.epoch_metadata {
            writeln!(f, "Epoch: {}", metadata.epoch)?;
            writeln!(f, "Reward Slot: {}", metadata.effective_slot)?;
            let timestamp = metadata.block_time;
            writeln!(f, "Block Time: {}", unix_timestamp_to_string(timestamp))?;
        }
        writeln!(f, "Epoch Rewards:")?;
        writeln!(
            f,
            "  {:<44}  {:<18}  {:<18}  {:>14}  {:>14}  {:>10}",
            "Address", "Amount", "New Balance", "Percent Change", "APR", "Commission"
        )?;
        for keyed_reward in &self.rewards {
            match &keyed_reward.reward {
                Some(reward) => {
                    writeln!(
                        f,
                        "  {:<44}  ◎{:<17.9}  ◎{:<17.9}  {:>13.9}%  {:>14}  {:>10}",
                        keyed_reward.address,
                        lamports_to_sol(reward.amount),
                        lamports_to_sol(reward.post_balance),
                        reward.percent_change,
                        reward
                            .apr
                            .map(|apr| format!("{:.2}%", apr))
                            .unwrap_or_default(),
                        reward
                            .commission
                            .map(|commission| format!("{}%", commission))
                            .unwrap_or_else(|| "-".to_string())
                    )?;
                }
                None => {
                    writeln!(f, "  {:<44}  No rewards in epoch", keyed_reward.address,)?;
                }
            }
        }
        Ok(())
    }
}

fn show_votes_and_credits(
    f: &mut fmt::Formatter,
    votes: &[CliLockout],
    epoch_voting_history: &[CliEpochVotingHistory],
) -> fmt::Result {
    if votes.is_empty() {
        return Ok(());
    }

    // Existence of this should guarantee the occurrence of vote truncation
    let newest_history_entry = epoch_voting_history.iter().rev().next();

    writeln!(
        f,
        "{} Votes (using {}/{} entries):",
        (if newest_history_entry.is_none() {
            "All"
        } else {
            "Recent"
        }),
        votes.len(),
        MAX_LOCKOUT_HISTORY
    )?;

    for vote in votes.iter().rev() {
        writeln!(
            f,
            "- slot: {} (confirmation count: {})",
            vote.slot, vote.confirmation_count
        )?;
    }
    if let Some(newest) = newest_history_entry {
        writeln!(
            f,
            "- ... (truncated {} rooted votes, which have been credited)",
            newest.credits
        )?;
    }

    if !epoch_voting_history.is_empty() {
        writeln!(
            f,
            "{} Epoch Voting History (using {}/{} entries):",
            (if epoch_voting_history.len() < MAX_EPOCH_CREDITS_HISTORY {
                "All"
            } else {
                "Recent"
            }),
            epoch_voting_history.len(),
            MAX_EPOCH_CREDITS_HISTORY
        )?;
        writeln!(
            f,
            "* missed credits include slots unavailable to vote on due to delinquent leaders",
        )?;
    }

    for entry in epoch_voting_history.iter().rev() {
        writeln!(
            f, // tame fmt so that this will be folded like following
            "- epoch: {}",
            entry.epoch
        )?;
        writeln!(
            f,
            "  credits range: ({}..{}]",
            entry.prev_credits, entry.credits
        )?;
        writeln!(
            f,
            "  credits/slots: {}/{}",
            entry.credits_earned, entry.slots_in_epoch
        )?;
    }
    if let Some(oldest) = epoch_voting_history.iter().next() {
        if oldest.prev_credits > 0 {
            // Oldest entry doesn't start with 0. so history must be truncated...

            // count of this combined pseudo credits range: (0..=oldest.prev_credits] like the above
            // (or this is just [1..=oldest.prev_credits] for human's simpler minds)
            let count = oldest.prev_credits;

            writeln!(
                f,
                "- ... (omitting {} past rooted votes, which have already been credited)",
                count
            )?;
        }
    }

    Ok(())
}

fn show_epoch_rewards(
    f: &mut fmt::Formatter,
    epoch_rewards: &Option<Vec<CliEpochReward>>,
) -> fmt::Result {
    if let Some(epoch_rewards) = epoch_rewards {
        if epoch_rewards.is_empty() {
            return Ok(());
        }

        writeln!(f, "Epoch Rewards:")?;
        writeln!(
            f,
            "  {:<6}  {:<11}  {:<18}  {:<18}  {:>14}  {:>14}  {:>10}",
            "Epoch", "Reward Slot", "Amount", "New Balance", "Percent Change", "APR", "Commission"
        )?;
        for reward in epoch_rewards {
            writeln!(
                f,
                "  {:<6}  {:<11}  ◎{:<17.9}  ◎{:<17.9}  {:>13.9}%  {:>14}  {:>10}",
                reward.epoch,
                reward.effective_slot,
                lamports_to_sol(reward.amount),
                lamports_to_sol(reward.post_balance),
                reward.percent_change,
                reward
                    .apr
                    .map(|apr| format!("{:.2}%", apr))
                    .unwrap_or_default(),
                reward
                    .commission
                    .map(|commission| format!("{}%", commission))
                    .unwrap_or_else(|| "-".to_string())
            )?;
        }
    }
    Ok(())
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliStakeState {
    pub stake_type: CliStakeType,
    pub account_balance: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credits_observed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delegated_stake: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delegated_vote_account_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activation_epoch: Option<Epoch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deactivation_epoch: Option<Epoch>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub authorized: Option<CliAuthorized>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub lockup: Option<CliLockup>,
    #[serde(skip_serializing)]
    pub use_lamports_unit: bool,
    #[serde(skip_serializing)]
    pub current_epoch: Epoch,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rent_exempt_reserve: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_stake: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activating_stake: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deactivating_stake: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch_rewards: Option<Vec<CliEpochReward>>,
}

impl QuietDisplay for CliStakeState {}
impl VerboseDisplay for CliStakeState {
    fn write_str(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write!(w, "{}", self)?;
        if let Some(credits) = self.credits_observed {
            writeln!(w, "Credits Observed: {}", credits)?;
        }
        Ok(())
    }
}

impl fmt::Display for CliStakeState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn show_authorized(f: &mut fmt::Formatter, authorized: &CliAuthorized) -> fmt::Result {
            writeln!(f, "Stake Authority: {}", authorized.staker)?;
            writeln!(f, "Withdraw Authority: {}", authorized.withdrawer)?;
            Ok(())
        }
        fn show_lockup(f: &mut fmt::Formatter, lockup: Option<&CliLockup>) -> fmt::Result {
            if let Some(lockup) = lockup {
                if lockup.unix_timestamp != UnixTimestamp::default() {
                    writeln!(
                        f,
                        "Lockup Timestamp: {}",
                        unix_timestamp_to_string(lockup.unix_timestamp)
                    )?;
                }
                if lockup.epoch != Epoch::default() {
                    writeln!(f, "Lockup Epoch: {}", lockup.epoch)?;
                }
                writeln!(f, "Lockup Custodian: {}", lockup.custodian)?;
            }
            Ok(())
        }

        writeln!(
            f,
            "Balance: {}",
            build_balance_message(self.account_balance, self.use_lamports_unit, true)
        )?;

        if let Some(rent_exempt_reserve) = self.rent_exempt_reserve {
            writeln!(
                f,
                "Rent Exempt Reserve: {}",
                build_balance_message(rent_exempt_reserve, self.use_lamports_unit, true)
            )?;
        }

        match self.stake_type {
            CliStakeType::RewardsPool => writeln!(f, "Stake account is a rewards pool")?,
            CliStakeType::Uninitialized => writeln!(f, "Stake account is uninitialized")?,
            CliStakeType::Initialized => {
                writeln!(f, "Stake account is undelegated")?;
                show_authorized(f, self.authorized.as_ref().unwrap())?;
                show_lockup(f, self.lockup.as_ref())?;
            }
            CliStakeType::Stake => {
                let show_delegation = {
                    self.active_stake.is_some()
                        || self.activating_stake.is_some()
                        || self.deactivating_stake.is_some()
                        || self
                            .deactivation_epoch
                            .map(|de| de > self.current_epoch)
                            .unwrap_or(true)
                };
                if show_delegation {
                    let delegated_stake = self.delegated_stake.unwrap();
                    writeln!(
                        f,
                        "Delegated Stake: {}",
                        build_balance_message(delegated_stake, self.use_lamports_unit, true)
                    )?;
                    if self
                        .deactivation_epoch
                        .map(|d| self.current_epoch <= d)
                        .unwrap_or(true)
                    {
                        let active_stake = self.active_stake.unwrap_or(0);
                        writeln!(
                            f,
                            "Active Stake: {}",
                            build_balance_message(active_stake, self.use_lamports_unit, true),
                        )?;
                        let activating_stake = self.activating_stake.or_else(|| {
                            if self.active_stake.is_none() {
                                Some(delegated_stake)
                            } else {
                                None
                            }
                        });
                        if let Some(activating_stake) = activating_stake {
                            writeln!(
                                f,
                                "Activating Stake: {}",
                                build_balance_message(
                                    activating_stake,
                                    self.use_lamports_unit,
                                    true
                                ),
                            )?;
                            writeln!(
                                f,
                                "Stake activates starting from epoch: {}",
                                self.activation_epoch.unwrap()
                            )?;
                        }
                    }

                    if let Some(deactivation_epoch) = self.deactivation_epoch {
                        if self.current_epoch > deactivation_epoch {
                            let deactivating_stake = self.deactivating_stake.or(self.active_stake);
                            if let Some(deactivating_stake) = deactivating_stake {
                                writeln!(
                                    f,
                                    "Inactive Stake: {}",
                                    build_balance_message(
                                        delegated_stake - deactivating_stake,
                                        self.use_lamports_unit,
                                        true
                                    ),
                                )?;
                                writeln!(
                                    f,
                                    "Deactivating Stake: {}",
                                    build_balance_message(
                                        deactivating_stake,
                                        self.use_lamports_unit,
                                        true
                                    ),
                                )?;
                            }
                        }
                        writeln!(
                            f,
                            "Stake deactivates starting from epoch: {}",
                            deactivation_epoch
                        )?;
                    }
                    if let Some(delegated_vote_account_address) =
                        &self.delegated_vote_account_address
                    {
                        writeln!(
                            f,
                            "Delegated Vote Account Address: {}",
                            delegated_vote_account_address
                        )?;
                    }
                } else {
                    writeln!(f, "Stake account is undelegated")?;
                }
                show_authorized(f, self.authorized.as_ref().unwrap())?;
                show_lockup(f, self.lockup.as_ref())?;
                show_epoch_rewards(f, &self.epoch_rewards)?
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum CliStakeType {
    Stake,
    RewardsPool,
    Uninitialized,
    Initialized,
}

impl Default for CliStakeType {
    fn default() -> Self {
        Self::Uninitialized
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliStakeHistory {
    pub entries: Vec<CliStakeHistoryEntry>,
    #[serde(skip_serializing)]
    pub use_lamports_unit: bool,
}

impl QuietDisplay for CliStakeHistory {}
impl VerboseDisplay for CliStakeHistory {}

impl fmt::Display for CliStakeHistory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln!(
            f,
            "{}",
            style(format!(
                "  {:<5}  {:>20}  {:>20}  {:>20}",
                "Epoch", "Effective Stake", "Activating Stake", "Deactivating Stake",
            ))
            .bold()
        )?;
        let config = BuildBalanceMessageConfig {
            use_lamports_unit: self.use_lamports_unit,
            show_unit: false,
            trim_trailing_zeros: false,
        };
        for entry in &self.entries {
            writeln!(
                f,
                "  {:>5}  {:>20}  {:>20}  {:>20} {}",
                entry.epoch,
                build_balance_message_with_config(entry.effective_stake, &config),
                build_balance_message_with_config(entry.activating_stake, &config),
                build_balance_message_with_config(entry.deactivating_stake, &config),
                if self.use_lamports_unit {
                    "lamports"
                } else {
                    "SOL"
                }
            )?;
        }
        Ok(())
    }
}

impl From<&(Epoch, StakeHistoryEntry)> for CliStakeHistoryEntry {
    fn from((epoch, entry): &(Epoch, StakeHistoryEntry)) -> Self {
        Self {
            epoch: *epoch,
            effective_stake: entry.effective,
            activating_stake: entry.activating,
            deactivating_stake: entry.deactivating,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliStakeHistoryEntry {
    pub epoch: Epoch,
    pub effective_stake: u64,
    pub activating_stake: u64,
    pub deactivating_stake: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliAuthorized {
    pub staker: String,
    pub withdrawer: String,
}

impl From<&Authorized> for CliAuthorized {
    fn from(authorized: &Authorized) -> Self {
        Self {
            staker: authorized.staker.to_string(),
            withdrawer: authorized.withdrawer.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliLockup {
    pub unix_timestamp: UnixTimestamp,
    pub epoch: Epoch,
    pub custodian: String,
}

impl From<&Lockup> for CliLockup {
    fn from(lockup: &Lockup) -> Self {
        Self {
            unix_timestamp: lockup.unix_timestamp,
            epoch: lockup.epoch,
            custodian: lockup.custodian.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct CliValidatorInfoVec(Vec<CliValidatorInfo>);

impl CliValidatorInfoVec {
    pub fn new(list: Vec<CliValidatorInfo>) -> Self {
        Self(list)
    }
}

impl QuietDisplay for CliValidatorInfoVec {}
impl VerboseDisplay for CliValidatorInfoVec {}

impl fmt::Display for CliValidatorInfoVec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.0.is_empty() {
            writeln!(f, "No validator info accounts found")?;
        }
        for validator_info in &self.0 {
            writeln!(f)?;
            write!(f, "{}", validator_info)?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliValidatorInfo {
    pub identity_pubkey: String,
    pub info_pubkey: String,
    pub info: Map<String, Value>,
}

impl QuietDisplay for CliValidatorInfo {}
impl VerboseDisplay for CliValidatorInfo {}

impl fmt::Display for CliValidatorInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln_name_value(f, "Validator Identity:", &self.identity_pubkey)?;
        writeln_name_value(f, "  Info Address:", &self.info_pubkey)?;
        for (key, value) in self.info.iter() {
            writeln_name_value(
                f,
                &format!("  {}:", to_title_case(key)),
                value.as_str().unwrap_or("?"),
            )?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliVoteAccount {
    pub account_balance: u64,
    pub validator_identity: String,
    #[serde(flatten)]
    pub authorized_voters: CliAuthorizedVoters,
    pub authorized_withdrawer: String,
    pub credits: u64,
    pub commission: u8,
    pub root_slot: Option<Slot>,
    pub recent_timestamp: BlockTimestamp,
    pub votes: Vec<CliLockout>,
    pub epoch_voting_history: Vec<CliEpochVotingHistory>,
    #[serde(skip_serializing)]
    pub use_lamports_unit: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch_rewards: Option<Vec<CliEpochReward>>,
}

impl QuietDisplay for CliVoteAccount {}
impl VerboseDisplay for CliVoteAccount {}

impl fmt::Display for CliVoteAccount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "Account Balance: {}",
            build_balance_message(self.account_balance, self.use_lamports_unit, true)
        )?;
        writeln!(f, "Validator Identity: {}", self.validator_identity)?;
        writeln!(f, "Vote Authority: {}", self.authorized_voters)?;
        writeln!(f, "Withdraw Authority: {}", self.authorized_withdrawer)?;
        writeln!(f, "Credits: {}", self.credits)?;
        writeln!(f, "Commission: {}%", self.commission)?;
        writeln!(
            f,
            "Root Slot: {}",
            match self.root_slot {
                Some(slot) => slot.to_string(),
                None => "~".to_string(),
            }
        )?;
        writeln!(
            f,
            "Recent Timestamp: {} from slot {}",
            unix_timestamp_to_string(self.recent_timestamp.timestamp),
            self.recent_timestamp.slot
        )?;
        show_votes_and_credits(f, &self.votes, &self.epoch_voting_history)?;
        show_epoch_rewards(f, &self.epoch_rewards)?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliAuthorizedVoters {
    authorized_voters: BTreeMap<Epoch, String>,
}

impl QuietDisplay for CliAuthorizedVoters {}
impl VerboseDisplay for CliAuthorizedVoters {}

impl fmt::Display for CliAuthorizedVoters {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.authorized_voters)
    }
}

impl From<&AuthorizedVoters> for CliAuthorizedVoters {
    fn from(authorized_voters: &AuthorizedVoters) -> Self {
        let mut voter_map: BTreeMap<Epoch, String> = BTreeMap::new();
        for (epoch, voter) in authorized_voters.iter() {
            voter_map.insert(*epoch, voter.to_string());
        }
        Self {
            authorized_voters: voter_map,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliEpochVotingHistory {
    pub epoch: Epoch,
    pub slots_in_epoch: u64,
    pub credits_earned: u64,
    pub credits: u64,
    pub prev_credits: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliLockout {
    pub slot: Slot,
    pub confirmation_count: u32,
}

impl From<&Lockout> for CliLockout {
    fn from(lockout: &Lockout) -> Self {
        Self {
            slot: lockout.slot,
            confirmation_count: lockout.confirmation_count,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliBlockTime {
    pub slot: Slot,
    pub timestamp: UnixTimestamp,
}

impl QuietDisplay for CliBlockTime {}
impl VerboseDisplay for CliBlockTime {}

impl fmt::Display for CliBlockTime {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln_name_value(f, "Block:", &self.slot.to_string())?;
        writeln_name_value(f, "Date:", &unix_timestamp_to_string(self.timestamp))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliLeaderSchedule {
    pub epoch: Epoch,
    pub leader_schedule_entries: Vec<CliLeaderScheduleEntry>,
}

impl QuietDisplay for CliLeaderSchedule {}
impl VerboseDisplay for CliLeaderSchedule {}

impl fmt::Display for CliLeaderSchedule {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for entry in &self.leader_schedule_entries {
            writeln!(f, "  {:<15} {:<44}", entry.slot, entry.leader)?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliLeaderScheduleEntry {
    pub slot: Slot,
    pub leader: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliInflation {
    pub governor: RpcInflationGovernor,
    pub current_rate: RpcInflationRate,
}

impl QuietDisplay for CliInflation {}
impl VerboseDisplay for CliInflation {}

impl fmt::Display for CliInflation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{}", style("Inflation Governor:").bold())?;
        if (self.governor.initial - self.governor.terminal).abs() < f64::EPSILON {
            writeln!(
                f,
                "Fixed rate:              {:>5.2}%",
                self.governor.terminal * 100.
            )?;
        } else {
            writeln!(
                f,
                "Initial rate:            {:>5.2}%",
                self.governor.initial * 100.
            )?;
            writeln!(
                f,
                "Terminal rate:           {:>5.2}%",
                self.governor.terminal * 100.
            )?;
            writeln!(
                f,
                "Rate reduction per year: {:>5.2}%",
                self.governor.taper * 100.
            )?;
            writeln!(
                f,
                "* Rate reduction is derived using the target slot time in genesis config"
            )?;
        }
        if self.governor.foundation_term > 0. {
            writeln!(
                f,
                "Foundation percentage:   {:>5.2}%",
                self.governor.foundation
            )?;
            writeln!(
                f,
                "Foundation term:         {:.1} years",
                self.governor.foundation_term
            )?;
        }

        writeln!(
            f,
            "\n{}",
            style(format!("Inflation for Epoch {}:", self.current_rate.epoch)).bold()
        )?;
        writeln!(
            f,
            "Total rate:              {:>5.2}%",
            self.current_rate.total * 100.
        )?;
        writeln!(
            f,
            "Staking rate:            {:>5.2}%",
            self.current_rate.validator * 100.
        )?;

        if self.current_rate.foundation > 0. {
            writeln!(
                f,
                "Foundation rate:         {:>5.2}%",
                self.current_rate.foundation * 100.
            )?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CliSignOnlyData {
    pub blockhash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub signers: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub absent: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub bad_sig: Vec<String>,
}

impl QuietDisplay for CliSignOnlyData {}
impl VerboseDisplay for CliSignOnlyData {}

impl fmt::Display for CliSignOnlyData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln_name_value(f, "Blockhash:", &self.blockhash)?;
        if let Some(message) = self.message.as_ref() {
            writeln_name_value(f, "Transaction Message:", message)?;
        }
        if !self.signers.is_empty() {
            writeln!(f, "{}", style("Signers (Pubkey=Signature):").bold())?;
            for signer in self.signers.iter() {
                writeln!(f, " {}", signer)?;
            }
        }
        if !self.absent.is_empty() {
            writeln!(f, "{}", style("Absent Signers (Pubkey):").bold())?;
            for pubkey in self.absent.iter() {
                writeln!(f, " {}", pubkey)?;
            }
        }
        if !self.bad_sig.is_empty() {
            writeln!(f, "{}", style("Bad Signatures (Pubkey):").bold())?;
            for pubkey in self.bad_sig.iter() {
                writeln!(f, " {}", pubkey)?;
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliSignature {
    pub signature: String,
}

impl QuietDisplay for CliSignature {}
impl VerboseDisplay for CliSignature {}

impl fmt::Display for CliSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln_name_value(f, "Signature:", &self.signature)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliAccountBalances {
    pub accounts: Vec<RpcAccountBalance>,
}

impl QuietDisplay for CliAccountBalances {}
impl VerboseDisplay for CliAccountBalances {}

impl fmt::Display for CliAccountBalances {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "{}",
            style(format!("{:<44}  {}", "Address", "Balance")).bold()
        )?;
        for account in &self.accounts {
            writeln!(
                f,
                "{:<44}  {}",
                account.address,
                &format!("{} SOL", lamports_to_sol(account.lamports))
            )?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliSupply {
    pub total: u64,
    pub circulating: u64,
    pub non_circulating: u64,
    pub non_circulating_accounts: Vec<String>,
    #[serde(skip_serializing)]
    pub print_accounts: bool,
}

impl From<RpcSupply> for CliSupply {
    fn from(rpc_supply: RpcSupply) -> Self {
        Self {
            total: rpc_supply.total,
            circulating: rpc_supply.circulating,
            non_circulating: rpc_supply.non_circulating,
            non_circulating_accounts: rpc_supply.non_circulating_accounts,
            print_accounts: false,
        }
    }
}

impl QuietDisplay for CliSupply {}
impl VerboseDisplay for CliSupply {}

impl fmt::Display for CliSupply {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln_name_value(f, "Total:", &format!("{} SOL", lamports_to_sol(self.total)))?;
        writeln_name_value(
            f,
            "Circulating:",
            &format!("{} SOL", lamports_to_sol(self.circulating)),
        )?;
        writeln_name_value(
            f,
            "Non-Circulating:",
            &format!("{} SOL", lamports_to_sol(self.non_circulating)),
        )?;
        if self.print_accounts {
            writeln!(f)?;
            writeln_name_value(f, "Non-Circulating Accounts:", " ")?;
            for account in &self.non_circulating_accounts {
                writeln!(f, "  {}", account)?;
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliFeesInner {
    pub slot: Slot,
    pub blockhash: String,
    pub lamports_per_signature: u64,
    pub last_valid_slot: Option<Slot>,
    pub last_valid_block_height: Option<Slot>,
}

impl QuietDisplay for CliFeesInner {}
impl VerboseDisplay for CliFeesInner {}

impl fmt::Display for CliFeesInner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln_name_value(f, "Blockhash:", &self.blockhash)?;
        writeln_name_value(
            f,
            "Lamports per signature:",
            &self.lamports_per_signature.to_string(),
        )?;
        let last_valid_block_height = self
            .last_valid_block_height
            .map(|s| s.to_string())
            .unwrap_or_default();
        writeln_name_value(f, "Last valid block height:", &last_valid_block_height)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliFees {
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub inner: Option<CliFeesInner>,
}

impl QuietDisplay for CliFees {}
impl VerboseDisplay for CliFees {}

impl fmt::Display for CliFees {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.inner.as_ref() {
            Some(inner) => write!(f, "{}", inner),
            None => write!(f, "Fees unavailable"),
        }
    }
}

impl CliFees {
    pub fn some(
        slot: Slot,
        blockhash: Hash,
        lamports_per_signature: u64,
        last_valid_slot: Option<Slot>,
        last_valid_block_height: Option<Slot>,
    ) -> Self {
        Self {
            inner: Some(CliFeesInner {
                slot,
                blockhash: blockhash.to_string(),
                lamports_per_signature,
                last_valid_slot,
                last_valid_block_height,
            }),
        }
    }
    pub fn none() -> Self {
        Self { inner: None }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliTokenAccount {
    pub address: String,
    #[serde(flatten)]
    pub token_account: UiTokenAccount,
}

impl QuietDisplay for CliTokenAccount {}
impl VerboseDisplay for CliTokenAccount {}

impl fmt::Display for CliTokenAccount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln_name_value(f, "Address:", &self.address)?;
        let account = &self.token_account;
        writeln_name_value(
            f,
            "Balance:",
            &account.token_amount.real_number_string_trimmed(),
        )?;
        let mint = format!(
            "{}{}",
            account.mint,
            if account.is_native { " (native)" } else { "" }
        );
        writeln_name_value(f, "Mint:", &mint)?;
        writeln_name_value(f, "Owner:", &account.owner)?;
        writeln_name_value(f, "State:", &format!("{:?}", account.state))?;
        if let Some(delegate) = &account.delegate {
            writeln!(f, "Delegation:")?;
            writeln_name_value(f, "  Delegate:", delegate)?;
            let allowance = account.delegated_amount.as_ref().unwrap();
            writeln_name_value(f, "  Allowance:", &allowance.real_number_string_trimmed())?;
        }
        writeln_name_value(
            f,
            "Close authority:",
            account.close_authority.as_ref().unwrap_or(&String::new()),
        )?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliProgramId {
    pub program_id: String,
}

impl QuietDisplay for CliProgramId {}
impl VerboseDisplay for CliProgramId {}

impl fmt::Display for CliProgramId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln_name_value(f, "Program Id:", &self.program_id)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliProgramBuffer {
    pub buffer: String,
}

impl QuietDisplay for CliProgramBuffer {}
impl VerboseDisplay for CliProgramBuffer {}

impl fmt::Display for CliProgramBuffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln_name_value(f, "Buffer:", &self.buffer)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CliProgramAccountType {
    Buffer,
    Program,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliProgramAuthority {
    pub authority: String,
    pub account_type: CliProgramAccountType,
}

impl QuietDisplay for CliProgramAuthority {}
impl VerboseDisplay for CliProgramAuthority {}

impl fmt::Display for CliProgramAuthority {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln_name_value(f, "Account Type:", &format!("{:?}", self.account_type))?;
        writeln_name_value(f, "Authority:", &self.authority)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliProgram {
    pub program_id: String,
    pub owner: String,
    pub data_len: usize,
}
impl QuietDisplay for CliProgram {}
impl VerboseDisplay for CliProgram {}
impl fmt::Display for CliProgram {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln_name_value(f, "Program Id:", &self.program_id)?;
        writeln_name_value(f, "Owner:", &self.owner)?;
        writeln_name_value(
            f,
            "Data Length:",
            &format!("{:?} ({:#x?}) bytes", self.data_len, self.data_len),
        )?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliUpgradeableProgram {
    pub program_id: String,
    pub owner: String,
    pub programdata_address: String,
    pub authority: String,
    pub last_deploy_slot: u64,
    pub data_len: usize,
}
impl QuietDisplay for CliUpgradeableProgram {}
impl VerboseDisplay for CliUpgradeableProgram {}
impl fmt::Display for CliUpgradeableProgram {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln_name_value(f, "Program Id:", &self.program_id)?;
        writeln_name_value(f, "Owner:", &self.owner)?;
        writeln_name_value(f, "ProgramData Address:", &self.programdata_address)?;
        writeln_name_value(f, "Authority:", &self.authority)?;
        writeln_name_value(
            f,
            "Last Deployed In Slot:",
            &self.last_deploy_slot.to_string(),
        )?;
        writeln_name_value(
            f,
            "Data Length:",
            &format!("{:?} ({:#x?}) bytes", self.data_len, self.data_len),
        )?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliUpgradeableBuffer {
    pub address: String,
    pub authority: String,
    pub data_len: usize,
    pub lamports: u64,
    #[serde(skip_serializing)]
    pub use_lamports_unit: bool,
}
impl QuietDisplay for CliUpgradeableBuffer {}
impl VerboseDisplay for CliUpgradeableBuffer {}
impl fmt::Display for CliUpgradeableBuffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln_name_value(f, "Buffer Address:", &self.address)?;
        writeln_name_value(f, "Authority:", &self.authority)?;
        writeln_name_value(
            f,
            "Balance:",
            &build_balance_message(self.lamports, self.use_lamports_unit, true),
        )?;
        writeln_name_value(
            f,
            "Data Length:",
            &format!("{:?} ({:#x?}) bytes", self.data_len, self.data_len),
        )?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliUpgradeableBuffers {
    pub buffers: Vec<CliUpgradeableBuffer>,
    #[serde(skip_serializing)]
    pub use_lamports_unit: bool,
}
impl QuietDisplay for CliUpgradeableBuffers {}
impl VerboseDisplay for CliUpgradeableBuffers {}
impl fmt::Display for CliUpgradeableBuffers {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln!(
            f,
            "{}",
            style(format!(
                "{:<44} | {:<44} | {}",
                "Buffer Address", "Authority", "Balance"
            ))
            .bold()
        )?;
        for buffer in self.buffers.iter() {
            writeln!(
                f,
                "{}",
                &format!(
                    "{:<44} | {:<44} | {}",
                    buffer.address,
                    buffer.authority,
                    build_balance_message(buffer.lamports, self.use_lamports_unit, true)
                )
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct ReturnSignersConfig {
    pub dump_transaction_message: bool,
}

pub fn return_signers(
    tx: &Transaction,
    output_format: &OutputFormat,
) -> Result<String, Box<dyn std::error::Error>> {
    return_signers_with_config(tx, output_format, &ReturnSignersConfig::default())
}

pub fn return_signers_with_config(
    tx: &Transaction,
    output_format: &OutputFormat,
    config: &ReturnSignersConfig,
) -> Result<String, Box<dyn std::error::Error>> {
    let verify_results = tx.verify_with_results();
    let mut signers = Vec::new();
    let mut absent = Vec::new();
    let mut bad_sig = Vec::new();
    tx.signatures
        .iter()
        .zip(tx.message.account_keys.iter())
        .zip(verify_results.into_iter())
        .for_each(|((sig, key), res)| {
            if res {
                signers.push(format!("{}={}", key, sig))
            } else if *sig == Signature::default() {
                absent.push(key.to_string());
            } else {
                bad_sig.push(key.to_string());
            }
        });
    let message = if config.dump_transaction_message {
        let message_data = tx.message_data();
        Some(base64::encode(&message_data))
    } else {
        None
    };

    let cli_command = CliSignOnlyData {
        blockhash: tx.message.recent_blockhash.to_string(),
        message,
        signers,
        absent,
        bad_sig,
    };

    Ok(output_format.formatted_string(&cli_command))
}

pub fn parse_sign_only_reply_string(reply: &str) -> SignOnly {
    let object: Value = serde_json::from_str(reply).unwrap();
    let blockhash_str = object.get("blockhash").unwrap().as_str().unwrap();
    let blockhash = blockhash_str.parse::<Hash>().unwrap();
    let mut present_signers: Vec<(Pubkey, Signature)> = Vec::new();
    let signer_strings = object.get("signers");
    if let Some(sig_strings) = signer_strings {
        present_signers = sig_strings
            .as_array()
            .unwrap()
            .iter()
            .map(|signer_string| {
                let mut signer = signer_string.as_str().unwrap().split('=');
                let key = Pubkey::from_str(signer.next().unwrap()).unwrap();
                let sig = Signature::from_str(signer.next().unwrap()).unwrap();
                (key, sig)
            })
            .collect();
    }
    let mut absent_signers: Vec<Pubkey> = Vec::new();
    let signer_strings = object.get("absent");
    if let Some(sig_strings) = signer_strings {
        absent_signers = sig_strings
            .as_array()
            .unwrap()
            .iter()
            .map(|val| {
                let s = val.as_str().unwrap();
                Pubkey::from_str(s).unwrap()
            })
            .collect();
    }
    let mut bad_signers: Vec<Pubkey> = Vec::new();
    let signer_strings = object.get("badSig");
    if let Some(sig_strings) = signer_strings {
        bad_signers = sig_strings
            .as_array()
            .unwrap()
            .iter()
            .map(|val| {
                let s = val.as_str().unwrap();
                Pubkey::from_str(s).unwrap()
            })
            .collect();
    }

    let message = object
        .get("message")
        .and_then(|o| o.as_str())
        .map(|m| m.to_string());

    SignOnly {
        blockhash,
        message,
        present_signers,
        absent_signers,
        bad_signers,
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CliSignatureVerificationStatus {
    None,
    Pass,
    Fail,
}

impl CliSignatureVerificationStatus {
    pub fn verify_transaction(tx: &Transaction) -> Vec<Self> {
        tx.verify_with_results()
            .iter()
            .zip(&tx.signatures)
            .map(|(stat, sig)| match stat {
                true => CliSignatureVerificationStatus::Pass,
                false if sig == &Signature::default() => CliSignatureVerificationStatus::None,
                false => CliSignatureVerificationStatus::Fail,
            })
            .collect()
    }
}

impl fmt::Display for CliSignatureVerificationStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Pass => write!(f, "pass"),
            Self::Fail => write!(f, "fail"),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliBlock {
    #[serde(flatten)]
    pub encoded_confirmed_block: EncodedConfirmedBlock,
    #[serde(skip_serializing)]
    pub slot: Slot,
}

impl QuietDisplay for CliBlock {}
impl VerboseDisplay for CliBlock {}

impl fmt::Display for CliBlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Slot: {}", self.slot)?;
        writeln!(
            f,
            "Parent Slot: {}",
            self.encoded_confirmed_block.parent_slot
        )?;
        writeln!(f, "Blockhash: {}", self.encoded_confirmed_block.blockhash)?;
        writeln!(
            f,
            "Previous Blockhash: {}",
            self.encoded_confirmed_block.previous_blockhash
        )?;
        if let Some(block_time) = self.encoded_confirmed_block.block_time {
            writeln!(f, "Block Time: {:?}", Local.timestamp(block_time, 0))?;
        }
        if let Some(block_height) = self.encoded_confirmed_block.block_height {
            writeln!(f, "Block Height: {:?}", block_height)?;
        }
        if !self.encoded_confirmed_block.rewards.is_empty() {
            let mut rewards = self.encoded_confirmed_block.rewards.clone();
            rewards.sort_by(|a, b| a.pubkey.cmp(&b.pubkey));
            let mut total_rewards = 0;
            writeln!(f, "Rewards:")?;
            writeln!(
                f,
                "  {:<44}  {:^15}  {:<15}  {:<20}  {:>14}  {:>10}",
                "Address", "Type", "Amount", "New Balance", "Percent Change", "Commission"
            )?;
            for reward in rewards {
                let sign = if reward.lamports < 0 { "-" } else { "" };

                total_rewards += reward.lamports;
                writeln!(
                    f,
                    "  {:<44}  {:^15}  {:>15}  {}  {}",
                    reward.pubkey,
                    if let Some(reward_type) = reward.reward_type {
                        format!("{}", reward_type)
                    } else {
                        "-".to_string()
                    },
                    format!(
                        "{}◎{:<14.9}",
                        sign,
                        lamports_to_sol(reward.lamports.abs() as u64)
                    ),
                    if reward.post_balance == 0 {
                        "          -                 -".to_string()
                    } else {
                        format!(
                            "◎{:<19.9}  {:>13.9}%",
                            lamports_to_sol(reward.post_balance),
                            (reward.lamports.abs() as f64
                                / (reward.post_balance as f64 - reward.lamports as f64))
                                * 100.0
                        )
                    },
                    reward
                        .commission
                        .map(|commission| format!("{:>9}%", commission))
                        .unwrap_or_else(|| "    -".to_string())
                )?;
            }

            let sign = if total_rewards < 0 { "-" } else { "" };
            writeln!(
                f,
                "Total Rewards: {}◎{:<12.9}",
                sign,
                lamports_to_sol(total_rewards.abs() as u64)
            )?;
        }
        for (index, transaction_with_meta) in
            self.encoded_confirmed_block.transactions.iter().enumerate()
        {
            writeln!(f, "Transaction {}:", index)?;
            writeln_transaction(
                f,
                &transaction_with_meta.transaction.decode().unwrap(),
                &transaction_with_meta.meta,
                "  ",
                None,
                None,
            )?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliTransaction {
    pub transaction: EncodedTransaction,
    pub meta: Option<UiTransactionStatusMeta>,
    pub block_time: Option<UnixTimestamp>,
    #[serde(skip_serializing)]
    pub slot: Option<Slot>,
    #[serde(skip_serializing)]
    pub decoded_transaction: Transaction,
    #[serde(skip_serializing)]
    pub prefix: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sigverify_status: Vec<CliSignatureVerificationStatus>,
}

impl QuietDisplay for CliTransaction {}
impl VerboseDisplay for CliTransaction {}

impl fmt::Display for CliTransaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln_transaction(
            f,
            &self.decoded_transaction,
            &self.meta,
            &self.prefix,
            if !self.sigverify_status.is_empty() {
                Some(&self.sigverify_status)
            } else {
                None
            },
            self.block_time,
        )
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliTransactionConfirmation {
    pub confirmation_status: Option<TransactionConfirmationStatus>,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub transaction: Option<CliTransaction>,
    #[serde(skip_serializing)]
    pub get_transaction_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<TransactionError>,
}

impl QuietDisplay for CliTransactionConfirmation {}
impl VerboseDisplay for CliTransactionConfirmation {
    fn write_str(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result {
        if let Some(transaction) = &self.transaction {
            writeln!(
                w,
                "\nTransaction executed in slot {}:",
                transaction.slot.expect("slot should exist")
            )?;
            write!(w, "{}", transaction)?;
        } else if let Some(confirmation_status) = &self.confirmation_status {
            if confirmation_status != &TransactionConfirmationStatus::Finalized {
                writeln!(w)?;
                writeln!(
                    w,
                    "Unable to get finalized transaction details: not yet finalized"
                )?;
            } else if let Some(err) = &self.get_transaction_error {
                writeln!(w)?;
                writeln!(w, "Unable to get finalized transaction details: {}", err)?;
            }
        }
        writeln!(w)?;
        write!(w, "{}", self)
    }
}

impl fmt::Display for CliTransactionConfirmation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.confirmation_status {
            None => write!(f, "Not found"),
            Some(confirmation_status) => {
                if let Some(err) = &self.err {
                    write!(f, "Transaction failed: {}", err)
                } else {
                    write!(f, "{:?}", confirmation_status)
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CliGossipNode {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_label: Option<String>,
    pub identity_pubkey: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gossip_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tpu_port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc_host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

impl CliGossipNode {
    pub fn new(info: RpcContactInfo, labels: &HashMap<String, String>) -> Self {
        Self {
            ip_address: info.gossip.map(|addr| addr.ip().to_string()),
            identity_label: labels.get(&info.pubkey).cloned(),
            identity_pubkey: info.pubkey,
            gossip_port: info.gossip.map(|addr| addr.port()),
            tpu_port: info.tpu.map(|addr| addr.port()),
            rpc_host: info.rpc.map(|addr| addr.to_string()),
            version: info.version,
        }
    }
}

fn unwrap_to_string_or_none<T>(option: Option<T>) -> String
where
    T: std::string::ToString,
{
    unwrap_to_string_or_default(option, "none")
}

fn unwrap_to_string_or_default<T>(option: Option<T>, default: &str) -> String
where
    T: std::string::ToString,
{
    option
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or_else(|| default.to_string())
}

impl fmt::Display for CliGossipNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:15} | {:44} | {:6} | {:5} | {:21} | {}",
            unwrap_to_string_or_none(self.ip_address.as_ref()),
            self.identity_label
                .as_ref()
                .unwrap_or(&self.identity_pubkey),
            unwrap_to_string_or_none(self.gossip_port.as_ref()),
            unwrap_to_string_or_none(self.tpu_port.as_ref()),
            unwrap_to_string_or_none(self.rpc_host.as_ref()),
            unwrap_to_string_or_default(self.version.as_ref(), "unknown"),
        )
    }
}

impl QuietDisplay for CliGossipNode {}
impl VerboseDisplay for CliGossipNode {}

#[derive(Serialize, Deserialize)]
pub struct CliGossipNodes(pub Vec<CliGossipNode>);

impl fmt::Display for CliGossipNodes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "IP Address      | Node identifier                              \
             | Gossip | TPU   | RPC Address           | Version\n\
             ----------------+----------------------------------------------+\
             --------+-------+-----------------------+----------------",
        )?;
        for node in self.0.iter() {
            writeln!(f, "{}", node)?;
        }
        writeln!(f, "Nodes: {}", self.0.len())
    }
}

impl QuietDisplay for CliGossipNodes {}
impl VerboseDisplay for CliGossipNodes {}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{
        message::Message,
        pubkey::Pubkey,
        signature::{keypair_from_seed, NullSigner, Signature, Signer, SignerError},
        system_instruction,
        transaction::Transaction,
    };

    #[test]
    fn test_return_signers() {
        struct BadSigner {
            pubkey: Pubkey,
        }

        impl BadSigner {
            pub fn new(pubkey: Pubkey) -> Self {
                Self { pubkey }
            }
        }

        impl Signer for BadSigner {
            fn try_pubkey(&self) -> Result<Pubkey, SignerError> {
                Ok(self.pubkey)
            }

            fn try_sign_message(&self, _message: &[u8]) -> Result<Signature, SignerError> {
                Ok(Signature::new(&[1u8; 64]))
            }

            fn is_interactive(&self) -> bool {
                false
            }
        }

        let present: Box<dyn Signer> = Box::new(keypair_from_seed(&[2u8; 32]).unwrap());
        let absent: Box<dyn Signer> = Box::new(NullSigner::new(&Pubkey::new(&[3u8; 32])));
        let bad: Box<dyn Signer> = Box::new(BadSigner::new(Pubkey::new(&[4u8; 32])));
        let to = Pubkey::new(&[5u8; 32]);
        let nonce = Pubkey::new(&[6u8; 32]);
        let from = present.pubkey();
        let fee_payer = absent.pubkey();
        let nonce_auth = bad.pubkey();
        let mut tx = Transaction::new_unsigned(Message::new_with_nonce(
            vec![system_instruction::transfer(&from, &to, 42)],
            Some(&fee_payer),
            &nonce,
            &nonce_auth,
        ));

        let signers = vec![present.as_ref(), absent.as_ref(), bad.as_ref()];
        let blockhash = Hash::new(&[7u8; 32]);
        tx.try_partial_sign(&signers, blockhash).unwrap();
        let res = return_signers(&tx, &OutputFormat::JsonCompact).unwrap();
        let sign_only = parse_sign_only_reply_string(&res);
        assert_eq!(sign_only.blockhash, blockhash);
        assert_eq!(sign_only.message, None);
        assert_eq!(sign_only.present_signers[0].0, present.pubkey());
        assert_eq!(sign_only.absent_signers[0], absent.pubkey());
        assert_eq!(sign_only.bad_signers[0], bad.pubkey());

        let expected_msg = "AwECBwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDgTl3Dqh9\
            F19Wo1Rmw0x+zMuNipG07jeiXfYPW4/Js5QEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE\
            BAQEBAYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBQUFBQUFBQUFBQUFBQUFBQUF\
            BQUFBQUFBQUFBQUFBQUGp9UXGSxWjuCKhF9z0peIzwNcMUWyGrNE2AYuqUAAAAAAAAAAAAAA\
            AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcH\
            BwcCBgMDBQIEBAAAAAYCAQQMAgAAACoAAAAAAAAA"
            .to_string();
        let config = ReturnSignersConfig {
            dump_transaction_message: true,
        };
        let res = return_signers_with_config(&tx, &OutputFormat::JsonCompact, &config).unwrap();
        let sign_only = parse_sign_only_reply_string(&res);
        assert_eq!(sign_only.blockhash, blockhash);
        assert_eq!(sign_only.message, Some(expected_msg));
        assert_eq!(sign_only.present_signers[0].0, present.pubkey());
        assert_eq!(sign_only.absent_signers[0], absent.pubkey());
        assert_eq!(sign_only.bad_signers[0], bad.pubkey());
    }

    #[test]
    fn test_verbose_quiet_output_formats() {
        #[derive(Deserialize, Serialize)]
        struct FallbackToDisplay {}
        impl std::fmt::Display for FallbackToDisplay {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "display")
            }
        }
        impl QuietDisplay for FallbackToDisplay {}
        impl VerboseDisplay for FallbackToDisplay {}

        let f = FallbackToDisplay {};
        assert_eq!(&OutputFormat::Display.formatted_string(&f), "display");
        assert_eq!(&OutputFormat::DisplayQuiet.formatted_string(&f), "display");
        assert_eq!(
            &OutputFormat::DisplayVerbose.formatted_string(&f),
            "display"
        );

        #[derive(Deserialize, Serialize)]
        struct DiscreteVerbosityDisplay {}
        impl std::fmt::Display for DiscreteVerbosityDisplay {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "display")
            }
        }
        impl QuietDisplay for DiscreteVerbosityDisplay {
            fn write_str(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result {
                write!(w, "quiet")
            }
        }
        impl VerboseDisplay for DiscreteVerbosityDisplay {
            fn write_str(&self, w: &mut dyn std::fmt::Write) -> std::fmt::Result {
                write!(w, "verbose")
            }
        }

        let f = DiscreteVerbosityDisplay {};
        assert_eq!(&OutputFormat::Display.formatted_string(&f), "display");
        assert_eq!(&OutputFormat::DisplayQuiet.formatted_string(&f), "quiet");
        assert_eq!(
            &OutputFormat::DisplayVerbose.formatted_string(&f),
            "verbose"
        );
    }
}
