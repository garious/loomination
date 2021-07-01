use crate::snapshot_utils::{ArchiveFormat, SnapshotVersion};
use crate::{accounts_db::SnapshotStorages, bank::BankSlotDelta};
use solana_sdk::clock::Slot;
use solana_sdk::genesis_config::ClusterType;
use solana_sdk::hash::Hash;
use std::{
    path::PathBuf,
    sync::mpsc::{Receiver, SendError, Sender},
};
use tempfile::TempDir;

pub type AccountsPackageSender = Sender<AccountsPackagePre>;
pub type AccountsPackageReceiver = Receiver<AccountsPackagePre>;
pub type AccountsPackageSendError = SendError<AccountsPackagePre>;

#[derive(Debug)]
pub struct AccountsPackagePre {
    pub slot: Slot,
    pub block_height: Slot,
    pub slot_deltas: Vec<BankSlotDelta>,
    pub snapshot_links: TempDir,
    pub storages: SnapshotStorages,
    pub hash: Hash, // temporarily here while we still have to calculate hash before serializing bank
    pub archive_format: ArchiveFormat,
    pub snapshot_version: SnapshotVersion,
    pub snapshot_output_dir: PathBuf,
    pub expected_capitalization: u64,
    pub hash_for_testing: Option<Hash>,
    pub cluster_type: ClusterType,
}

impl AccountsPackagePre {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        slot: Slot,
        block_height: u64,
        slot_deltas: Vec<BankSlotDelta>,
        snapshot_links: TempDir,
        storages: SnapshotStorages,
        hash: Hash,
        archive_format: ArchiveFormat,
        snapshot_version: SnapshotVersion,
        snapshot_output_dir: PathBuf,
        expected_capitalization: u64,
        hash_for_testing: Option<Hash>,
        cluster_type: ClusterType,
    ) -> Self {
        Self {
            slot,
            block_height,
            slot_deltas,
            snapshot_links,
            storages,
            hash,
            archive_format,
            snapshot_version,
            snapshot_output_dir,
            expected_capitalization,
            hash_for_testing,
            cluster_type,
        }
    }
}

pub struct AccountsPackage {
    pub slot: Slot,
    pub block_height: Slot,
    pub slot_deltas: Vec<BankSlotDelta>,
    pub snapshot_links: TempDir,
    pub storages: SnapshotStorages,
    pub tar_output_file: PathBuf,
    pub hash: Hash,
    pub archive_format: ArchiveFormat,
    pub snapshot_version: SnapshotVersion,
}

impl AccountsPackage {
    pub fn new(
        slot: Slot,
        block_height: u64,
        slot_deltas: Vec<BankSlotDelta>,
        snapshot_links: TempDir,
        storages: SnapshotStorages,
        tar_output_file: PathBuf,
        hash: Hash,
        archive_format: ArchiveFormat,
        snapshot_version: SnapshotVersion,
    ) -> Self {
        Self {
            slot,
            block_height,
            slot_deltas,
            snapshot_links,
            storages,
            tar_output_file,
            hash,
            archive_format,
            snapshot_version,
        }
    }
}
