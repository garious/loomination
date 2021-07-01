use crate::snapshot_runtime_info::SyncSnapshotRuntimeInfo;
use crate::snapshot_utils::ArchiveFormat;
use crate::snapshot_utils::SnapshotVersion;
use solana_sdk::clock::Slot;
use std::path::PathBuf;

/// Snapshot configuration and runtime information
#[derive(Clone, Debug)]
pub struct SnapshotConfig {
    /// Generate a new snapshot every this many slots
    pub snapshot_interval_slots: Slot,

    /// Where to store the latest packaged snapshot
    pub snapshot_package_output_path: PathBuf,

    /// Where to place the snapshots for recent slots
    pub snapshot_path: PathBuf,

    /// The archive format to use for snapshots
    pub archive_format: ArchiveFormat,

    /// Snapshot version to generate
    pub snapshot_version: SnapshotVersion,

    /// Maximum number of snapshots to retain
    pub maximum_snapshots_to_retain: usize,

    /// Shared snapshot runtime information
    pub snapshot_runtime_info: SyncSnapshotRuntimeInfo,
}
