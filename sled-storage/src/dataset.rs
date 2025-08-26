// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! ZFS dataset related functionality

use crate::config::MountConfig;
use crate::keyfile::KeyFile;
use camino::Utf8PathBuf;
use illumos_utils::zfs::{
    self, DestroyDatasetErrorVariant, EncryptionDetails, Keypath, Mountpoint,
    SizeDetails, Zfs,
};
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use omicron_common::api::external::ByteCount;
use omicron_common::api::internal::shared::DatasetKind;
use omicron_common::disk::{
    CompressionAlgorithm, DatasetName, DiskIdentity, DiskVariant, GzipLevel,
};
use rand::distr::{Alphanumeric, SampleString};
use slog::{Logger, debug, info, warn};
use slog_error_chain::InlineErrorChain;
use std::process::Stdio;
use std::str::FromStr;
use std::sync::OnceLock;

pub const INSTALL_DATASET: &'static str = "install";
pub const CRASH_DATASET: &'static str = "crash";
pub const CLUSTER_DATASET: &'static str = "cluster";
pub const CONFIG_DATASET: &'static str = "config";
pub const M2_DEBUG_DATASET: &'static str = "debug";
pub const M2_BACKING_DATASET: &'static str = "backing";
pub const M2_ARTIFACT_DATASET: &'static str = "update";

pub const DEBUG_DATASET_QUOTA: ByteCount =
    if cfg!(any(test, feature = "testing")) {
        // Tuned for zone_bundle tests
        ByteCount::from_mebibytes_u32(1)
    } else {
        // TODO-correctness: This value of 100GiB is a pretty wild guess, and
        // should be tuned as needed.
        ByteCount::from_gibibytes_u32(100)
    };
// TODO-correctness: This value of 100GiB is a pretty wild guess, and should be
// tuned as needed.
pub const DUMP_DATASET_QUOTA: ByteCount = ByteCount::from_gibibytes_u32(100);
// passed to zfs create -o compression=
pub const DUMP_DATASET_COMPRESSION: CompressionAlgorithm =
    CompressionAlgorithm::GzipN { level: GzipLevel::new::<9>() };
// TODO-correctness: This value of 20 GiB is a wild guess -- given TUF repo
// sizes as of Oct 2024, it would be capable of storing about 10 distinct system
// versions.
pub const ARTIFACT_DATASET_QUOTA: ByteCount = ByteCount::from_gibibytes_u32(20);

// U.2 datasets live under the encrypted dataset and inherit encryption
pub const ZONE_DATASET: &'static str = "crypt/zone";
pub const DUMP_DATASET: &'static str = "crypt/debug";
pub const U2_DEBUG_DATASET: &'static str = "crypt/debug";

// This is the root dataset for all U.2 drives. Encryption is inherited.
pub const CRYPT_DATASET: &'static str = "crypt";

pub const U2_EXPECTED_DATASET_COUNT: usize = 2;
pub const U2_EXPECTED_DATASETS: [ExpectedDataset; U2_EXPECTED_DATASET_COUNT] = [
    // Stores filesystems for zones
    ExpectedDataset::new(ZONE_DATASET).wipe(),
    // For storing full kernel RAM dumps
    ExpectedDataset::new(DUMP_DATASET)
        .quota(DUMP_DATASET_QUOTA)
        .compression(DUMP_DATASET_COMPRESSION),
];

const M2_EXPECTED_DATASET_COUNT: usize = 7;
const M2_EXPECTED_DATASETS: [ExpectedDataset; M2_EXPECTED_DATASET_COUNT] = [
    // Stores software images.
    //
    // Should be duplicated to both M.2s.
    ExpectedDataset::new(INSTALL_DATASET),
    // Stores crash dumps.
    ExpectedDataset::new(CRASH_DATASET),
    // Backing store for OS data that should be persisted across reboots.
    // Its children are selectively overlay mounted onto parts of the ramdisk
    // root.
    ExpectedDataset::new(M2_BACKING_DATASET),
    // Stores cluter configuration information.
    //
    // Should be duplicated to both M.2s.
    ExpectedDataset::new(CLUSTER_DATASET),
    // Stores configuration data, including:
    // - What services should be launched on this sled
    // - Information about how to initialize the Sled Agent
    // - (For scrimlets) RSS setup information
    //
    // Should be duplicated to both M.2s.
    ExpectedDataset::new(CONFIG_DATASET),
    // Store debugging data, such as service bundles.
    ExpectedDataset::new(M2_DEBUG_DATASET).quota(DEBUG_DATASET_QUOTA),
    // Stores software artifacts (zones, OS images, Hubris images, etc.)
    // extracted from TUF repos by Nexus.
    ExpectedDataset::new(M2_ARTIFACT_DATASET).quota(ARTIFACT_DATASET_QUOTA),
];

// Helper type for describing expected datasets and their optional quota.
#[derive(Clone, Copy, Debug)]
pub struct ExpectedDataset {
    // Name for the dataset
    name: &'static str,
    // Optional quota, in _bytes_
    quota: Option<ByteCount>,
    // Identifies if the dataset should be deleted on boot
    wipe: bool,
    // Optional compression mode
    compression: CompressionAlgorithm,
}

impl ExpectedDataset {
    const fn new(name: &'static str) -> Self {
        ExpectedDataset {
            name,
            quota: None,
            wipe: false,
            compression: CompressionAlgorithm::Off,
        }
    }

    pub fn get_name(&self) -> &'static str {
        self.name
    }

    pub fn get_quota(&self) -> Option<ByteCount> {
        self.quota
    }

    pub fn get_compression(&self) -> CompressionAlgorithm {
        self.compression
    }

    const fn quota(mut self, quota: ByteCount) -> Self {
        self.quota = Some(quota);
        self
    }

    const fn wipe(mut self) -> Self {
        self.wipe = true;
        self
    }

    const fn compression(mut self, compression: CompressionAlgorithm) -> Self {
        self.compression = compression;
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    #[error("Cannot open {path} due to {error}")]
    IoError { path: Utf8PathBuf, error: std::io::Error },
    #[error(transparent)]
    DestroyFilesystem(#[from] illumos_utils::zfs::DestroyDatasetError),
    #[error(transparent)]
    EnsureDataset(#[from] illumos_utils::zfs::EnsureDatasetError),
    #[error("KeyManager error: {0}")]
    KeyManager(#[from] key_manager::Error),
    #[error("Missing StorageKeyRequester when creating U.2 disk")]
    MissingStorageKeyRequester,
    #[error("Encrypted filesystem '{0}' missing 'oxide:epoch' property")]
    CannotParseEpochProperty(String),
    #[error(
        "Encrypted dataset '{dataset}' cannot set 'oxide:agent' property: {err}"
    )]
    CannotSetAgentProperty {
        dataset: String,
        #[source]
        err: Box<zfs::SetValueError>,
    },
    #[error("Failed to make datasets encrypted")]
    EncryptionMigration(#[from] DatasetEncryptionMigrationError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Ensure that the zpool contains all the datasets we would like it to
/// contain.
///
/// WARNING: In all cases where a U.2 is a possible `DiskVariant`, a
/// `StorageKeyRequester` must be passed so that disk encryption can
/// be used. The `StorageManager` for the sled-agent always has a
/// `StorageKeyRequester` available, and so the only place we should pass
/// `None` is for the M.2s touched by the Installinator.
pub(crate) async fn ensure_zpool_has_datasets(
    log: &Logger,
    mount_config: &MountConfig,
    zpool_name: &ZpoolName,
    disk_identity: &DiskIdentity,
    key_requester: Option<&StorageKeyRequester>,
) -> Result<(), DatasetError> {
    info!(log, "Ensuring zpool has datasets"; "zpool" => ?zpool_name, "disk_identity" => ?disk_identity);
    let (root, datasets) = match zpool_name.kind().into() {
        DiskVariant::M2 => (None, M2_EXPECTED_DATASETS.iter()),
        DiskVariant::U2 => (Some(CRYPT_DATASET), U2_EXPECTED_DATASETS.iter()),
    };

    let zoned = false;

    // Ensure the root encrypted filesystem exists
    // Datasets below this in the hierarchy will inherit encryption
    if let Some(dataset) = root {
        let Some(key_requester) = key_requester else {
            return Err(DatasetError::MissingStorageKeyRequester);
        };
        let mountpoint =
            zpool_name.dataset_mountpoint(&mount_config.root, dataset);
        let keypath: Keypath =
            illumos_utils::zfs::Keypath::new(disk_identity, &mount_config.root);

        let epoch = if let Ok(epoch_str) =
            Zfs::get_oxide_value(dataset, "epoch").await
        {
            if let Ok(epoch) = epoch_str.parse::<u64>() {
                epoch
            } else {
                return Err(DatasetError::CannotParseEpochProperty(
                    dataset.to_string(),
                ));
            }
        } else {
            // We got an error trying to call `Zfs::get_oxide_value`
            // which indicates that the dataset doesn't exist or there
            // was a problem  running the command.
            //
            // Note that `Zfs::get_oxide_value` will succeed even if
            // the epoch is missing. `epoch_str` will show up as a dash
            // (`-`) and will not parse into a `u64`. So we don't have
            // to worry about that case here as it is handled above.
            //
            // If the error indicated that the command failed for some
            // other reason, but the dataset actually existed, we will
            // try to create the dataset below and that will fail. So
            // there is no harm in just loading the latest secret here.
            info!(log, "Loading latest secret"; "disk_id"=>?disk_identity);
            let epoch = key_requester.load_latest_secret().await?;
            info!(log, "Loaded latest secret"; "epoch"=>%epoch, "disk_id"=>?disk_identity);
            epoch
        };

        info!(log, "Retrieving key"; "epoch"=>%epoch, "disk_id"=>?disk_identity);
        let key = key_requester.get_key(epoch, disk_identity.clone()).await?;
        info!(log, "Got key"; "epoch"=>%epoch, "disk_id"=>?disk_identity);

        let mut keyfile =
            KeyFile::create(keypath.clone(), key.expose_secret(), log)
                .await
                .map_err(|error| DatasetError::IoError {
                    path: keypath.0.clone(),
                    error,
                })?;

        let encryption_details = EncryptionDetails { keypath, epoch };

        info!(
            log,
            "Ensuring encrypted filesystem: {} for epoch {}", dataset, epoch
        );
        let name = format!("{}/{}", zpool_name, dataset);
        let result = Zfs::ensure_dataset(zfs::DatasetEnsureArgs {
            name: &name,
            mountpoint: Mountpoint(mountpoint),
            can_mount: zfs::CanMount::On,
            zoned,
            encryption_details: Some(encryption_details),
            size_details: None,
            id: None,
            additional_options: None,
        })
        .await
        .inspect_err(|err| {
            warn!(
                log,
                "Failed to ensure encrypted root filesystem";
                "name" => ?name,
                "err" => InlineErrorChain::new(&err),
            );
        });

        keyfile.zero_and_unlink().await.map_err(|error| {
            DatasetError::IoError { path: keyfile.path().0.clone(), error }
        })?;

        result?;
    };

    for dataset in datasets.into_iter() {
        let mountpoint =
            zpool_name.dataset_mountpoint(&mount_config.root, dataset.name);
        let name = &format!("{}/{}", zpool_name, dataset.name);

        // Use a value that's alive for the duration of this sled agent
        // to answer the question: should we wipe this disk, or have
        // we seen it before?
        //
        // If this value comes from a prior iteration of the sled agent,
        // we opt to remove the corresponding dataset.
        static AGENT_LOCAL_VALUE: OnceLock<String> = OnceLock::new();
        let agent_local_value = AGENT_LOCAL_VALUE
            .get_or_init(|| Alphanumeric.sample_string(&mut rand::rng(), 20));

        if dataset.wipe {
            match Zfs::get_oxide_value(name, "agent").await {
                Ok(v) if &v == agent_local_value => {
                    info!(log, "Skipping automatic wipe for dataset: {}", name);
                }
                Ok(_) | Err(_) => {
                    info!(log, "Automatically destroying dataset: {}", name);
                    Zfs::destroy_dataset(name).await.or_else(|err| {
                        // If we can't find the dataset, that's fine -- it might
                        // not have been formatted yet.
                        if matches!(
                            err.err,
                            DestroyDatasetErrorVariant::NotFound
                        ) {
                            Ok(())
                        } else {
                            Err(err)
                        }
                    })?;
                }
            }
        }

        let encryption_details = None;
        let size_details = Some(SizeDetails {
            quota: dataset.quota,
            reservation: None,
            compression: dataset.compression,
        });
        Zfs::ensure_dataset(zfs::DatasetEnsureArgs {
            name,
            mountpoint: Mountpoint(mountpoint),
            can_mount: zfs::CanMount::On,
            zoned,
            encryption_details,
            size_details,
            id: None,
            additional_options: None,
        })
        .await
        .inspect_err(|err| {
            warn!(
                log,
                "Failed to ensure dataset";
                "name" => ?name,
                "err" => InlineErrorChain::new(&err),
            );
        })?;

        if dataset.wipe {
            Zfs::set_oxide_value(name, "agent", agent_local_value)
                .await
                .map_err(|err| DatasetError::CannotSetAgentProperty {
                    dataset: name.clone(),
                    err: Box::new(err),
                })?;
        }
    }
    info!(log, "Finished ensuring zpool has datasets"; "zpool" => ?zpool_name, "disk_identity" => ?disk_identity);
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetEncryptionMigrationError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("Failed to run command")]
    FailedCommand { command: String, stderr: Option<String> },

    #[error("Cannot create new encrypted dataset")]
    DatasetCreation(#[from] illumos_utils::zfs::EnsureDatasetError),

    #[error("Missing stdout stream during 'zfs send' command")]
    MissingStdoutForZfsSend,
}

fn status_ok_or_get_stderr(
    command: &tokio::process::Command,
    output: &std::process::Output,
) -> Result<(), DatasetEncryptionMigrationError> {
    if !output.status.success() {
        let stdcmd = command.as_std();
        return Err(DatasetEncryptionMigrationError::FailedCommand {
            command: format!(
                "{:?} {:?}",
                stdcmd.get_program(),
                stdcmd
                    .get_args()
                    .collect::<Vec<_>>()
                    .join(std::ffi::OsStr::new(" "))
            ),
            stderr: Some(String::from_utf8_lossy(&output.stderr).to_string()),
        });
    }
    Ok(())
}

/// Migrates unencrypted datasets to their encrypted formats.
pub(crate) async fn ensure_zpool_datasets_are_encrypted(
    log: &Logger,
    zpool_name: &ZpoolName,
) -> Result<(), DatasetEncryptionMigrationError> {
    info!(log, "Looking for unencrypted datasets in {zpool_name}");
    let unencrypted_datasets =
        find_all_unencrypted_datasets_directly_within_pool(&log, &zpool_name)
            .await?;

    // TODO: Could do this in parallel?
    for dataset in unencrypted_datasets {
        let log = &log.new(slog::o!("dataset" => dataset.clone()));
        info!(log, "Found unencrypted dataset");

        ensure_zpool_dataset_is_encrypted(&log, &zpool_name, &dataset).await?;
    }
    Ok(())
}

async fn find_all_unencrypted_datasets_directly_within_pool(
    log: &Logger,
    zpool_name: &ZpoolName,
) -> Result<Vec<String>, DatasetEncryptionMigrationError> {
    let mut command = tokio::process::Command::new(illumos_utils::zfs::ZFS);
    let pool_name = zpool_name.to_string();
    let cmd = command.args(&[
        "list",
        "-rHo",
        "name,encryption",
        "-d",
        "1",
        &pool_name,
    ]);
    let output = cmd.output().await?;
    status_ok_or_get_stderr(&cmd, &output)?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines = stdout.trim().split('\n');

    let mut unencrypted_datasets = vec![];
    for line in lines {
        let mut iter = line.split_whitespace();
        let Some(dataset) = iter.next() else {
            continue;
        };
        let log = log.new(slog::o!("dataset" => dataset.to_string()));

        let Some(encryption) = iter.next() else {
            continue;
        };

        // We don't bother checking HOW the dataset is encrypted, just that it
        // IS encrypted somehow. The sled agent is slightly more opinionated, as
        // it looks for "aes-256-gcm" explicitly, but we currently don't plan on
        // providing support for migrating between encryption schemes
        // automatically.
        let encrypted = match encryption {
            "off" | "-" => false,
            _ => true,
        };
        if encrypted {
            debug!(log, "Found dataset, but it is already encrypted");
            continue;
        }
        debug!(log, "Found dataset, and it isn't encrypted");
        if let Some(dataset) =
            dataset.strip_prefix(&format!("{pool_name}/")).map(String::from)
        {
            unencrypted_datasets.push(dataset);
        }
    }
    Ok(unencrypted_datasets)
}

// Precondition:
// - We found the dataset as a direct descendant of "zpool_name", which
// has encryption set to "off".
//
// "dataset" does not include the zpool prefix; format!("{zpool_name}/dataset")
// would be the full name of the unencrypted dataset.
async fn ensure_zpool_dataset_is_encrypted(
    log: &Logger,
    zpool_name: &ZpoolName,
    unencrypted_dataset: &str,
) -> Result<(), DatasetEncryptionMigrationError> {
    let Ok(kind) = DatasetKind::from_str(&unencrypted_dataset) else {
        info!(log, "Unrecognized dataset kind");
        return Ok(());
    };
    info!(log, "Dataset recognized");
    let unencrypted_dataset = format!("{zpool_name}/{unencrypted_dataset}");

    if !kind.dataset_should_be_encrypted() {
        info!(log, "Dataset should not be encrypted");
        return Ok(());
    }
    info!(log, "Dataset should be encrypted");

    let encrypted_dataset = DatasetName::new(*zpool_name, kind);
    let encrypted_dataset = encrypted_dataset.full_name();

    let (unencrypted_dataset_exists, encrypted_dataset_exists) = (
        dataset_exists(&unencrypted_dataset).await?,
        dataset_exists(&encrypted_dataset).await?,
    );

    match (unencrypted_dataset_exists, encrypted_dataset_exists) {
        (false, _) => {
            // In this case, there is no unencrypted dataset! Bail out, there is
            // nothing to transfer.
            return Ok(());
        }
        (true, true) => {
            // In this case, the following is true:
            // - An unencrypted dataset exists
            // - An encrypted dataset exists
            //
            // This is indicative of an incomplete transfer from "old" -> "new".
            // If we managed to create the encrypted dataset, and got far enough to
            // rename to it's "non-tmp" location, then pick up where we left off:
            // - Mark the encrypted dataset as usable
            // - Remove the unencrypted dataset
            info!(
                log,
                "Dataset already has encrypted variant, resuming migration"
            );
            return finalize_encryption_migration(
                &log,
                &encrypted_dataset,
                &unencrypted_dataset,
            )
            .await;
        }
        (true, false) => {
            // This is the "normal" transfer case, initially: We have an
            // unencrypted dataset that should become encrypted.
            info!(log, "Dataset has not yet been encrypted");
        }
    }

    let snapshot_name = |dataset: &str| format!("{dataset}@migration");

    // A snapshot taken to provide a point-in-time view of the dataset for
    // copying.
    let unencrypted_dataset_snapshot = snapshot_name(&unencrypted_dataset);
    // A "temporary" name for the encrypted target dataset.
    let encrypted_dataset_tmp = format!("{}-tmp", encrypted_dataset);
    // A snapshot that is automatically generated by "zfs receive".
    let encrypted_dataset_tmp_snapshot = snapshot_name(&encrypted_dataset_tmp);

    // Get rid of snapshots and temporary datasets.
    //
    // This removes work of any prior sled agents that might have failed halfway
    // through this operation.
    let _ = zfs_destroy(&unencrypted_dataset_snapshot).await;
    let _ = zfs_destroy(&encrypted_dataset_tmp).await;

    zfs_create_snapshot(&unencrypted_dataset_snapshot).await?;
    info!(log, "Encrypted dataset snapshotted");

    // Transfer to a "tmp" dataset that's encrypted, but not mountable.
    //
    // This makes it clear it's a "work-in-progress" dataset until the transfer
    // has fully completed.
    zfs_transfer_to_unmountable_dataset(
        &unencrypted_dataset_snapshot,
        &encrypted_dataset_tmp,
    )
    .await?;
    info!(log, "Dataset transferred to encrypted (temporary) location");

    zfs_destroy(&unencrypted_dataset_snapshot).await?;
    zfs_destroy(&encrypted_dataset_tmp_snapshot).await?;
    info!(log, "Removed snapshots");

    // We tragically cannot "zfs rename" any datasets with "zoned=on".
    //
    // We perform the rename first, then set "zoned=on" with "canmount=on".
    // This prevents the dataset from being used by zones until these properties
    // have finally been set.
    zfs_rename(&encrypted_dataset_tmp, &encrypted_dataset).await?;

    return finalize_encryption_migration(
        &log,
        &encrypted_dataset,
        &unencrypted_dataset,
    )
    .await;
}

// Returns true if the dataset exists.
async fn dataset_exists(
    dataset: &str,
) -> Result<bool, DatasetEncryptionMigrationError> {
    let mut command = tokio::process::Command::new(illumos_utils::zfs::ZFS);
    let cmd = command.args(&["list", "-H", dataset]);
    Ok(cmd.status().await?.success())
}

// Destroys the dataset and all children, recursively.
async fn zfs_destroy(
    dataset: &str,
) -> Result<(), DatasetEncryptionMigrationError> {
    let mut command = tokio::process::Command::new(illumos_utils::zfs::ZFS);
    let cmd = command.args(&["destroy", "-r", dataset]);
    let output = cmd.output().await?;
    status_ok_or_get_stderr(&cmd, &output)?;
    Ok(())
}

// Creates a snapshot named "dataset_snapshot".
async fn zfs_create_snapshot(
    dataset_snapshot: &str,
) -> Result<(), DatasetEncryptionMigrationError> {
    let mut command = tokio::process::Command::new(illumos_utils::zfs::ZFS);
    let cmd = command.args(&["snapshot", dataset_snapshot]);
    let output = cmd.output().await?;
    status_ok_or_get_stderr(&cmd, &output)?;
    Ok(())
}

// Uses "zfs send" and "zfs receive" to create an unmountable, unzoned dataset.
//
// These properties are set to allow the caller to continue manipulating the
// dataset (via rename, setting other properties, etc) before it's used.
async fn zfs_transfer_to_unmountable_dataset(
    from: &str,
    to: &str,
) -> Result<(), DatasetEncryptionMigrationError> {
    let mut command = tokio::process::Command::new(illumos_utils::zfs::ZFS);
    let sender_cmd = command
        .args(&["send", from])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut sender = sender_cmd.spawn()?;

    let Some(sender_stdout) = sender.stdout.take() else {
        return Err(DatasetEncryptionMigrationError::MissingStdoutForZfsSend);
    };
    let sender_stdout: Stdio = sender_stdout.try_into().map_err(|_| {
        DatasetEncryptionMigrationError::MissingStdoutForZfsSend
    })?;

    let mut command = tokio::process::Command::new(illumos_utils::zfs::ZFS);
    let receiver_cmd = command
        .args(&[
            "receive",
            "-o",
            "mountpoint=/data",
            "-o",
            "canmount=off",
            "-o",
            "zoned=off",
            to,
        ])
        .stdin(sender_stdout)
        .stderr(Stdio::piped());
    let receiver = receiver_cmd.spawn()?;

    let output = receiver.wait_with_output().await?;
    status_ok_or_get_stderr(&receiver_cmd, &output)?;
    let output = sender.wait_with_output().await?;
    status_ok_or_get_stderr(&sender_cmd, &output)?;

    Ok(())
}

// Sets "properties" on "dataset".
//
// Each member of "properties" should have the form "key=value".
async fn zfs_set(
    dataset: &str,
    properties: &[&str],
) -> Result<(), DatasetEncryptionMigrationError> {
    let mut command = tokio::process::Command::new(illumos_utils::zfs::ZFS);
    let cmd = command.arg("set");
    for property in properties {
        cmd.arg(property);
    }
    cmd.arg(dataset);

    let output = cmd.output().await?;
    status_ok_or_get_stderr(&cmd, &output)?;
    Ok(())
}

// Sets properties to make a dataset "ready to be used by zones".
async fn zfs_set_zoned_and_mountable(
    dataset: &str,
) -> Result<(), DatasetEncryptionMigrationError> {
    zfs_set(&dataset, &["zoned=on", "canmount=on"]).await
}

// Renames a dataset from "from" to "to".
async fn zfs_rename(
    from: &str,
    to: &str,
) -> Result<(), DatasetEncryptionMigrationError> {
    let mut command = tokio::process::Command::new(illumos_utils::zfs::ZFS);
    let cmd = command.args(&["rename", from, to]);
    let output = cmd.output().await?;
    status_ok_or_get_stderr(&cmd, &output)?;
    Ok(())
}

async fn finalize_encryption_migration(
    log: &Logger,
    encrypted_dataset: &str,
    unencrypted_dataset: &str,
) -> Result<(), DatasetEncryptionMigrationError> {
    zfs_set_zoned_and_mountable(&encrypted_dataset).await?;
    info!(log, "Dataset is encrypted, zoned, and mountable"; "dataset" => encrypted_dataset);

    zfs_destroy(&unencrypted_dataset).await?;
    info!(log, "Destroyed unencrypted dataset"; "dataset" => unencrypted_dataset);
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use omicron_uuid_kinds::InternalZpoolUuid;

    #[test]
    fn serialize_dataset_name() {
        let pool = ZpoolName::Internal(InternalZpoolUuid::new_v4());
        let kind = DatasetKind::Crucible;
        let name = DatasetName::new(pool, kind);
        serde_json::to_string(&name).unwrap();
    }
}
