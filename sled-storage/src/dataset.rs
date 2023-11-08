// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! ZFS dataset related functionality

use crate::keyfile::KeyFile;
use camino::Utf8PathBuf;
use cfg_if::cfg_if;
use illumos_utils::zfs::{
    self, DestroyDatasetErrorVariant, EncryptionDetails, Keypath, Mountpoint,
    SizeDetails, Zfs,
};
use illumos_utils::zpool::ZpoolName;
use key_manager::StorageKeyRequester;
use omicron_common::disk::DiskIdentity;
use rand::distributions::{Alphanumeric, DistString};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sled_hardware::DiskVariant;
use slog::{info, Logger};
use std::sync::OnceLock;

pub const INSTALL_DATASET: &'static str = "install";
pub const CRASH_DATASET: &'static str = "crash";
pub const CLUSTER_DATASET: &'static str = "cluster";
pub const CONFIG_DATASET: &'static str = "config";
pub const M2_DEBUG_DATASET: &'static str = "debug";
pub const M2_BACKING_DATASET: &'static str = "backing";

cfg_if! {
    if #[cfg(any(test, feature = "testing"))] {
        // Tuned for zone_bundle tests
        pub const DEBUG_DATASET_QUOTA: usize = 100 * (1 << 10);
    } else {
        // TODO-correctness: This value of 100GiB is a pretty wild guess, and should be
        // tuned as needed.
        pub const DEBUG_DATASET_QUOTA: usize = 100 * (1 << 30);
    }
}
// TODO-correctness: This value of 100GiB is a pretty wild guess, and should be
// tuned as needed.
pub const DUMP_DATASET_QUOTA: usize = 100 * (1 << 30);
// passed to zfs create -o compression=
pub const DUMP_DATASET_COMPRESSION: &'static str = "gzip-9";

// U.2 datasets live under the encrypted dataset and inherit encryption
pub const ZONE_DATASET: &'static str = "crypt/zone";
pub const DUMP_DATASET: &'static str = "crypt/debug";
pub const U2_DEBUG_DATASET: &'static str = "crypt/debug";

// This is the root dataset for all U.2 drives. Encryption is inherited.
pub const CRYPT_DATASET: &'static str = "crypt";

const U2_EXPECTED_DATASET_COUNT: usize = 2;
static U2_EXPECTED_DATASETS: [ExpectedDataset; U2_EXPECTED_DATASET_COUNT] = [
    // Stores filesystems for zones
    ExpectedDataset::new(ZONE_DATASET).wipe(),
    // For storing full kernel RAM dumps
    ExpectedDataset::new(DUMP_DATASET)
        .quota(DUMP_DATASET_QUOTA)
        .compression(DUMP_DATASET_COMPRESSION),
];

const M2_EXPECTED_DATASET_COUNT: usize = 6;
static M2_EXPECTED_DATASETS: [ExpectedDataset; M2_EXPECTED_DATASET_COUNT] = [
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
];

// Helper type for describing expected datasets and their optional quota.
#[derive(Clone, Copy, Debug)]
struct ExpectedDataset {
    // Name for the dataset
    name: &'static str,
    // Optional quota, in _bytes_
    quota: Option<usize>,
    // Identifies if the dataset should be deleted on boot
    wipe: bool,
    // Optional compression mode
    compression: Option<&'static str>,
}

impl ExpectedDataset {
    const fn new(name: &'static str) -> Self {
        ExpectedDataset { name, quota: None, wipe: false, compression: None }
    }

    const fn quota(mut self, quota: usize) -> Self {
        self.quota = Some(quota);
        self
    }

    const fn wipe(mut self) -> Self {
        self.wipe = true;
        self
    }

    const fn compression(mut self, compression: &'static str) -> Self {
        self.compression = Some(compression);
        self
    }
}

/// The type of a dataset, and an auxiliary information necessary
/// to successfully launch a zone managing the associated data.
#[derive(
    Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash,
)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DatasetKind {
    CockroachDb,
    Crucible,
    Clickhouse,
    ClickhouseKeeper,
    ExternalDns,
    InternalDns,
}

impl std::fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use DatasetKind::*;
        let s = match self {
            Crucible => "crucible",
            CockroachDb { .. } => "cockroachdb",
            Clickhouse => "clickhouse",
            ClickhouseKeeper => "clickhouse_keeper",
            ExternalDns { .. } => "external_dns",
            InternalDns { .. } => "internal_dns",
        };
        write!(f, "{}", s)
    }
}

#[derive(
    Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, JsonSchema,
)]
pub struct DatasetName {
    // A unique identifier for the Zpool on which the dataset is stored.
    pool_name: ZpoolName,
    // A name for the dataset within the Zpool.
    kind: DatasetKind,
}

impl DatasetName {
    pub fn new(pool_name: ZpoolName, kind: DatasetKind) -> Self {
        Self { pool_name, kind }
    }

    pub fn pool(&self) -> &ZpoolName {
        &self.pool_name
    }

    pub fn dataset(&self) -> &DatasetKind {
        &self.kind
    }

    pub fn full(&self) -> String {
        format!("{}/{}", self.pool_name, self.kind)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DatasetError {
    #[error("Cannot open {path} due to {error}")]
    IoError { path: Utf8PathBuf, error: std::io::Error },
    #[error(transparent)]
    DestroyFilesystem(#[from] illumos_utils::zfs::DestroyDatasetError),
    #[error(transparent)]
    EnsureFilesystem(#[from] illumos_utils::zfs::EnsureFilesystemError),
    #[error("KeyManager error: {0}")]
    KeyManager(#[from] key_manager::Error),
    #[error("Missing StorageKeyRequester when creating U.2 disk")]
    MissingStorageKeyRequester,
    #[error("Encrypted filesystem '{0}' missing 'oxide:epoch' property")]
    CannotParseEpochProperty(String),
    #[error("Encrypted dataset '{dataset}' cannot set 'oxide:agent' property: {err}")]
    CannotSetAgentProperty {
        dataset: String,
        #[source]
        err: Box<zfs::SetValueError>,
    },
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
    zpool_name: &ZpoolName,
    disk_identity: &DiskIdentity,
    key_requester: Option<&StorageKeyRequester>,
) -> Result<(), DatasetError> {
    let (root, datasets) = match zpool_name.kind().into() {
        DiskVariant::M2 => (None, M2_EXPECTED_DATASETS.iter()),
        DiskVariant::U2 => (Some(CRYPT_DATASET), U2_EXPECTED_DATASETS.iter()),
    };

    let zoned = false;
    let do_format = true;

    // Ensure the root encrypted filesystem exists
    // Datasets below this in the hierarchy will inherit encryption
    if let Some(dataset) = root {
        let Some(key_requester) = key_requester else {
            return Err(DatasetError::MissingStorageKeyRequester);
        };
        let mountpoint = zpool_name.dataset_mountpoint(dataset);
        let keypath: Keypath = disk_identity.into();

        let epoch = if let Ok(epoch_str) =
            Zfs::get_oxide_value(dataset, "epoch")
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
            info!(log, "Loading latest secret"; "disk_id"=>#?disk_identity);
            let epoch = key_requester.load_latest_secret().await?;
            info!(log, "Loaded latest secret"; "epoch"=>%epoch, "disk_id"=>#?disk_identity);
            epoch
        };

        info!(log, "Retrieving key"; "epoch"=>%epoch, "disk_id"=>#?disk_identity);
        let key = key_requester.get_key(epoch, disk_identity.clone()).await?;
        info!(log, "Got key"; "epoch"=>%epoch, "disk_id"=>#?disk_identity);

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
        let result = Zfs::ensure_filesystem(
            &format!("{}/{}", zpool_name, dataset),
            Mountpoint::Path(mountpoint),
            zoned,
            do_format,
            Some(encryption_details),
            None,
            None,
        );

        keyfile.zero_and_unlink().await.map_err(|error| {
            DatasetError::IoError { path: keyfile.path().0.clone(), error }
        })?;

        result?;
    };

    for dataset in datasets.into_iter() {
        let mountpoint = zpool_name.dataset_mountpoint(dataset.name);
        let name = &format!("{}/{}", zpool_name, dataset.name);

        // Use a value that's alive for the duration of this sled agent
        // to answer the question: should we wipe this disk, or have
        // we seen it before?
        //
        // If this value comes from a prior iteration of the sled agent,
        // we opt to remove the corresponding dataset.
        static AGENT_LOCAL_VALUE: OnceLock<String> = OnceLock::new();
        let agent_local_value = AGENT_LOCAL_VALUE.get_or_init(|| {
            Alphanumeric.sample_string(&mut rand::thread_rng(), 20)
        });

        if dataset.wipe {
            match Zfs::get_oxide_value(name, "agent") {
                Ok(v) if &v == agent_local_value => {
                    info!(log, "Skipping automatic wipe for dataset: {}", name);
                }
                Ok(_) | Err(_) => {
                    info!(log, "Automatically destroying dataset: {}", name);
                    Zfs::destroy_dataset(name).or_else(|err| {
                        // If we can't find the dataset, that's fine -- it might
                        // not have been formatted yet.
                        if let DestroyDatasetErrorVariant::NotFound = err.err {
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
            compression: dataset.compression,
        });
        Zfs::ensure_filesystem(
            name,
            Mountpoint::Path(mountpoint),
            zoned,
            do_format,
            encryption_details,
            size_details,
            None,
        )?;

        if dataset.wipe {
            Zfs::set_oxide_value(name, "agent", agent_local_value).map_err(
                |err| DatasetError::CannotSetAgentProperty {
                    dataset: name.clone(),
                    err: Box::new(err),
                },
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn serialize_dataset_name() {
        let pool = ZpoolName::new_internal(Uuid::new_v4());
        let kind = DatasetKind::Crucible;
        let name = DatasetName::new(pool, kind);
        serde_json::to_string(&name).unwrap();
    }
}
