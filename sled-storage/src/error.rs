// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Storage related errors

use crate::dataset::DatasetError;
use crate::disk::DiskError;
use camino::Utf8PathBuf;
use omicron_common::api::external::ByteCountRangeError;
use omicron_common::api::external::Generation;
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    DiskError(#[from] DiskError),

    #[error(transparent)]
    DatasetError(#[from] DatasetError),

    // TODO: We could add the context of "why are we doint this op", maybe?
    #[error(transparent)]
    ZfsListDataset(#[from] illumos_utils::zfs::ListDatasetsError),

    #[error(transparent)]
    ZfsEnsureFilesystem(#[from] illumos_utils::zfs::EnsureFilesystemError),

    #[error(transparent)]
    ZfsSetValue(#[from] illumos_utils::zfs::SetValueError),

    #[error(transparent)]
    ZfsGetValue(#[from] illumos_utils::zfs::GetValueError),

    #[error(transparent)]
    GetZpoolInfo(#[from] illumos_utils::zpool::GetInfoError),

    #[error(transparent)]
    Fstyp(#[from] illumos_utils::fstyp::Error),

    #[error(transparent)]
    ZoneCommand(#[from] illumos_utils::running_zone::RunCommandError),

    #[error(transparent)]
    ZoneBoot(#[from] illumos_utils::running_zone::BootError),

    #[error(transparent)]
    ZoneEnsureAddress(#[from] illumos_utils::running_zone::EnsureAddressError),

    #[error(transparent)]
    ZoneInstall(#[from] illumos_utils::running_zone::InstallZoneError),

    #[error("Failed to parse UUID from {path}: {err}")]
    ParseUuid {
        path: Utf8PathBuf,
        #[source]
        err: uuid::Error,
    },

    #[error("Dataset {name} exists with a different uuid (has {old}, requested {new})")]
    UuidMismatch { name: String, old: Uuid, new: Uuid },

    #[error("Error parsing pool {name}'s size: {err}")]
    BadPoolSize {
        name: String,
        #[source]
        err: ByteCountRangeError,
    },

    #[error("Failed to parse the dataset {name}'s UUID: {err}")]
    ParseDatasetUuid {
        name: String,
        #[source]
        err: uuid::Error,
    },

    #[error("Not ready to manage U.2s (key manager is not ready)")]
    KeyManagerNotReady,

    #[error("Physical disk configuration out-of-date (asked for {requested}, but latest is {current})")]
    PhysicalDiskConfigurationOutdated {
        requested: Generation,
        current: Generation,
    },

    #[error("Invalid configuration (UUID mismatch in arguments)")]
    ConfigUuidMismatch,

    #[error("Dataset configuration out-of-date (asked for {requested}, but latest is {current})")]
    DatasetConfigurationOutdated { requested: Generation, current: Generation },

    #[error("Dataset configuration changed for the same generation number: {generation}")]
    DatasetConfigurationChanged { generation: Generation },

    #[error("Failed to update ledger in internal storage")]
    Ledger(#[from] omicron_common::ledger::Error),

    #[error("No ledger found on internal storage")]
    LedgerNotFound,

    #[error("Zpool Not Found: {0}")]
    ZpoolNotFound(String),
}

impl From<Error> for omicron_common::api::external::Error {
    fn from(err: Error) -> Self {
        use omicron_common::api::external::Error as ExternalError;
        use omicron_common::api::external::LookupType;
        use omicron_common::api::external::ResourceType;

        match err {
            Error::LedgerNotFound => ExternalError::ObjectNotFound {
                type_name: ResourceType::SledLedger,
                lookup_type: LookupType::ByOther(
                    "Could not find record on M.2s".to_string(),
                ),
            },
            Error::ZpoolNotFound(name) => ExternalError::ObjectNotFound {
                type_name: ResourceType::Zpool,
                lookup_type: LookupType::ByName(name),
            },
            Error::KeyManagerNotReady => ExternalError::ServiceUnavailable {
                internal_message:
                    "Not ready to manage disks, try again after trust quorum"
                        .to_string(),
            },
            _ => omicron_common::api::external::Error::InternalError {
                internal_message: err.to_string(),
            },
        }
    }
}
