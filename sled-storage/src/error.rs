// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Storage related errors

use crate::dataset::{DatasetError, DatasetName};
use crate::disk::DiskError;
use camino::Utf8PathBuf;
use omicron_common::api::external::ByteCountRangeError;
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

    #[error("No U.2 Zpools found")]
    NoU2Zpool,

    #[error("Failed to parse UUID from {path}: {err}")]
    ParseUuid {
        path: Utf8PathBuf,
        #[source]
        err: uuid::Error,
    },

    #[error("Dataset {name:?} exists with a different uuid (has {old}, requested {new})")]
    UuidMismatch { name: Box<DatasetName>, old: Uuid, new: Uuid },

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

    #[error("Zpool Not Found: {0}")]
    ZpoolNotFound(String),
}
