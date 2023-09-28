// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Disk related types

use camino::Utf8PathBuf;
use illumos_utils::zpool::{ZpoolKind, ZpoolName};
use omicron_common::disk::DiskIdentity;
use sled_hardware::{Disk, DiskVariant};

/// A wrapper around real disks or synthetic disks backed by a file
#[derive(PartialEq, Eq, Clone)]
pub(crate) enum DiskWrapper {
    Real { disk: Disk, devfs_path: Utf8PathBuf },
    Synthetic { zpool_name: ZpoolName },
}

impl From<Disk> for DiskWrapper {
    fn from(disk: Disk) -> Self {
        let devfs_path = disk.devfs_path().clone();
        Self::Real { disk, devfs_path }
    }
}

impl DiskWrapper {
    fn identity(&self) -> DiskIdentity {
        match self {
            DiskWrapper::Real { disk, .. } => disk.identity().clone(),
            DiskWrapper::Synthetic { zpool_name } => {
                let id = zpool_name.id();
                DiskIdentity {
                    vendor: "synthetic-vendor".to_string(),
                    serial: format!("synthetic-serial-{id}"),
                    model: "synthetic-model".to_string(),
                }
            }
        }
    }

    fn variant(&self) -> DiskVariant {
        match self {
            DiskWrapper::Real { disk, .. } => disk.variant(),
            DiskWrapper::Synthetic { zpool_name } => match zpool_name.kind() {
                ZpoolKind::External => DiskVariant::U2,
                ZpoolKind::Internal => DiskVariant::M2,
            },
        }
    }

    fn zpool_name(&self) -> &ZpoolName {
        match self {
            DiskWrapper::Real { disk, .. } => disk.zpool_name(),
            DiskWrapper::Synthetic { zpool_name } => zpool_name,
        }
    }
}
