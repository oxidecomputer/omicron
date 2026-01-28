// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SledMode;
use crate::disk::{DiskPaths, Partition, PooledDiskError, UnparsedDisk};
use omicron_common::disk::{DiskIdentity, DiskVariant};
use omicron_uuid_kinds::ZpoolUuid;
use sled_hardware_types::{Baseboard, SledCpuFamily};
use slog::Logger;
use std::collections::HashMap;
use tokio::sync::broadcast;

#[derive(Debug, thiserror::Error)]
pub enum NvmeFormattingError {
    #[error("NVMe formatting is unsupported on this platform")]
    UnsupportedPlatform,
}

/// An unimplemented, stub representation of the underlying hardware.
///
/// This is intended for non-illumos systems to have roughly the same interface
/// as illumos systems - it allows compilation to "work" on non-illumos
/// platforms, which can be handy for editor support.
///
/// If you're actually trying to run the Sled Agent on non-illumos platforms,
/// use the simulated sled agent, which does not attempt to abstract hardware.
#[derive(Clone)]
pub struct HardwareManager {}

impl HardwareManager {
    pub fn new(
        _log: &Logger,
        _sled_mode: SledMode,
        _nonsled_observed_disks: Vec<UnparsedDisk>,
    ) -> Result<Self, String> {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn baseboard(&self) -> Baseboard {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn cpu_family(&self) -> SledCpuFamily {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn online_processor_count(&self) -> u32 {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn usable_physical_pages(&self) -> u64 {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn usable_physical_ram_bytes(&self) -> u64 {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn disks(&self) -> HashMap<DiskIdentity, UnparsedDisk> {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn is_scrimlet(&self) -> bool {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn is_scrimlet_asic_available(&self) -> bool {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn monitor(&self) -> broadcast::Receiver<super::HardwareUpdate> {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }
}

pub async fn ensure_partition_layout(
    _log: &Logger,
    _paths: &DiskPaths,
    _variant: DiskVariant,
    _identity: &DiskIdentity,
    _zpool_id: Option<ZpoolUuid>,
) -> Result<Vec<Partition>, PooledDiskError> {
    unimplemented!("Accessing hardware unsupported on non-illumos");
}

/// Return true if the host system is an Oxide sled.
pub fn is_oxide_sled() -> anyhow::Result<bool> {
    Ok(false)
}
