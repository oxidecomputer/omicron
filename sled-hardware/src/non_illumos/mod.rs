// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::disk::{
    DiskPaths, DiskVariant, Partition, PooledDiskError, UnparsedDisk,
};
use crate::SledMode;
use omicron_common::disk::DiskIdentity;
use sled_hardware_types::Baseboard;
use slog::Logger;
use std::collections::HashSet;
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
    pub fn new(_log: &Logger, _sled_mode: SledMode) -> Result<Self, String> {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn baseboard(&self) -> Baseboard {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn online_processor_count(&self) -> u32 {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn usable_physical_ram_bytes(&self) -> u64 {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn disks(&self) -> HashSet<UnparsedDisk> {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn is_scrimlet(&self) -> bool {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn is_scrimlet_driver_loaded(&self) -> bool {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn monitor(&self) -> broadcast::Receiver<super::HardwareUpdate> {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }
}

pub fn ensure_partition_layout(
    _log: &Logger,
    _paths: &DiskPaths,
    _variant: DiskVariant,
    _identity: &DiskIdentity,
) -> Result<Vec<Partition>, PooledDiskError> {
    unimplemented!("Accessing hardware unsupported on non-illumos");
}

/// Return true if the host system is an Oxide Gimlet.
pub fn is_gimlet() -> anyhow::Result<bool> {
    Ok(false)
}
