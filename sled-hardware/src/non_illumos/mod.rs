// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::{
    Baseboard, DiskError, DiskPaths, DiskVariant, Partition, ScrimletMode,
    UnparsedDisk,
};
use slog::Logger;
use std::collections::HashSet;
use tokio::sync::broadcast;

/// An unimplemented, stub representation of the underlying hardware.
///
/// This is intended for non-illumos systems to have roughly the same interface
/// as illumos systems - it allows compilation to "work" on non-illumos
/// platforms, which can be handy for editor support.
///
/// If you're actually trying to run the Sled Agent on non-illumos platforms,
/// use the simulated sled agent, which does not attempt to abstract hardware.
pub struct HardwareManager {}

impl HardwareManager {
    pub fn new(
        _log: Logger,
        _scrimlet_override: Option<ScrimletMode>,
    ) -> Result<Self, String> {
        unimplemented!("Accessing hardware unsupported on non-illumos");
    }

    pub fn baseboard(&self) -> Baseboard {
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
) -> Result<Vec<Partition>, DiskError> {
    unimplemented!("Accessing hardware unsupported on non-illumos");
}

/// Return true if the host system is an Oxide Gimlet.
pub fn is_gimlet() -> anyhow::Result<bool> {
    Ok(false)
}
