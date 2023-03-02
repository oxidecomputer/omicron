// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::anyhow;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use sled_hardware::Disk;
use sled_hardware::DiskVariant;
use sled_hardware::HardwareManager;
use slog::info;
use slog::Logger;

pub struct Hardware {
    m2_disks: Vec<Disk>,
}

impl Hardware {
    pub fn scan(log: &Logger) -> Result<Self> {
        let is_gimlet = sled_hardware::is_gimlet()
            .context("failed to detect whether host is a gimlet")?;
        ensure!(is_gimlet, "hardware scan only supported on gimlets");

        let hardware = HardwareManager::new(log, None).map_err(|err| {
            anyhow!("failed to create HardwareManager: {err}")
        })?;

        let disks = hardware.disks();

        info!(
            log, "found gimlet hardware";
            "baseboard" => ?hardware.baseboard(),
            "is_scrimlet" => hardware.is_scrimlet(),
            "num_disks" => disks.len(),
        );

        let m2_disks = disks
            .into_iter()
            .filter_map(|disk| {
                // Skip U.2 disks
                match disk.variant() {
                    DiskVariant::U2 => {
                        info!(
                            log, "ignoring U.2 disk";
                            "path" => disk.devfs_path().display(),
                        );
                        return None;
                    }
                    DiskVariant::M2 => (),
                }

                Some(
                    Disk::new(log, disk)
                        .context("failed to instantiate Disk handle for M.2"),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { m2_disks })
    }

    pub fn m2_disks(&self) -> &[Disk] {
        &self.m2_disks
    }
}
