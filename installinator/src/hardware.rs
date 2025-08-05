// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::ensure;
use omicron_common::disk::DiskVariant;
use sled_hardware::HardwareManager;
use sled_hardware::SledMode;
use sled_storage::config::MountConfig;
use sled_storage::disk::Disk;
use sled_storage::disk::RawDisk;
use slog::Logger;
use slog::info;

pub struct Hardware {
    m2_disks: Vec<Disk>,
}

impl Hardware {
    pub async fn scan(log: &Logger) -> Result<Self> {
        let is_oxide_sled = sled_hardware::is_oxide_sled()
            .context("failed to detect whether host is an oxide sled")?;
        ensure!(is_oxide_sled, "hardware scan only supported on oxide sleds");

        let hardware = HardwareManager::new(log, SledMode::Auto, vec![])
            .map_err(|err| {
                anyhow!("failed to create HardwareManager: {err}")
            })?;

        let disks: Vec<RawDisk> =
            hardware.disks().into_values().map(|disk| disk.into()).collect();

        info!(
            log, "found oxide sled hardware";
            "baseboard" => ?hardware.baseboard(),
            "is_scrimlet" => hardware.is_scrimlet(),
            "num_disks" => disks.len(),
        );

        let mut m2_disks = vec![];
        for disk in disks {
            match disk.variant() {
                DiskVariant::U2 => {
                    info!(
                        log, "ignoring U.2 disk";
                        "path" => disk.devfs_path().as_str(),
                    );
                }
                DiskVariant::M2 => {
                    let disk = Disk::new(
                        log,
                        &MountConfig::default(),
                        disk,
                        None,
                        None,
                    )
                    .await
                    .context("failed to instantiate Disk handle for M.2")?;
                    m2_disks.push(disk);
                }
            }
        }

        Ok(Self { m2_disks })
    }

    pub fn m2_disks(&self) -> &[Disk] {
        &self.m2_disks
    }
}
