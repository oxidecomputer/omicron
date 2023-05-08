// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::process::Command;

use anyhow::anyhow;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use sled_hardware::Disk;
use sled_hardware::DiskVariant;
use sled_hardware::HardwareManager;
use sled_hardware::SledMode;
use slog::info;
use slog::Logger;

const DISKINFO_PATH: &str = "/usr/bin/diskinfo";

pub struct Hardware {
    m2_disks: Vec<Disk>,
}

impl Hardware {
    pub async fn scan(log: &Logger) -> Result<Self> {
        let is_gimlet = sled_hardware::is_gimlet()
            .context("failed to detect whether host is a gimlet")?;
        ensure!(is_gimlet, "hardware scan only supported on gimlets");

        // Workaround https://github.com/oxidecomputer/stlouis/issues/395:
        // `Disk::new()` below expects the `...:wd,raw` whole disk minor node to
        // exist, but it doesn't always; running `diskinfo` ahead of time forces
        // it to show up.
        let status = Command::new(DISKINFO_PATH)
            .status()
            .with_context(|| format!("failed to run `{DISKINFO_PATH}`"))?;
        ensure!(status.success(), "{DISKINFO_PATH} failed: {status}");

        let hardware =
            HardwareManager::new(log, SledMode::Auto).map_err(|err| {
                anyhow!("failed to create HardwareManager: {err}")
            })?;

        let disks = hardware.disks();

        info!(
            log, "found gimlet hardware";
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
                    let disk = Disk::new(log, disk, None)
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
