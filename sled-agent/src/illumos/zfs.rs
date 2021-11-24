// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for poking at ZFS.

use crate::illumos::{execute, PFEXEC};
use omicron_common::api::external::Error;

pub const ZONE_ZFS_DATASET_MOUNTPOINT: &str = "/zone";
pub const ZONE_ZFS_DATASET: &str = "rpool/zone";
const ZFS: &str = "/usr/sbin/zfs";

/// Wraps commands for interacting with ZFS.
pub struct Zfs {}

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Zfs {
    /// Creates a new ZFS filesystem named `name`, unless one already exists.
    pub fn ensure_dataset(name: &str) -> Result<(), Error> {
        // If the dataset exists, we're done.
        let mut command = std::process::Command::new(ZFS);
        let cmd = command.args(&["list", "-Hpo", "name,type,mountpoint", name]);

        // If the list command returns any valid output, validate it.
        if let Ok(output) = execute(cmd) {
            let stdout = String::from_utf8(output.stdout).map_err(|e| {
                Error::InternalError {
                    internal_message: format!(
                        "Cannot parse 'zfs list' output as UTF-8: {}",
                        e
                    ),
                }
            })?;
            let values: Vec<&str> = stdout.trim().split('\t').collect();
            if values != &[name, "filesystem", ZONE_ZFS_DATASET_MOUNTPOINT] {
                return Err(Error::InternalError {
                    internal_message: format!(
                        "{} exists, but has unexpected values: {:?}",
                        name, values
                    ),
                });
            }
            return Ok(());
        }

        // If it doesn't exist, make it.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZFS,
            "create",
            "-o",
            &format!("mountpoint={}", ZONE_ZFS_DATASET_MOUNTPOINT),
            name,
        ]);
        execute(cmd)?;
        Ok(())
    }
}
