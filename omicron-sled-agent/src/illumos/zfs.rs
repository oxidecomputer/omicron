//! Utilities for poking at ZFS.

use crate::illumos::{execute, PFEXEC};
use omicron_common::api::ApiError;

pub const ZONE_ZFS_DATASET_MOUNTPOINT: &str = "/zone";
pub const ZONE_ZFS_DATASET: &str = "rpool/zone";
const ZFS: &str = "/usr/sbin/zfs";

/// Wraps commands for interacting with ZFS.
pub struct Zfs {}

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Zfs {
    /// Creates a new ZFS filesystem named `name`, unless one already exists.
    pub fn ensure_dataset(name: &str) -> Result<(), ApiError> {
        // If the dataset exists, we're done.
        let mut command = std::process::Command::new(ZFS);
        let cmd = command.args(&["list", "-Hpo", "name,type,mountpoint", name]);

        // If the list command returns any valid output, validate it.
        if let Ok(output) = execute(cmd) {
            let stdout = String::from_utf8(output.stdout).map_err(|e| {
                ApiError::InternalError {
                    message: format!(
                        "Cannot parse 'zfs list' output as UTF-8: {}",
                        e
                    ),
                }
            })?;
            let values: Vec<&str> = stdout.trim().split('\t').collect();
            if values != &[name, "filesystem", ZONE_ZFS_DATASET_MOUNTPOINT] {
                return Err(ApiError::InternalError {
                    message: format!(
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
