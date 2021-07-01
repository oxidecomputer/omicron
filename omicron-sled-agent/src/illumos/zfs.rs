//! Utilities for poking at ZFS.

use omicron_common::error::ApiError;
use crate::illumos::{execute, PFEXEC};

pub const ZONE_ZFS_POOL_MOUNTPOINT: &str = "/zone";
pub const ZONE_ZFS_POOL: &str = "rpool/zone";

/// Wraps commands for interacting with ZFS.
pub struct Zfs {}

impl Zfs {
    /// Creates a new ZFS filesystem named `name`, unless one already exists.
    pub fn ensure_zpool(name: &str) -> Result<(), ApiError> {
        // If the zpool exists, we're done.
        let mut command = std::process::Command::new("zfs");
        let cmd = command.args(&["list", name]);
        if execute(cmd).is_ok() {
            return Ok(());
        }

        // If it doesn't exist, make it.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            "zfs",
            "create",
            "-o",
            &format!("mountpoint={}", ZONE_ZFS_POOL_MOUNTPOINT),
            name,
        ]);
        execute(cmd)?;
        Ok(())
    }
}
