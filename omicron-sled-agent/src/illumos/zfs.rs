//! Utilities for poking at ZFS.

use crate::illumos::{execute, PFEXEC};
use omicron_common::error::ApiError;

pub const ZONE_ZFS_POOL_MOUNTPOINT: &str = "/zone";
pub const ZONE_ZFS_POOL: &str = "rpool/zone";
const ZFS: &str = "/usr/sbin/zfs";

/// Wraps commands for interacting with ZFS.
pub struct Zfs {}

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Zfs {
    /// Creates a new ZFS filesystem named `name`, unless one already exists.
    pub fn ensure_zpool(name: &str) -> Result<(), ApiError> {
        // If the zpool exists, we're done.
        let mut command = std::process::Command::new(ZFS);
        let cmd = command.args(&["list", name]);
        if execute(cmd).is_ok() {
            return Ok(());
        }

        // If it doesn't exist, make it.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZFS,
            "create",
            "-o",
            &format!("mountpoint={}", ZONE_ZFS_POOL_MOUNTPOINT),
            name,
        ]);
        execute(cmd)?;
        Ok(())
    }
}
