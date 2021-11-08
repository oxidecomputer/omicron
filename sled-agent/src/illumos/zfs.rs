//! Utilities for poking at ZFS.

use crate::illumos::{execute, PFEXEC};
use omicron_common::api::external::Error;
use std::fmt;
use std::path::PathBuf;

pub const ZONE_ZFS_DATASET_MOUNTPOINT: &str = "/zone";
pub const ZONE_ZFS_DATASET: &str = "rpool/zone";
const ZFS: &str = "/usr/sbin/zfs";

/// Wraps commands for interacting with ZFS.
pub struct Zfs {}

/// Describes a mountpoint for a ZFS filesystem.
pub enum Mountpoint {
    Legacy,
    Path(PathBuf),
}

impl fmt::Display for Mountpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Mountpoint::Legacy => write!(f, "legacy"),
            Mountpoint::Path(p) => write!(f, "{}", p.display()),
        }
    }
}

#[cfg_attr(test, mockall::automock, allow(dead_code))]
impl Zfs {
    /// Creates a new ZFS filesystem named `name`, unless one already exists.
    pub fn ensure_filesystem(
        name: &str,
        mountpoint: Mountpoint,
    ) -> Result<(), Error> {
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
            if values != &[name, "filesystem", &mountpoint.to_string()] {
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
            &format!("mountpoint={}", mountpoint),
            name,
        ]);
        execute(cmd)?;
        Ok(())
    }

    pub fn set_oxide_value(
        filesystem_name: &str,
        name: &str,
        value: &str,
    ) -> Result<(), Error> {
        Zfs::set_value(filesystem_name, &format!("oxide:{}", name), value)
    }

    fn set_value(
        filesystem_name: &str,
        name: &str,
        value: &str,
    ) -> Result<(), Error> {
        let mut command = std::process::Command::new(PFEXEC);
        let value_arg = format!("{}={}", name, value);
        let cmd = command.args(&[ZFS, "set", &value_arg, filesystem_name]);
        execute(cmd)?;
        Ok(())
    }

    pub fn get_oxide_value(
        filesystem_name: &str,
        name: &str,
    ) -> Result<String, Error> {
        Zfs::get_value(filesystem_name, &format!("oxide:{}", name))
    }

    fn get_value(filesystem_name: &str, name: &str) -> Result<String, Error> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd =
            command.args(&[ZFS, "get", "-Ho", "value", &name, filesystem_name]);
        let output = execute(cmd)?;
        let stdout = String::from_utf8(output.stdout).map_err(|e| {
            Error::InternalError {
                internal_message: format!(
                    "Cannot parse 'zfs get' output as UTF-8: {}",
                    e
                ),
            }
        })?;
        let value = stdout.trim();
        if value == "-" {
            return Err(Error::InternalError {
                internal_message: format!(
                    "Property {} does not exist for {}",
                    name, filesystem_name
                ),
            });
        }
        Ok(value.to_string())
    }
}
