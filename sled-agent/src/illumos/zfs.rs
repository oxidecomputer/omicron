// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for poking at ZFS.

use crate::illumos::{execute, PFEXEC};
use std::fmt;
use std::path::PathBuf;

pub const ZONE_ZFS_DATASET_MOUNTPOINT: &str = "/zone";
pub const ZONE_ZFS_DATASET: &str = "rpool/zone";
const ZFS: &str = "/usr/sbin/zfs";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("ZFS execution error: {0}")]
    Execution(#[from] crate::illumos::ExecutionError),

    #[error("Does not exist: {0}")]
    NotFound(String),

    #[error("Unexpected output from ZFS commands: {0}")]
    Output(String),

    #[error("Failed to parse output: {0}")]
    Parse(#[from] std::string::FromUtf8Error),
}

/// Wraps commands for interacting with ZFS.
pub struct Zfs {}

/// Describes a mountpoint for a ZFS filesystem.
pub enum Mountpoint {
    #[allow(dead_code)]
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
    /// Lists all filesystems within a dataset.
    pub fn list_filesystems(name: &str) -> Result<Vec<String>, Error> {
        let mut command = std::process::Command::new(ZFS);
        let cmd = command.args(&["list", "-d", "1", "-rHpo", "name", name]);

        let output = execute(cmd)?;
        let stdout = String::from_utf8(output.stdout)?;
        let filesystems: Vec<String> = stdout
            .trim()
            .split('\n')
            .filter(|n| *n != name)
            .map(|s| {
                String::from(s.strip_prefix(&format!("{}/", name)).unwrap())
            })
            .collect();
        Ok(filesystems)
    }

    /// Creates a new ZFS filesystem named `name`, unless one already exists.
    pub fn ensure_filesystem(
        name: &str,
        mountpoint: Mountpoint,
        do_format: bool,
    ) -> Result<(), Error> {
        // If the dataset exists, we're done.
        let mut command = std::process::Command::new(ZFS);
        let cmd = command.args(&["list", "-Hpo", "name,type,mountpoint", name]);

        // If the list command returns any valid output, validate it.
        if let Ok(output) = execute(cmd) {
            let stdout = String::from_utf8(output.stdout)?;
            let values: Vec<&str> = stdout.trim().split('\t').collect();
            if values != &[name, "filesystem", &mountpoint.to_string()] {
                return Err(Error::Output(stdout));
            }
            return Ok(());
        }

        if !do_format {
            return Err(Error::NotFound(format!(
                "Filesystem {} not found",
                name
            )));
        }

        // If it doesn't exist, make it.
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[
            ZFS,
            "create",
            // The filesystem is managed from the Global Zone.
            "-o",
            "zoned=on",
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
        let stdout = String::from_utf8(output.stdout)?;
        let value = stdout.trim();
        if value == "-" {
            return Err(Error::NotFound(format!(
                "Property {}, within filesystem {}",
                name, filesystem_name
            )));
        }
        Ok(value.to_string())
    }
}
