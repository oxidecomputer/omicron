// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for poking at ZFS.

use crate::{execute, PFEXEC};
use std::fmt;
use std::path::PathBuf;

pub const ZONE_ZFS_DATASET_MOUNTPOINT: &str = "/zone";
pub const ZONE_ZFS_DATASET: &str = "rpool/zone";
const ZFS: &str = "/usr/sbin/zfs";

/// Error returned by [`Zfs::list_datasets`].
#[derive(thiserror::Error, Debug)]
#[error("Could not list datasets within zpool {name}: {err}")]
pub struct ListDatasetsError {
    name: String,
    #[source]
    err: crate::ExecutionError,
}

/// Error returned by [`Zfs::destroy_dataset`].
#[derive(thiserror::Error, Debug)]
#[error("Could not destroy dataset {name}: {err}")]
pub struct DestroyDatasetError {
    name: String,
    #[source]
    err: crate::ExecutionError,
}

#[derive(thiserror::Error, Debug)]
enum EnsureFilesystemErrorRaw {
    #[error("ZFS execution error: {0}")]
    Execution(#[from] crate::ExecutionError),

    #[error("Filesystem does not exist, and formatting was not requested")]
    NotFoundNotFormatted,

    #[error("Unexpected output from ZFS commands: {0}")]
    Output(String),
}

/// Error returned by [`Zfs::ensure_zoned_filesystem`].
#[derive(thiserror::Error, Debug)]
#[error(
    "Failed to ensure filesystem '{name}' exists at '{mountpoint:?}': {err}"
)]
pub struct EnsureFilesystemError {
    name: String,
    mountpoint: Mountpoint,
    #[source]
    err: EnsureFilesystemErrorRaw,
}

/// Error returned by [`Zfs::set_oxide_value`]
#[derive(thiserror::Error, Debug)]
#[error(
    "Failed to set value '{name}={value}' on filesystem {filesystem}: {err}"
)]
pub struct SetValueError {
    filesystem: String,
    name: String,
    value: String,
    err: crate::ExecutionError,
}

#[derive(thiserror::Error, Debug)]
enum GetValueErrorRaw {
    #[error(transparent)]
    Execution(#[from] crate::ExecutionError),

    #[error("No value found with that name")]
    MissingValue,
}

/// Error returned by [`Zfs::get_oxide_value`].
#[derive(thiserror::Error, Debug)]
#[error("Failed to get value '{name}' from filesystem {filesystem}: {err}")]
pub struct GetValueError {
    filesystem: String,
    name: String,
    err: GetValueErrorRaw,
}

/// Wraps commands for interacting with ZFS.
pub struct Zfs {}

/// Describes a mountpoint for a ZFS filesystem.
#[derive(Debug, Clone)]
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

#[cfg_attr(any(test, feature = "testing"), mockall::automock, allow(dead_code))]
impl Zfs {
    /// Lists all datasets within a pool or existing dataset.
    pub fn list_datasets(name: &str) -> Result<Vec<String>, ListDatasetsError> {
        let mut command = std::process::Command::new(ZFS);
        let cmd = command.args(&["list", "-d", "1", "-rHpo", "name", name]);

        let output = execute(cmd)
            .map_err(|err| ListDatasetsError { name: name.to_string(), err })?;
        let stdout = String::from_utf8_lossy(&output.stdout);
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

    /// Destroys a dataset.
    pub fn destroy_dataset(name: &str) -> Result<(), DestroyDatasetError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[ZFS, "destroy", "-r", name]);
        execute(cmd).map_err(|err| DestroyDatasetError {
            name: name.to_string(),
            err,
        })?;
        Ok(())
    }

    /// Creates a new ZFS filesystem named `name`, unless one already exists.
    pub fn ensure_zoned_filesystem(
        name: &str,
        mountpoint: Mountpoint,
        do_format: bool,
    ) -> Result<(), EnsureFilesystemError> {
        // If the dataset exists, we're done.
        let mut command = std::process::Command::new(ZFS);
        let cmd = command.args(&["list", "-Hpo", "name,type,mountpoint", name]);

        // If the list command returns any valid output, validate it.
        if let Ok(output) = execute(cmd) {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let values: Vec<&str> = stdout.trim().split('\t').collect();
            if values != &[name, "filesystem", &mountpoint.to_string()] {
                return Err(EnsureFilesystemError {
                    name: name.to_string(),
                    mountpoint,
                    err: EnsureFilesystemErrorRaw::Output(stdout.to_string()),
                });
            }
            return Ok(());
        }

        if !do_format {
            return Err(EnsureFilesystemError {
                name: name.to_string(),
                mountpoint,
                err: EnsureFilesystemErrorRaw::NotFoundNotFormatted,
            });
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
        execute(cmd).map_err(|err| EnsureFilesystemError {
            name: name.to_string(),
            mountpoint,
            err: err.into(),
        })?;
        Ok(())
    }

    pub fn set_oxide_value(
        filesystem_name: &str,
        name: &str,
        value: &str,
    ) -> Result<(), SetValueError> {
        Zfs::set_value(filesystem_name, &format!("oxide:{}", name), value)
    }

    fn set_value(
        filesystem_name: &str,
        name: &str,
        value: &str,
    ) -> Result<(), SetValueError> {
        let mut command = std::process::Command::new(PFEXEC);
        let value_arg = format!("{}={}", name, value);
        let cmd = command.args(&[ZFS, "set", &value_arg, filesystem_name]);
        execute(cmd).map_err(|err| SetValueError {
            filesystem: filesystem_name.to_string(),
            name: name.to_string(),
            value: value.to_string(),
            err,
        })?;
        Ok(())
    }

    pub fn get_oxide_value(
        filesystem_name: &str,
        name: &str,
    ) -> Result<String, GetValueError> {
        Zfs::get_value(filesystem_name, &format!("oxide:{}", name))
    }

    fn get_value(
        filesystem_name: &str,
        name: &str,
    ) -> Result<String, GetValueError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd =
            command.args(&[ZFS, "get", "-Ho", "value", &name, filesystem_name]);
        let output = execute(cmd).map_err(|err| GetValueError {
            filesystem: filesystem_name.to_string(),
            name: name.to_string(),
            err: err.into(),
        })?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let value = stdout.trim();
        if value == "-" {
            return Err(GetValueError {
                filesystem: filesystem_name.to_string(),
                name: name.to_string(),
                err: GetValueErrorRaw::MissingValue,
            });
        }
        Ok(value.to_string())
    }
}
