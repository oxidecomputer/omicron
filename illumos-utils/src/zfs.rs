// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for poking at ZFS.

use crate::host::{BoxedExecutor, ExecutionError, PFEXEC};
use camino::Utf8PathBuf;
use omicron_common::disk::DiskIdentity;
use std::fmt;

// These locations in the ramdisk must only be used by the switch zone.
//
// We need the switch zone online before we can create the U.2 drives and
// encrypt the zpools during rack initialization. Without the switch zone we
// cannot get the rack initialization request from wicketd in RSS which allows
// us to  initialize the trust quorum and derive the encryption keys needed for
// the U.2 disks.
pub const ZONE_ZFS_RAMDISK_DATASET_MOUNTPOINT: &str = "/zone";
pub const ZONE_ZFS_RAMDISK_DATASET: &str = "rpool/zone";

pub const ZFS: &str = "/usr/sbin/zfs";
pub const KEYPATH_ROOT: &str = "/var/run/oxide/";

/// Error returned by [`Zfs::list_datasets`].
#[derive(thiserror::Error, Debug)]
#[error("Could not list datasets within zpool {name}: {err}")]
pub struct ListDatasetsError {
    name: String,
    #[source]
    err: ExecutionError,
}

#[derive(thiserror::Error, Debug)]
pub enum DestroyDatasetErrorVariant {
    #[error("Dataset not found")]
    NotFound,
    #[error(transparent)]
    Other(ExecutionError),
}

/// Error returned by [`Zfs::destroy_dataset`].
#[derive(thiserror::Error, Debug)]
#[error("Could not destroy dataset {name}: {err}")]
pub struct DestroyDatasetError {
    name: String,
    #[source]
    pub err: DestroyDatasetErrorVariant,
}

#[derive(thiserror::Error, Debug)]
enum EnsureFilesystemErrorRaw {
    #[error("ZFS execution error: {0}")]
    Execution(#[from] ExecutionError),

    #[error("Filesystem does not exist, and formatting was not requested")]
    NotFoundNotFormatted,

    #[error("Unexpected output from ZFS commands: {0}")]
    Output(String),

    #[error("Failed to mount encrypted filesystem: {0}")]
    MountEncryptedFsFailed(ExecutionError),
}

/// Error returned by [`Zfs::ensure_filesystem`].
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
    err: ExecutionError,
}

#[derive(thiserror::Error, Debug)]
enum GetValueErrorRaw {
    #[error(transparent)]
    Execution(#[from] ExecutionError),

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
    Path(Utf8PathBuf),
}

impl fmt::Display for Mountpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Mountpoint::Legacy => write!(f, "legacy"),
            Mountpoint::Path(p) => write!(f, "{p}"),
        }
    }
}

/// This is the path for an encryption key used by ZFS
#[derive(Debug, Clone)]
pub struct Keypath(pub Utf8PathBuf);

impl fmt::Display for Keypath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&DiskIdentity> for Keypath {
    fn from(id: &DiskIdentity) -> Self {
        let filename = format!(
            "{}-{}-{}-zfs-aes-256-gcm.key",
            id.vendor, id.serial, id.model
        );
        let mut path = Utf8PathBuf::new();
        path.push(KEYPATH_ROOT);
        path.push(filename);
        Keypath(path)
    }
}

#[derive(Debug)]
pub struct EncryptionDetails {
    pub keypath: Keypath,
    pub epoch: u64,
}

#[derive(Debug, Default)]
pub struct SizeDetails {
    pub quota: Option<usize>,
    pub compression: Option<&'static str>,
}

impl Zfs {
    /// Lists all datasets within a pool or existing dataset.
    pub fn list_datasets(
        executor: &BoxedExecutor,
        name: &str,
    ) -> Result<Vec<String>, ListDatasetsError> {
        let mut command = std::process::Command::new(ZFS);
        let cmd = command.args(&["list", "-d", "1", "-rHpo", "name", name]);

        let output = executor
            .execute(cmd)
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
    pub fn destroy_dataset(
        executor: &BoxedExecutor,
        name: &str,
    ) -> Result<(), DestroyDatasetError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[ZFS, "destroy", "-r", name]);
        executor.execute(cmd).map_err(|err| {
            let variant = match err {
                ExecutionError::CommandFailure(info)
                    if info.stderr.contains("does not exist") =>
                {
                    DestroyDatasetErrorVariant::NotFound
                }
                _ => DestroyDatasetErrorVariant::Other(err),
            };
            DestroyDatasetError { name: name.to_string(), err: variant }
        })?;
        Ok(())
    }

    /// Creates a new ZFS filesystem named `name`, unless one already exists.
    ///
    /// Applies an optional quota, provided _in bytes_.
    pub fn ensure_filesystem(
        executor: &BoxedExecutor,
        name: &str,
        mountpoint: Mountpoint,
        zoned: bool,
        do_format: bool,
        encryption_details: Option<EncryptionDetails>,
        size_details: Option<SizeDetails>,
    ) -> Result<(), EnsureFilesystemError> {
        let (exists, mounted) =
            Self::dataset_exists(executor, name, &mountpoint)?;
        if exists {
            if let Some(SizeDetails { quota, compression }) = size_details {
                // apply quota and compression mode (in case they've changed across
                // sled-agent versions since creation)
                Self::apply_properties(
                    executor,
                    name,
                    &mountpoint,
                    quota,
                    compression,
                )?;
            }

            if encryption_details.is_none() {
                // If the dataset exists, we're done. Unencrypted datasets are
                // automatically mounted.
                return Ok(());
            } else {
                if mounted {
                    // The dataset exists and is mounted
                    return Ok(());
                }
                // We need to load the encryption key and mount the filesystem
                return Self::mount_encrypted_dataset(
                    executor,
                    name,
                    &mountpoint,
                );
            }
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
        let cmd = command.args(&[ZFS, "create"]);
        if zoned {
            cmd.args(&["-o", "zoned=on"]);
        }
        if let Some(details) = encryption_details {
            let keyloc = format!("keylocation=file://{}", details.keypath);
            let epoch = format!("oxide:epoch={}", details.epoch);
            cmd.args(&[
                "-o",
                "encryption=aes-256-gcm",
                "-o",
                "keyformat=raw",
                "-o",
                &keyloc,
                "-o",
                &epoch,
            ]);
        }

        cmd.args(&["-o", &format!("mountpoint={}", mountpoint), name]);
        executor.execute(cmd).map_err(|err| EnsureFilesystemError {
            name: name.to_string(),
            mountpoint: mountpoint.clone(),
            err: err.into(),
        })?;

        if let Some(SizeDetails { quota, compression }) = size_details {
            // Apply any quota and compression mode.
            Self::apply_properties(
                executor,
                name,
                &mountpoint,
                quota,
                compression,
            )?;
        }

        Ok(())
    }

    fn apply_properties(
        executor: &BoxedExecutor,
        name: &str,
        mountpoint: &Mountpoint,
        quota: Option<usize>,
        compression: Option<&'static str>,
    ) -> Result<(), EnsureFilesystemError> {
        if let Some(quota) = quota {
            if let Err(err) =
                Self::set_value(executor, name, "quota", &format!("{quota}"))
            {
                return Err(EnsureFilesystemError {
                    name: name.to_string(),
                    mountpoint: mountpoint.clone(),
                    // Take the execution error from the SetValueError
                    err: err.err.into(),
                });
            }
        }
        if let Some(compression) = compression {
            if let Err(err) =
                Self::set_value(executor, name, "compression", compression)
            {
                return Err(EnsureFilesystemError {
                    name: name.to_string(),
                    mountpoint: mountpoint.clone(),
                    // Take the execution error from the SetValueError
                    err: err.err.into(),
                });
            }
        }
        Ok(())
    }

    fn mount_encrypted_dataset(
        executor: &BoxedExecutor,
        name: &str,
        mountpoint: &Mountpoint,
    ) -> Result<(), EnsureFilesystemError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd = command.args(&[ZFS, "mount", "-l", name]);
        executor.execute(cmd).map_err(|err| EnsureFilesystemError {
            name: name.to_string(),
            mountpoint: mountpoint.clone(),
            err: EnsureFilesystemErrorRaw::MountEncryptedFsFailed(err),
        })?;
        Ok(())
    }

    // Return (true, mounted) if the dataset exists, (false, false) otherwise,
    // where mounted is if the dataset is mounted.
    fn dataset_exists(
        executor: &BoxedExecutor,
        name: &str,
        mountpoint: &Mountpoint,
    ) -> Result<(bool, bool), EnsureFilesystemError> {
        let mut command = std::process::Command::new(ZFS);
        let cmd = command.args(&[
            "list",
            "-Hpo",
            "name,type,mountpoint,mounted",
            name,
        ]);
        // If the list command returns any valid output, validate it.
        if let Ok(output) = executor.execute(cmd) {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let values: Vec<&str> = stdout.trim().split('\t').collect();
            if &values[..3] != &[name, "filesystem", &mountpoint.to_string()] {
                return Err(EnsureFilesystemError {
                    name: name.to_string(),
                    mountpoint: mountpoint.clone(),
                    err: EnsureFilesystemErrorRaw::Output(stdout.to_string()),
                });
            }
            let mounted = values[3] == "yes";
            Ok((true, mounted))
        } else {
            Ok((false, false))
        }
    }

    pub fn set_oxide_value(
        executor: &BoxedExecutor,
        filesystem_name: &str,
        name: &str,
        value: &str,
    ) -> Result<(), SetValueError> {
        Zfs::set_value(
            executor,
            filesystem_name,
            &format!("oxide:{}", name),
            value,
        )
    }

    fn set_value(
        executor: &BoxedExecutor,
        filesystem_name: &str,
        name: &str,
        value: &str,
    ) -> Result<(), SetValueError> {
        let mut command = std::process::Command::new(PFEXEC);
        let value_arg = format!("{}={}", name, value);
        let cmd = command.args(&[ZFS, "set", &value_arg, filesystem_name]);
        executor.execute(cmd).map_err(|err| SetValueError {
            filesystem: filesystem_name.to_string(),
            name: name.to_string(),
            value: value.to_string(),
            err,
        })?;
        Ok(())
    }

    pub fn get_oxide_value(
        executor: &BoxedExecutor,
        filesystem_name: &str,
        name: &str,
    ) -> Result<String, GetValueError> {
        Zfs::get_value(executor, filesystem_name, &format!("oxide:{}", name))
    }

    fn get_value(
        executor: &BoxedExecutor,
        filesystem_name: &str,
        name: &str,
    ) -> Result<String, GetValueError> {
        let mut command = std::process::Command::new(PFEXEC);
        let cmd =
            command.args(&[ZFS, "get", "-Ho", "value", &name, filesystem_name]);
        let output = executor.execute(cmd).map_err(|err| GetValueError {
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

/// Returns all datasets managed by Omicron
pub fn get_all_omicron_datasets_for_delete(
    executor: &BoxedExecutor,
) -> anyhow::Result<Vec<String>> {
    let mut datasets = vec![];

    // Collect all datasets within Oxide zpools.
    //
    // This includes cockroachdb, clickhouse, and crucible datasets.
    let zpools = crate::zpool::Zpool::list(executor)?;
    for pool in &zpools {
        let internal = pool.kind() == crate::zpool::ZpoolKind::Internal;
        let pool = pool.to_string();
        for dataset in &Zfs::list_datasets(executor, &pool)? {
            // Avoid erasing crashdump datasets on internal pools
            if dataset == "crash" && internal {
                continue;
            }

            // The swap device might be in use, so don't assert that it can be deleted.
            if dataset == "swap" && internal {
                continue;
            }

            datasets.push(format!("{pool}/{dataset}"));
        }
    }

    // Collect all datasets for ramdisk-based Oxide zones, if any exist.
    if let Ok(ramdisk_datasets) =
        Zfs::list_datasets(executor, &ZONE_ZFS_RAMDISK_DATASET)
    {
        for dataset in &ramdisk_datasets {
            datasets.push(format!("{}/{dataset}", ZONE_ZFS_RAMDISK_DATASET));
        }
    };

    Ok(datasets)
}
