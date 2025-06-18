// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Generic code to read a metadata file from an install dataset.

use camino::{Utf8Path, Utf8PathBuf};
use iddqd::{IdOrdItem, IdOrdMap, id_upcast};
use omicron_uuid_kinds::InternalZpoolUuid;
use serde::de::DeserializeOwned;
use sled_agent_config_reconciler::InternalDisksWithBootDisk;
use sled_agent_types::zone_images::{
    ArcIoError, ArcSerdeJsonError, InstallMetadataReadError,
};
use std::fs;

#[derive(Debug)]
pub(crate) struct AllInstallMetadataFiles<T: 'static> {
    pub(crate) boot_zpool: InternalZpoolUuid,
    pub(crate) boot_dataset_dir: Utf8PathBuf,
    pub(crate) boot_disk_path: Utf8PathBuf,
    pub(crate) boot_disk_metadata:
        Result<Option<InstallMetadata<T>>, InstallMetadataReadError>,
    pub(crate) non_boot_disk_metadata: IdOrdMap<InstallMetadataNonBootInfo<T>>,
}

impl<T> AllInstallMetadataFiles<T>
where
    T: DeserializeOwned + PartialEq,
{
    /// Attempt to find manifest files.
    ///
    /// For now we treat the boot disk as authoritative, since install-dataset
    /// artifacts are always served from the boot disk. There is a class of
    /// issues here related to transient failures on one of the M.2s that we're
    /// acknowledging but not tackling for now.
    ///
    /// In general, this API follows an interpreter pattern: first read all the
    /// results and put them in a map, then make decisions based on them in a
    /// separate step. This enables better testing and ensures that changes to
    /// behavior are described in the type system.
    ///
    /// `with_default` is used to provide a default value if the metadata file
    /// is not found.
    pub(crate) fn read_all<F>(
        _log: &slog::Logger,
        metadata_file_name: &str,
        internal_disks: &InternalDisksWithBootDisk,
        with_default: F,
    ) -> Self
    where
        F: Fn(&Utf8Path) -> Result<Option<T>, InstallMetadataReadError>,
    {
        let boot_dataset_dir = internal_disks.boot_disk_install_dataset();

        // Read the file from the boot disk.
        let boot_disk_path = boot_dataset_dir.join(metadata_file_name);
        let boot_disk_metadata =
            read_install_metadata_file::<T>(&boot_dataset_dir, &boot_disk_path);
        let boot_disk_metadata =
            InstallMetadata::new(boot_disk_metadata, || {
                with_default(&boot_dataset_dir)
            });

        // Read the file from all other disks. We attempt to make sure they match up
        // and will log a warning if they don't, though (until we have a better
        // story on transient failures) it's not fatal.
        let non_boot_datasets = internal_disks.non_boot_disk_install_datasets();
        let non_boot_disk_overrides = non_boot_datasets
            .map(|(zpool_id, dataset_dir)| {
                let path = dataset_dir.join(metadata_file_name);
                let res = read_install_metadata_file::<T>(&dataset_dir, &path);
                let res =
                    InstallMetadata::new(res, || with_default(&dataset_dir));

                let result =
                    InstallMetadataNonBootResult::new(res, &boot_disk_metadata);

                InstallMetadataNonBootInfo {
                    zpool_id,
                    dataset_dir,
                    path,
                    result,
                }
            })
            .collect();

        Self {
            boot_zpool: internal_disks.boot_disk_zpool_id(),
            boot_dataset_dir,
            boot_disk_path,
            boot_disk_metadata,
            non_boot_disk_metadata: non_boot_disk_overrides,
        }
    }
}

pub(crate) fn read_install_metadata_file<T>(
    dataset_dir: &Utf8Path,
    metadata_path: &Utf8Path,
) -> Result<Option<T>, InstallMetadataReadError>
where
    T: DeserializeOwned,
{
    // First check that the dataset directory exists. This distinguishes the
    // two cases:
    //
    // 1. The install dataset is missing (an error).
    // 2. The install dataset is present, but the metadata file is missing
    //    (expected).
    //
    // It would be nice if we could use openat-style APIs to read the file
    // from the opened directory, but:
    //
    // * those don't exist in rust std
    // * it's not crucial -- we don't expect TOCTTOU races much in this code
    //   path, and we're not generally resilient to them anyway.
    //
    // We use symlink_metadata (lstat) rather than metadata (stat) because
    // there really shouldn't be any symlinks involved.
    let dir_metadata = fs::symlink_metadata(dataset_dir).map_err(|error| {
        InstallMetadataReadError::DatasetDirMetadata {
            dataset_dir: dataset_dir.to_owned(),
            error: ArcIoError::new(error),
        }
    })?;
    if !dir_metadata.is_dir() {
        return Err(InstallMetadataReadError::DatasetNotDirectory {
            dataset_dir: dataset_dir.to_owned(),
            file_type: dir_metadata.file_type(),
        });
    }

    let metadata = match fs::read_to_string(metadata_path) {
        Ok(data) => {
            let metadata =
                serde_json::from_str::<T>(&data).map_err(|error| {
                    InstallMetadataReadError::Deserialize {
                        path: metadata_path.to_owned(),
                        error: ArcSerdeJsonError::new(error),
                        contents: data,
                    }
                })?;
            Some(metadata)
        }
        Err(error) => {
            if error.kind() == std::io::ErrorKind::NotFound {
                None
            } else {
                return Err(InstallMetadataReadError::Read {
                    path: metadata_path.to_owned(),
                    error: ArcIoError::new(error),
                });
            }
        }
    };

    Ok(metadata)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstallMetadata<T> {
    /// The value deserialized from disk or returned by the default function.
    pub value: T,

    /// True if the value was deserialized from disk.
    pub deserialized: bool,
}

impl<T> InstallMetadata<T> {
    fn new<F>(
        deserialized_metadata: Result<Option<T>, InstallMetadataReadError>,
        default_fn: F,
    ) -> Result<Option<Self>, InstallMetadataReadError>
    where
        F: FnOnce() -> Result<Option<T>, InstallMetadataReadError>,
    {
        match deserialized_metadata {
            Ok(Some(metadata)) => {
                Ok(Some(Self { value: metadata, deserialized: true }))
            }
            Ok(None) => match default_fn() {
                Ok(Some(metadata)) => {
                    Ok(Some(Self { value: metadata, deserialized: false }))
                }
                Ok(None) => Ok(None),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InstallMetadataNonBootInfo<T> {
    /// The ID of the zpool.
    pub zpool_id: InternalZpoolUuid,

    /// The dataset directory.
    pub dataset_dir: Utf8PathBuf,

    /// The path that was read from.
    pub path: Utf8PathBuf,

    /// The result of performing the read operation.
    pub result: InstallMetadataNonBootResult<T>,
}

impl<T> IdOrdItem for InstallMetadataNonBootInfo<T> {
    type Key<'a>
        = InternalZpoolUuid
    where
        T: 'a;

    fn key(&self) -> Self::Key<'_> {
        self.zpool_id
    }

    id_upcast!();
}

#[derive(Clone, Debug, PartialEq)]
pub enum InstallMetadataNonBootResult<T> {
    /// The metadata is present and matches the value on the boot disk (or the
    /// default value).
    MatchesPresent(InstallMetadata<T>),

    /// The file is absent and is also absent on the boot disk.
    MatchesAbsent,

    /// A mismatch between the boot disk and the other disk was detected.
    Mismatch(InstallMetadataNonBootMismatch<T>),

    /// An error occurred while reading the mupdate override info on this disk.
    ReadError(InstallMetadataReadError),
}

impl<T: PartialEq> InstallMetadataNonBootResult<T> {
    pub(crate) fn new(
        res: Result<Option<InstallMetadata<T>>, InstallMetadataReadError>,
        boot_disk_res: &Result<
            Option<InstallMetadata<T>>,
            InstallMetadataReadError,
        >,
    ) -> Self {
        match (res, boot_disk_res) {
            (Ok(Some(non_boot_disk_info)), Ok(Some(boot_disk_info))) => {
                if boot_disk_info == &non_boot_disk_info {
                    Self::MatchesPresent(non_boot_disk_info)
                } else {
                    Self::Mismatch(
                        InstallMetadataNonBootMismatch::ValueMismatch {
                            non_boot_disk_info,
                        },
                    )
                }
            }
            (Ok(Some(non_boot_disk_info)), Ok(None)) => Self::Mismatch(
                InstallMetadataNonBootMismatch::BootAbsentOtherPresent {
                    non_boot_disk_info,
                },
            ),
            (Ok(None), Ok(Some(_))) => Self::Mismatch(
                InstallMetadataNonBootMismatch::BootPresentOtherAbsent,
            ),
            (Ok(None), Ok(None)) => Self::MatchesAbsent,
            (Ok(non_boot_disk_info), Err(_)) => Self::Mismatch(
                InstallMetadataNonBootMismatch::BootDiskReadError {
                    non_boot_disk_info,
                },
            ),
            (Err(error), _) => Self::ReadError(error),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstallMetadataNonBootMismatch<T> {
    /// The file is present on the boot disk but absent on the other disk.
    BootPresentOtherAbsent,

    /// The file is absent on the boot disk but present on the other disk.
    BootAbsentOtherPresent {
        /// The information found on the other disk.
        non_boot_disk_info: InstallMetadata<T>,
    },

    /// The file's contents differ between the boot disk and the other disk.
    ValueMismatch { non_boot_disk_info: InstallMetadata<T> },

    /// There was a read error on the boot disk, so we were unable to verify
    /// consistency.
    BootDiskReadError {
        /// The value as found on this disk. This value is logged but not used.
        non_boot_disk_info: Option<InstallMetadata<T>>,
    },
}
